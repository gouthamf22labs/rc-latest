import { randomUUID } from 'crypto';
import { hostname } from 'os';
import type { Logger } from '../config/logger.config';
import type { Repository } from '../repository/repository.service';

/**
 * Process-wide lease on the right to hold WhatsApp sockets.
 *
 * The deploy platform starts the replacement container before it stops the old one, and
 * main.ts listens immediately while loadInstance() restores in the background. For ~10s
 * both processes held sockets on the same credentials, and WhatsApp resolved every
 * duplicate with 440 conflict/replaced: 64 instances on 2026-09-12 — every instance the
 * new process restored inside that window, and none after the old process died. Graceful
 * shutdown cannot prevent it, because SIGTERM only reaches the old process after the
 * overlap has already happened.
 *
 * So no socket opens until this process holds the lease. The replacement serves HTTP (the
 * platform's health check passes and it moves on to stopping the old container), waits,
 * and restores once the old process releases the lease on SIGTERM — or once it expires, if
 * the old process died without releasing it.
 *
 * A row with an expiry rather than pg_advisory_lock: DATABASE_URL goes through PgBouncer,
 * and a session-level advisory lock belongs to whichever server connection the pooler
 * handed out, which it then hands to other clients. Expiry is compared against the
 * database clock, so clock skew between containers does not matter.
 *
 * Tune with SOCKET_LEASE_TTL_MS (default 20000) and SOCKET_LEASE_POLL_MS (default 1000).
 * SOCKET_LEASE_ENABLED=false opens sockets without a lease.
 */
const LEASE_ID = 'whatsapp-sockets';

function envMs(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const TTL_MS = envMs('SOCKET_LEASE_TTL_MS', 20_000);
// Renew well inside the TTL so one slow round-trip or event-loop stall cannot let it lapse.
const RENEW_MS = Math.max(1_000, Math.floor(TTL_MS / 4));
const POLL_MS = envMs('SOCKET_LEASE_POLL_MS', 1_000);

function isMissingTable(error: unknown): boolean {
  const e = error as { code?: string; meta?: { code?: string }; message?: string };
  const text = `${e?.code ?? ''} ${e?.meta?.code ?? ''} ${e?.message ?? ''}`;
  return text.includes('42P01') || /TableDoesNotExist|SocketLease.*does not exist/i.test(text);
}

class SocketLease {
  readonly owner = `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;

  private repository?: Repository;
  private logger?: Logger;
  private acquiring?: Promise<void>;
  private renewTimer?: ReturnType<typeof setInterval>;
  private renewing?: Promise<void>;
  private claiming?: Promise<boolean>;
  private owned = false;
  private closed = false;

  /** True while this process may open sockets without waiting. */
  get isHeld(): boolean {
    return this.owned && !this.closed;
  }

  start(repository: Repository, logger: Logger): void {
    if (this.acquiring) {
      return;
    }
    if ((process.env.SOCKET_LEASE_ENABLED ?? 'true') === 'false') {
      logger.warn('socket lease disabled - sockets open without waiting for a previous process');
      return;
    }
    this.repository = repository;
    this.logger = logger;
    this.acquiring = this.acquire();
  }

  /**
   * Resolves when this process may open a socket. Never resolves once shutdown has begun:
   * a reconnect timer firing mid-drain must not open a socket the successor is about to
   * open on the same credentials. Pending promises do not keep the process alive.
   */
  ready(): Promise<void> {
    if (this.closed) {
      return new Promise(() => undefined);
    }
    return this.acquiring ?? Promise.resolve();
  }

  /** Stop opening sockets. First step of shutdown, before the drain. */
  close(): void {
    this.closed = true;
    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = undefined;
    }
  }

  /** Hand the lease to the successor. Call once sockets are closed and flushed. */
  async release(): Promise<void> {
    this.close();
    // A renew already on the wire would re-insert the row after the delete below, and a
    // first claim on the wire may already have committed: either way wait for it, so the
    // row is gone before main.ts calls process.exit.
    await this.renewing;
    await this.claiming?.catch(() => false);
    if (!this.owned || !this.repository) {
      return;
    }
    this.owned = false;
    try {
      await this.repository.$executeRaw`
        DELETE FROM "SocketLease" WHERE "id" = ${LEASE_ID} AND "owner" = ${this.owner}`;
      this.logger?.info('socket lease released', { owner: this.owner });
    } catch (error) {
      this.logger?.warn('socket lease release failed - successor waits for expiry', {
        owner: this.owner,
        error: String(error),
      });
    }
  }

  /**
   * claim(), with ownership recorded in the same promise that release() awaits — so a
   * claim that lands during shutdown is always visible to release() and cleaned up by it.
   */
  private claimOnce(): Promise<boolean> {
    const pending = this.claim().then((won) => {
      if (won) this.owned = true;
      return won;
    });
    this.claiming = pending;
    return pending.finally(() => {
      if (this.claiming === pending) this.claiming = undefined;
    });
  }

  /**
   * Take the lease if it is free, expired, or already ours (which is also how it renews).
   * Concurrent claimers serialise on the row lock, and the loser re-evaluates the WHERE
   * against the winner's row, so exactly one of them gets a row back.
   */
  private async claim(): Promise<boolean> {
    const rows = await this.repository!.$queryRaw<{ owner: string }[]>`
      INSERT INTO "SocketLease" ("id", "owner", "expiresAt")
      VALUES (${LEASE_ID}, ${this.owner}, now() + ${TTL_MS}::int * interval '1 millisecond')
      ON CONFLICT ("id") DO UPDATE
        SET "owner" = EXCLUDED."owner", "expiresAt" = EXCLUDED."expiresAt"
        WHERE "SocketLease"."owner" = EXCLUDED."owner"
           OR "SocketLease"."expiresAt" < now()
      RETURNING "owner"`;
    return rows.length > 0;
  }

  private async describeHolder(): Promise<Record<string, unknown>> {
    const rows = await this.repository!.$queryRaw<{ owner: string; expiresInMs: number }[]>`
      SELECT "owner",
             GREATEST(0, EXTRACT(EPOCH FROM ("expiresAt" - now())) * 1000)::int AS "expiresInMs"
      FROM "SocketLease" WHERE "id" = ${LEASE_ID}`;
    return rows[0] ? { holder: rows[0].owner, holderExpiresInMs: rows[0].expiresInMs } : {};
  }

  private async acquire(): Promise<void> {
    const since = Date.now();
    let announced = false;

    while (!this.closed) {
      try {
        if (await this.claimOnce()) {
          if (this.closed) {
            // Shutdown began while the claim was on the wire. release() awaited this claim
            // and deletes the row itself; releasing from here would race process.exit.
            break;
          }
          this.renewTimer = setInterval(() => {
            if (this.closed || this.renewing) return;
            this.renewing = this.renew().finally(() => (this.renewing = undefined));
          }, RENEW_MS);
          this.renewTimer.unref();
          this.logger!.info(
            `socket lease acquired${announced ? ` after ${Date.now() - since}ms` : ''}`,
            { owner: this.owner },
          );
          return;
        }
        if (!announced) {
          announced = true;
          this.logger!.warn(
            'socket lease held by another process - waiting for it before opening WhatsApp sockets',
            { owner: this.owner, ...(await this.describeHolder()) },
          );
        }
      } catch (error) {
        if (isMissingTable(error)) {
          // Fail open. Refusing to connect every account because a migration did not run
          // is a far bigger outage than the deploy-time overlap this exists to prevent.
          this.logger!.error(
            'SocketLease table missing (migration not applied) - opening sockets WITHOUT a lease',
            { error: String(error) },
          );
          return;
        }
        this.logger!.error('socket lease claim failed - retrying', { error: String(error) });
      }
      await new Promise((resolve) => setTimeout(resolve, POLL_MS));
    }

    // Shutdown began before the lease was won. Stay pending — see ready().
    return new Promise(() => undefined);
  }

  private async renew(): Promise<void> {
    try {
      if (!(await this.claimOnce())) {
        // Only possible if this process stalled past the TTL and a successor claimed the
        // expired lease: both now hold sockets on the same credentials.
        this.logger?.error('socket lease lost to another process while this one holds sockets', {
          owner: this.owner,
          ...(await this.describeHolder()),
        });
      }
    } catch (error) {
      this.logger?.warn('socket lease renew failed', { owner: this.owner, error: String(error) });
    }
  }
}

export const socketLease = new SocketLease();
