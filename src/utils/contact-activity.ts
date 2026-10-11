import { Prisma } from '@prisma/client';
import {
  isLidUser,
  isPnUser,
  jidNormalizedUser,
  WAMessageKey,
} from '@whiskeysockets/baileys';
import type { Logger } from '../config/logger.config';
import type { Repository } from '../repository/repository.service';
import { yieldToLoop } from './yield-to-loop';

/**
 * Who each connected number has chatted 1:1 with: one ContactActivity row per (number, peer)
 * with when they first and last talked and how many messages went each way.
 *
 * Two sources feed it. Live messages (record) count each message. History sync
 * (recordHistoryChat) only says "these two have a chat": it sets firstAt/lastAt and never the
 * counts, since a history chunk is a partial, possibly repeated view of the past.
 *
 * One row per person: a peer is keyed by their phone jid whenever it can be found (from the
 * message key, the history chat's pnJid, or the account's LID→PN store at flush time), with
 * the LID kept alongside. A row written under a LID before the phone was known is folded into
 * the phone row once a later write links the two.
 *
 * Keyed by the account's own phone number, not the instance: a QR re-pair creates a new
 * instance for the same number, and its history with people must carry over.
 *
 * Never a write per message. record() folds messages into an in-memory buffer keyed by
 * (number, peer), and one INSERT ... ON CONFLICT per ≤500 peers writes it every 3s, or as
 * soon as 500 peers are waiting. One flush runs at a time per process. A failed flush keeps
 * its rows for the next one (up to a cap) and logs; nothing here ever throws into the socket
 * path. The buffer is process-wide, so instance teardown loses nothing; process shutdown
 * drains it (main.ts).
 *
 * CONTACT_ACTIVITY_ENABLED=false turns it off: record() becomes a no-op.
 */
type Entry = {
  ownerNumber: string;
  peerJid: string;
  peerLid: string | null;
  firstAt: number;
  lastAt: number;
  fromMe: number;
  fromThem: number;
  source: 'live' | 'history';
  /** For a peer known only by LID: the account's LID→PN lookup, tried once at flush. */
  resolvePn?: (lid: string) => Promise<string | null | undefined>;
};

export type LidToPn = (lid: string) => Promise<string | null | undefined>;

const FLUSH_MS = 3_000;
const FLUSH_AT_KEYS = 500;
// Rows per statement: 500 × 8 bound values stays far below Postgres' 65535 limit.
const ROWS_PER_STATEMENT = 500;
// While the database is unreachable, failed rows are kept for a retry only up to this size;
// past it they are dropped (and counted) rather than growing without bound.
const MAX_RETAINED_KEYS = 20_000;
const SUMMARY_MS = 60_000;

// Not a conversation on their own: reactions, and protocol traffic (edits, deletes, key
// exchange) that rides alongside real messages.
const IGNORED_TYPES = new Set([
  'protocolMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'reactionMessage',
  'encReactionMessage',
]);

function isMissingTable(error: unknown): boolean {
  const e = error as { code?: string; meta?: { code?: string }; message?: string };
  const text = `${e?.code ?? ''} ${e?.meta?.code ?? ''} ${e?.message ?? ''}`;
  return (
    text.includes('42P01') ||
    /TableDoesNotExist|ContactActivity.*does not exist/i.test(text)
  );
}

/** A protobuf timestamp (number, numeric string, or Long) as unix seconds; 0 when absent. */
function unixSeconds(value: unknown): number {
  if (value == null) return 0;
  if (typeof value === 'number') return value > 0 ? value : 0;
  if (typeof value === 'string') return Number(value) > 0 ? Number(value) : 0;
  const long = value as { toNumber?: () => number; low?: number };
  const n = typeof long.toNumber === 'function' ? long.toNumber() : Number(long.low);
  return n > 0 ? n : 0;
}

/** The account's own phone digits from an ownerJid / user id (`91…:12@s.whatsapp.net`). */
export function ownerNumberOf(jid: string | undefined | null): string | undefined {
  const user = jid?.split('@')[0]?.split(':')[0];
  return user && /^\d+$/.test(user) ? user : undefined;
}

/**
 * The person on the other side of a 1:1 message, as a phone jid when the key carries one
 * and the LID alongside when it carries that. Undefined for anything that is not a chat with
 * another person: groups, channels, status, broadcast lists, and "message yourself".
 *
 * remoteJidAlt is the other addressing form of the sender. On a message we sent from another
 * device the sender is us, so an alt that is our own number or LID is ignored.
 */
export function peerOf(
  key: WAMessageKey,
  ownerNumber: string,
  ownLid?: string,
): { peerJid: string; peerLid: string | null } | undefined {
  // "Us", or WhatsApp's own service account (0@s.whatsapp.net), which is not a person.
  const own = (jid: string) =>
    jid === `${ownerNumber}@s.whatsapp.net` ||
    jid === '0@s.whatsapp.net' ||
    (!!ownLid && jid === ownLid);

  const remote = key?.remoteJid ? jidNormalizedUser(key.remoteJid) : '';
  if (!(isPnUser(remote) || isLidUser(remote)) || own(remote)) {
    return undefined;
  }

  let pn = isPnUser(remote) ? remote : undefined;
  let lid = isLidUser(remote) ? remote : undefined;
  const alt = key.remoteJidAlt ? jidNormalizedUser(key.remoteJidAlt) : '';
  if (alt && !own(alt)) {
    if (!pn && isPnUser(alt)) pn = alt;
    if (!lid && isLidUser(alt)) lid = alt;
  }

  return { peerJid: pn ?? lid ?? remote, peerLid: lid ?? null };
}

export type KnownContact = {
  jid: string;
  firstAt: Date;
  lastAt: Date;
  fromMeCount: number;
  fromThemCount: number;
};

const LOOKUP_CHUNK = 500;

/** Digits → `digits@s.whatsapp.net`; a phone or LID jid → normalized; anything else → undefined. */
export function normalizePeer(input: string): string | undefined {
  const value = input?.trim().toLowerCase();
  if (!value) {
    return undefined;
  }
  if (!value.includes('@')) {
    const digits = value.replace(/[\s+()-]/g, '');
    return /^\d{5,20}$/.test(digits) ? `${digits}@s.whatsapp.net` : undefined;
  }
  const jid = jidNormalizedUser(value);
  return isPnUser(jid) || isLidUser(jid) ? jid : undefined;
}

/**
 * Which of `inputs` this number has chatted 1:1 with, matched on either addressing form.
 * A peer first seen only by LID and later by phone can have a row under each; both match
 * the LID and are summed into one answer. Inputs that match nothing are left out.
 */
export async function findKnownContacts(
  repository: Repository,
  ownerNumber: string,
  inputs: string[],
): Promise<KnownContact[]> {
  const jids = [...new Set(inputs.map(normalizePeer).filter(Boolean))];
  const known = new Map<string, KnownContact>();

  for (let i = 0; i < jids.length; i += LOOKUP_CHUNK) {
    const chunk = jids.slice(i, i + LOOKUP_CHUNK);
    const wanted = new Set(chunk);
    const rows = await repository.contactActivity.findMany({
      where: {
        ownerNumber,
        OR: [{ peerJid: { in: chunk } }, { peerLid: { in: chunk } }],
      },
      select: {
        peerJid: true,
        peerLid: true,
        firstAt: true,
        lastAt: true,
        fromMeCount: true,
        fromThemCount: true,
      },
    });
    for (const row of rows) {
      for (const jid of new Set([row.peerJid, row.peerLid])) {
        if (!jid || !wanted.has(jid)) continue;
        const current = known.get(jid);
        if (!current) {
          known.set(jid, {
            jid,
            firstAt: row.firstAt,
            lastAt: row.lastAt,
            fromMeCount: row.fromMeCount,
            fromThemCount: row.fromThemCount,
          });
          continue;
        }
        if (row.firstAt < current.firstAt) current.firstAt = row.firstAt;
        if (row.lastAt > current.lastAt) current.lastAt = row.lastAt;
        current.fromMeCount += row.fromMeCount;
        current.fromThemCount += row.fromThemCount;
      }
    }
  }

  return [...known.values()];
}

class ContactActivity {
  private buffer = new Map<string, Entry>();
  private repository?: Repository;
  private logger?: Logger;
  private timer?: ReturnType<typeof setInterval>;
  private flushing?: Promise<void>;
  private enabled = false;
  private totals = { rows: 0, statements: 0, retained: 0, dropped: 0 };
  private summaryAt = Date.now();

  get isEnabled(): boolean {
    return this.enabled;
  }

  start(repository: Repository, logger: Logger, enabled: boolean): void {
    if (!enabled) {
      logger.warn('contact activity disabled (CONTACT_ACTIVITY_ENABLED=false)');
      return;
    }
    if (this.timer) {
      return;
    }
    this.repository = repository;
    this.logger = logger;
    this.enabled = true;
    this.timer = setInterval(() => void this.flush(), FLUSH_MS);
    this.timer.unref();
  }

  /** Counts one 1:1 message. `messageType` is getContentType's; `at` is unix seconds. */
  record(
    ownerNumber: string | undefined,
    key: WAMessageKey,
    messageType: string,
    at: number,
    ownLid?: string,
    resolvePn?: LidToPn,
  ): void {
    if (!this.enabled || !ownerNumber || IGNORED_TYPES.has(messageType)) {
      return;
    }
    // Called from the socket's message path: a bad key must cost one count, never the batch.
    try {
      const peer = peerOf(key, ownerNumber, ownLid);
      if (!peer) {
        return;
      }
      const ms = at > 0 ? at * 1000 : Date.now();
      const fromMe = key.fromMe ? 1 : 0;
      this.merge({
        ownerNumber,
        peerJid: peer.peerJid,
        peerLid: peer.peerLid,
        firstAt: ms,
        lastAt: ms,
        fromMe,
        fromThem: 1 - fromMe,
        source: 'live',
        resolvePn: peer.peerJid === peer.peerLid ? resolvePn : undefined,
      });
    } catch (error) {
      this.logger?.debug('contact activity record skipped', { error: String(error) });
      return;
    }
    if (this.buffer.size >= FLUSH_AT_KEYS) {
      void this.flush();
    }
  }

  /**
   * One 1:1 chat from history sync: a sighting with its dates and no counts. `pnForLid` is the
   * sync's own LID→PN pairs; anything it misses is tried against `resolvePn` at flush.
   * Returns whether it was recorded (false for groups, channels, ourselves, or no timestamp).
   */
  recordHistoryChat(
    ownerNumber: string | undefined,
    chat: { id?: string | null; pnJid?: string | null; lidJid?: string | null } & Record<string, any>,
    ownLid?: string,
    pnForLid?: Map<string, string>,
    resolvePn?: LidToPn,
  ): boolean {
    if (!this.enabled || !ownerNumber || !chat?.id) {
      return false;
    }
    try {
      const sec = Math.max(
        unixSeconds(chat.conversationTimestamp),
        unixSeconds(chat.lastMessageRecvTimestamp),
      );
      if (!sec) {
        return false;
      }
      const id = jidNormalizedUser(chat.id);
      const alt = (jid?: string | null) => (jid ? jidNormalizedUser(jid) : undefined);
      const lid = isLidUser(id) ? id : alt(chat.lidJid);
      const pn =
        (isPnUser(id) ? id : alt(chat.pnJid)) ?? (lid ? pnForLid?.get(lid) : undefined);
      const peer = peerOf(
        { remoteJid: pn ?? id, remoteJidAlt: lid ?? undefined, fromMe: false },
        ownerNumber,
        ownLid,
      );
      if (!peer) {
        return false;
      }
      const ms = sec * 1000;
      this.merge({
        ownerNumber,
        peerJid: peer.peerJid,
        peerLid: peer.peerLid,
        firstAt: ms,
        lastAt: ms,
        fromMe: 0,
        fromThem: 0,
        source: 'history',
        resolvePn: peer.peerJid === peer.peerLid ? resolvePn : undefined,
      });
    } catch (error) {
      this.logger?.debug('contact activity history chat skipped', { error: String(error) });
      return false;
    }
    if (this.buffer.size >= FLUSH_AT_KEYS) {
      void this.flush();
    }
    return true;
  }

  /** Writes what is buffered. Resolves when done; never rejects. */
  flush(): Promise<void> {
    if (!this.flushing && this.buffer.size > 0) {
      this.flushing = this.write().finally(() => (this.flushing = undefined));
    }
    return this.flushing ?? Promise.resolve();
  }

  /** Shutdown: stop the timer and write everything still buffered. */
  async drain(): Promise<void> {
    clearInterval(this.timer);
    await this.flushing;
    await this.flush();
  }

  private merge(entry: Entry, into: Map<string, Entry> = this.buffer): void {
    const id = `${entry.ownerNumber}|${entry.peerJid}`;
    const current = into.get(id);
    if (!current) {
      into.set(id, entry);
      return;
    }
    current.firstAt = Math.min(current.firstAt, entry.firstAt);
    current.lastAt = Math.max(current.lastAt, entry.lastAt);
    current.fromMe += entry.fromMe;
    current.fromThem += entry.fromThem;
    current.peerLid ??= entry.peerLid;
    current.resolvePn ??= entry.resolvePn;
    if (current.source !== entry.source) current.source = 'live';
  }

  /**
   * Swaps LID-only peers for their phone jid where the account's LID store knows it, then
   * re-merges: two entries can become the same person, and one INSERT ... ON CONFLICT may not
   * touch a row twice. A failed or empty lookup leaves the entry under its LID.
   */
  private async resolve(entries: Entry[]): Promise<Entry[]> {
    const pending = entries.filter((e) => e.resolvePn && isLidUser(e.peerJid));
    if (!pending.length) {
      return entries;
    }
    await Promise.all(
      pending.map(async (e) => {
        try {
          const pn = await e.resolvePn!(e.peerJid);
          const jid = pn ? jidNormalizedUser(pn) : '';
          if (isPnUser(jid) && jid !== `${e.ownerNumber}@s.whatsapp.net`) {
            e.peerLid = e.peerJid;
            e.peerJid = jid;
          }
        } catch {
          // The socket may be gone by now; the LID row is still right, just not merged yet.
        }
      }),
    );
    const merged = new Map<string, Entry>();
    for (const e of entries) {
      e.resolvePn = undefined;
      this.merge(e, merged);
    }
    return [...merged.values()];
  }

  private async write(): Promise<void> {
    // Taken whole, so record() keeps filling a fresh buffer while this one is on the wire.
    const taken = [...this.buffer.values()];
    this.buffer = new Map();
    const entries = await this.resolve(taken);

    for (let i = 0; i < entries.length && this.enabled; i += ROWS_PER_STATEMENT) {
      const slice = entries.slice(i, i + ROWS_PER_STATEMENT);
      try {
        await this.upsert(slice);
        this.totals.rows += slice.length;
        this.totals.statements++;
      } catch (error) {
        if (isMissingTable(error)) {
          // Deployed ahead of its migration: stop instead of failing every 3s.
          this.enabled = false;
          clearInterval(this.timer);
          this.logger?.error('ContactActivity table missing - contact activity disabled');
          return;
        }
        this.retain(entries.slice(i));
        this.logger?.error('contact activity flush failed', {
          error: (error as Error)?.message ?? String(error),
          rows: entries.length - i,
        });
        return;
      }
      if (i + ROWS_PER_STATEMENT < entries.length) {
        await yieldToLoop();
      }
    }

    // A burst can fill the buffer again while this flush was on the wire.
    if (this.buffer.size >= FLUSH_AT_KEYS) {
      await this.write();
    }
    this.summarize();
  }

  private retain(entries: Entry[]): void {
    for (const entry of entries) {
      if (this.buffer.size >= MAX_RETAINED_KEYS) {
        this.totals.dropped++;
        continue;
      }
      this.merge(entry);
      this.totals.retained++;
    }
  }

  private async upsert(entries: Entry[]): Promise<void> {
    // Times go as ISO strings with their Z, so the session's time zone cannot shift them.
    const values = entries.map(
      (e) => Prisma.sql`(
        ${e.ownerNumber}, ${e.peerJid}, ${e.peerLid},
        ${new Date(e.firstAt).toISOString()}::timestamptz,
        ${new Date(e.lastAt).toISOString()}::timestamptz,
        ${e.fromMe}::int, ${e.fromThem}::int, ${e.source}, NOW()
      )`,
    );
    await this.repository!.$executeRaw`
      INSERT INTO "ContactActivity" AS c (
        "ownerNumber", "peerJid", "peerLid", "firstAt", "lastAt",
        "fromMeCount", "fromThemCount", "source", "updatedAt"
      )
      VALUES ${Prisma.join(values)}
      ON CONFLICT ("ownerNumber", "peerJid") DO UPDATE SET
        "peerLid" = COALESCE(EXCLUDED."peerLid", c."peerLid"),
        "firstAt" = LEAST(c."firstAt", EXCLUDED."firstAt"),
        "lastAt" = GREATEST(c."lastAt", EXCLUDED."lastAt"),
        "fromMeCount" = c."fromMeCount" + EXCLUDED."fromMeCount",
        "fromThemCount" = c."fromThemCount" + EXCLUDED."fromThemCount",
        "source" = COALESCE(c."source", EXCLUDED."source"),
        "updatedAt" = NOW()
    `;

    // Fold a row kept under a LID into the phone row that now names the same LID. Only when
    // this batch linked a LID to a phone, and only for those owners.
    const linked = entries.filter((e) => e.peerLid && e.peerJid !== e.peerLid);
    if (!linked.length) {
      return;
    }
    const owners = [...new Set(linked.map((e) => e.ownerNumber))];
    const lids = [...new Set(linked.map((e) => e.peerLid!))];
    // The rows above are already written: a failure here must not send them back for a retry
    // (that would count them twice). The LID row stays; a later link folds it.
    await this.repository!.$executeRaw`
      WITH gone AS (
        DELETE FROM "ContactActivity" l
        USING "ContactActivity" p
        WHERE l."ownerNumber" = ANY(${owners}::varchar[])
          AND l."peerJid" = ANY(${lids}::varchar[])
          AND p."ownerNumber" = l."ownerNumber"
          AND p."peerLid" = l."peerJid"
          AND p."peerJid" <> l."peerJid"
        RETURNING l."ownerNumber", l."peerJid", l."firstAt", l."lastAt",
          l."fromMeCount", l."fromThemCount", l."source"
      )
      UPDATE "ContactActivity" c SET
        "firstAt" = LEAST(c."firstAt", g."firstAt"),
        "lastAt" = GREATEST(c."lastAt", g."lastAt"),
        "fromMeCount" = c."fromMeCount" + g."fromMeCount",
        "fromThemCount" = c."fromThemCount" + g."fromThemCount",
        "updatedAt" = NOW()
      FROM gone g
      WHERE c."ownerNumber" = g."ownerNumber" AND c."peerLid" = g."peerJid"
        AND c."peerJid" <> g."peerJid"
    `.catch((error) =>
      this.logger?.warn('contact activity LID merge failed', {
        error: (error as Error)?.message ?? String(error),
      }),
    );
  }

  private summarize(): void {
    if (Date.now() - this.summaryAt < SUMMARY_MS) {
      this.logger?.debug('contact activity flushed', { ...this.totals });
      return;
    }
    this.logger?.info('contact activity flushed (last minute)', {
      ...this.totals,
      buffered: this.buffer.size,
    });
    this.totals = { rows: 0, statements: 0, retained: 0, dropped: 0 };
    this.summaryAt = Date.now();
  }
}

export const contactActivity = new ContactActivity();
