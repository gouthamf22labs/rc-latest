/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename monitor.service.ts                                                 │
 * │ Developed by: Cleber Wilson                                                  │
 * │ Creation date: Nov 27, 2022                                                  │
 * │ Contact: contato@codechat.dev                                                │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @copyright © Cleber Wilson 2022. All rights reserved.                        │
 * │ Licensed under the Apache License, Version 2.0                               │
 * │                                                                              │
 * │  @license "https://github.com/code-chat-br/whatsapp-api/blob/main/LICENSE"   │
 * │                                                                              │
 * │ You may not use this file except in compliance with the License.             │
 * │ You may obtain a copy of the License at                                      │
 * │                                                                              │
 * │    http://www.apache.org/licenses/LICENSE-2.0                                │
 * │                                                                              │
 * │ Unless required by applicable law or agreed to in writing, software          │
 * │ distributed under the License is distributed on an "AS IS" BASIS,            │
 * │ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     │
 * │                                                                              │
 * │ See the License for the specific language governing permissions and          │
 * │ limitations under the License.                                               │
 * │                                                                              │
 * │ @class                                                                       │
 * │ @constructs WAMonitoringService                                              │
 * │ @param {EventEmitter2} eventEmitter                                          │
 * │ @param {ConfigService} configService                                         │
 * │ @param {RepositoryBroker} repository                                         │
 * │ @param {RedisCache} cache                                                    │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { existsSync, opendirSync, readdirSync, rmSync } from 'fs';
import { WAStartupService } from './whatsapp.service';
import { INSTANCE_DIR } from '../../config/path.config';
import EventEmitter2 from 'eventemitter2';
import { join } from 'path';
import { Logger } from '../../config/logger.config';
import {
  ConfigService,
  Database,
  InstanceExpirationTime,
  ProviderSession,
} from '../../config/env.config';
import { Repository } from '../../repository/repository.service';
import { Instance } from '@prisma/client';
import { ProviderFiles } from '../../provider/sessions';
import { Websocket } from '../../websocket/server';

export class WAMonitoringService {
  constructor(
    private readonly eventEmitter: EventEmitter2,
    private readonly configService: ConfigService,
    private readonly repository: Repository,
    private readonly providerFiles: ProviderFiles,
    private readonly ws: Websocket,
    logger: Logger,
  ) {
    this.removeInstance();
    this.noConnection();

    Object.assign(this.db, configService.get<Database>('DATABASE'));

    this.logger = logger.setCtx('wa-monitoring-service');

    this.startReconnectSweep();
  }

  private reconnectSweepTimer?: ReturnType<typeof setInterval>;

  /**
   * Periodic reconciliation backstop for reconnection. The primary reconnect is
   * event-driven and continuous (WAStartupService.scheduleReconnect self-heals on
   * every drop), so this sweep normally finds nothing — it exists to catch
   * instances that somehow fell out of that loop (stuck 'close' with no pending
   * attempt). It replaces the external cron that used to poke /instance/connect,
   * so there are no HTTP round-trips or cross-service state. Skips healthy /
   * connecting instances and creds-lost ones (isAwaitingRescan); socket builds are
   * concurrency-capped inside connectToWhatsapp and staggered here so the sweep is
   * not a synchronized wave. Toggle with RECONNECT_SWEEP_ENABLED / RECONNECT_SWEEP_MS.
   */
  private startReconnectSweep() {
    if ((process.env.RECONNECT_SWEEP_ENABLED ?? 'true') === 'false') {
      this.logger.info('reconnect sweep disabled');
      return;
    }
    const parsedInterval = Number.parseInt(process.env.RECONNECT_SWEEP_MS ?? '', 10);
    const intervalMs =
      Number.isFinite(parsedInterval) && parsedInterval > 0
        ? parsedInterval
        : 45 * 60 * 1000;
    const parsedStagger = Number.parseInt(process.env.STARTUP_STAGGER_MS ?? '', 10);
    const staggerMs =
      Number.isFinite(parsedStagger) && parsedStagger >= 0 ? parsedStagger : 800;

    this.reconnectSweepTimer = setInterval(() => {
      this.reconnectSweep(staggerMs).catch((err) =>
        this.logger.error('reconnect sweep failed', err),
      );
    }, intervalMs);
    this.logger.info(
      `reconnect sweep every ${intervalMs}ms (stagger ${staggerMs}ms)`,
    );
  }

  private async reconnectSweep(staggerMs: number) {
    let kicked = 0;
    for (const [name, instance] of this.waInstances) {
      try {
        const status = instance.getInstance()?.status;
        // Healthy or mid-connect → leave it. requiresRescan (creds-lost, or a
        // gave-up flapper) → only a human QR re-scan fixes it, so never auto-poke
        // it here (that's what marks it OFFLINE + fires the storm).
        if (status?.state === 'open' || status?.state === 'connecting') continue;
        if (status?.requiresRescan) continue;
        await instance.connectToWhatsapp();
        kicked++;
      } catch (err) {
        this.logger.warn(`reconnect sweep: failed to reconnect ${name}`, err);
      }
      if (staggerMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, staggerMs));
      }
    }
    if (kicked > 0) {
      this.logger.info(`reconnect sweep: kicked ${kicked} closed instance(s)`);
    }
  }

  public stopReconnectSweep() {
    if (this.reconnectSweepTimer) {
      clearInterval(this.reconnectSweepTimer);
      this.reconnectSweepTimer = undefined;
    }
  }

  private readonly db: Partial<Database> = {};

  private readonly logger: Logger;
  public readonly waInstances = new Map<string, WAStartupService>();

  private readonly providerSession = Object.freeze(
    this.configService.get<ProviderSession>('PROVIDER'),
  );

  private readonly instanceDelTimeout = {};

  public addInstance(instanceName: string, instance: WAStartupService) {
    const currentInstance = this.waInstances.get(instanceName);
    if (currentInstance) {
      this.clearListeners(instanceName);
    }
    this.waInstances.set(instanceName, instance);
    this.delInstanceTime(instanceName);
  }

  public delInstanceTime(instance: string) {
    const time = this.configService.get<InstanceExpirationTime>(
      'INSTANCE_EXPIRATION_TIME',
    );
    if (typeof time === 'number' && time > 0) {
      if (this.instanceDelTimeout[instance]) {
        clearTimeout(this.instanceDelTimeout[instance]);
      }

      this.instanceDelTimeout[instance] = setTimeout(
        () => {
          const ref = this.waInstances.get(instance);
          const info = ref?.getInstance();
          if (info?.status.state !== 'open') {
            this.waInstances.delete(instance);
          }
          delete this.instanceDelTimeout[instance];
        },
        1000 * 60 * time,
      );
    }
  }

  private async cleaningUp({ name }: Instance) {
    this.clearListeners(name);
    if (this.providerSession?.ENABLED) {
      await this.providerFiles.removeSession(name);
    } else {
      // Remove DB session rows so the next connect forces a fresh login
      await this.repository.session
        .deleteMany({ where: { Instance: { name } } })
        .catch((err) => this.logger.warn('session-cleanup-failed', err));
      rmSync(join(INSTANCE_DIR, name), { recursive: true, force: true });
    }

    await this.repository.instance.update({
      where: { name },
      data: {
        connectionStatus: 'OFFLINE',
      },
    });
  }

  private clearListeners(instanceName: string) {
    try {
      // Cancel any pending reconnect timer first, or a removed instance can
      // resurrect itself by firing connectToWhatsapp after teardown.
      this.waInstances.get(instanceName)?.stopReconnect?.();
      const client = this.waInstances.get(instanceName)?.client;
      if (client?.ev) {
        client.ev.removeAllListeners('connection.update');
        client.ev.removeAllListeners('messages.upsert');
        client.ev.removeAllListeners('messages.update');
        client.ev.removeAllListeners('messaging-history.set');
        client.ev.removeAllListeners('contacts.upsert');
        client.ev.removeAllListeners('chats.upsert');
        client.ev.removeAllListeners('creds.update');
        client.ev.flush();
      }
      // Close the underlying socket too. Removing listeners alone leaves the old
      // WebSocket connected to WhatsApp; a replaced-but-still-live socket plus a
      // freshly created one = two sockets on the same creds = WhatsApp
      // 'conflict: replaced' → endless reconnect storm.
      try { client?.ws?.close?.(); } catch { /* best-effort */ }
      try { client?.end?.(undefined); } catch { /* best-effort */ }
      this.waInstances.delete(instanceName);
    } catch {
      this.logger.error(`Error clearing ${instanceName} instance listeners`);
    }
  }

  /**
   * Construct an instance and put it in `waInstances`, WITHOUT connecting.
   *
   * Registration is deliberately separate from (and always precedes) the connect:
   * reconnectSweep() can only heal instances that are in the map, so an instance
   * that fails to connect must still end up registered — otherwise it is dead
   * until something pokes /instance/connect from outside the process.
   */
  private async registerInstance(name: string): Promise<WAStartupService | undefined> {
    const instance = await this.repository.instance.findUnique({ where: { name } });
    if (!instance) {
      this.eventEmitter.emit('remove.instance', instance);
      return undefined;
    }
    const init = new WAStartupService(
      this.configService,
      this.eventEmitter,
      this.repository,
      this.providerFiles,
      this.ws,
    );
    await init.setInstanceName(name);
    this.addInstance(init.instanceName, init);
    return init;
  }

  /**
   * Restore a single instance end-to-end. NEVER throws.
   *
   * connectToWhatsapp() throws on any failure, and the boot loop used to await it
   * with no per-instance catch — so the first instance that failed aborted the
   * whole restore and every instance after it was never registered nor connected
   * (and, not being in `waInstances`, was invisible to reconnectSweep too).
   */
  private async restoreInstance(name: string): Promise<boolean> {
    let init: WAStartupService | undefined;
    try {
      init = await this.registerInstance(name);
    } catch (err) {
      this.logger.warn(`boot: failed to register instance ${name}`, err);
      return false;
    }
    if (!init) {
      return false;
    }
    try {
      await init.connectToWhatsapp();
      return true;
    } catch (err) {
      // Registered but not connected — reconnectSweep() will pick it up.
      this.logger.warn(`boot: failed to connect instance ${name}`, err);
      return false;
    }
  }

  /**
   * Restore many instances with bounded parallelism. connectLimiter already caps
   * concurrent socket construction process-wide, but the old loop awaited each
   * instance in turn, so effective concurrency was 1 and restoring a large
   * population took an extremely long time. Workers pull from a shared cursor so
   * a slow instance doesn't stall the others. Tune with STARTUP_CONCURRENCY.
   */
  private readonly pendingRestore = new Map<string, Promise<boolean>>();

  /**
   * Resolve an instance for an inbound request, restoring it on demand.
   *
   * InstanceGuard used to answer "does this instance exist?" from `waInstances`
   * alone, with a filesystem fallback that is dead for DB-backed sessions (those
   * never create an INSTANCE_DIR folder — only useMultiFileAuthState does). So an
   * instance absent from memory was reported as "does not exist or is not
   * connected" even though its session was intact in the DB. That happens on every
   * restart: main.ts starts listening immediately while loadInstance() restores in
   * the background, so sends landing in that window got a hard 400. It is also
   * terminal after a `remove.instance`, since reconnectSweep only walks the map.
   *
   * Here a DB session is authoritative: register + connect the instance and let
   * the request through, rather than failing a message whose session is fine.
   */
  public async ensureInstance(name: string): Promise<boolean> {
    if (this.waInstances.get(name)) {
      return true;
    }

    // Provider-files and legacy file-based sessions keep their original discovery.
    if (this.providerSession?.ENABLED) {
      const [keyExists] = await this.providerFiles.allInstances();
      return !!keyExists?.data?.includes(name);
    }

    // Collapse concurrent requests for the same instance onto one restore, or a
    // burst of scheduled sends would each build a socket on the same creds →
    // WhatsApp 'conflict: replaced' → reconnect storm.
    const inFlight = this.pendingRestore.get(name);
    if (inFlight) {
      return inFlight;
    }

    const restore = this.restoreOnDemand(name).finally(() =>
      this.pendingRestore.delete(name),
    );
    this.pendingRestore.set(name, restore);
    return restore;
  }

  private async restoreOnDemand(name: string): Promise<boolean> {
    let hasSession: boolean;
    try {
      hasSession = !!(await this.repository.instance.findFirst({
        where: { name, Session: { some: {} } },
        select: { name: true },
      }));
    } catch (err) {
      this.logger.error(`on-demand restore: DB lookup failed for ${name}`, err);
      return existsSync(join(INSTANCE_DIR, name));
    }

    if (!hasSession) {
      // No session rows — genuinely unpaired or deleted. Fall back to the legacy
      // filesystem check before declaring it gone.
      return existsSync(join(INSTANCE_DIR, name));
    }

    this.logger.warn(`on-demand restore: ${name} missing from memory, restoring`);
    await this.restoreInstance(name);

    // Registered is enough to let the request through: the guard never checked
    // connection state for instances already in the map either, and the send path
    // reports the real socket state far more accurately than this guard can.
    const restored = !!this.waInstances.get(name);
    if (restored) {
      await this.waitForOpen(name);
    }
    return restored;
  }

  /**
   * Best-effort grace period so the caller's send lands on a live socket instead
   * of one still handshaking. Never fails the request — if the instance does not
   * reach 'open' in time, the send path surfaces the real reason.
   */
  private async waitForOpen(name: string): Promise<void> {
    const parsed = Number.parseInt(process.env.ONDEMAND_OPEN_TIMEOUT_MS ?? '', 10);
    const timeoutMs = Number.isFinite(parsed) && parsed >= 0 ? parsed : 20 * 1000;
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const state = this.waInstances.get(name)?.getInstance()?.status?.state;
      if (state === 'open') {
        return;
      }
      if (state !== 'connecting') {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }

  private async restoreAll(names: string[]): Promise<void> {
    if (names.length === 0) {
      return;
    }
    const parsed = Number.parseInt(process.env.STARTUP_CONCURRENCY ?? '', 10);
    const workers = Number.isFinite(parsed) && parsed > 0 ? parsed : 10;

    let cursor = 0;
    let connected = 0;
    const worker = async () => {
      while (cursor < names.length) {
        const name = names[cursor++];
        if (await this.restoreInstance(name)) {
          connected++;
        }
      }
    };

    await Promise.all(
      Array.from({ length: Math.min(workers, names.length) }, () => worker()),
    );

    const failed = names.length - connected;
    this.logger.info(
      `boot: restored ${connected}/${names.length} instance(s)` +
        `${failed > 0 ? ` (${failed} not connected - sweep will retry)` : ''}`,
    );
  }

  public async loadInstance() {
    try {
      if (this.providerSession.ENABLED) {
        const [instances] = await this.providerFiles.allInstances();
        await this.restoreAll((instances.data as string[]) ?? []);
        return;
      }

      // Prefer DB-based discovery: reconnect all instances that have session keys
      // in the Session table (DB auth state). Falls back to filesystem scan for
      // any legacy file-based sessions.
      const dbInstances = await this.repository.instance
        .findMany({
          where: { Session: { some: {} } },
          select: { name: true },
        })
        .catch((err) => {
          this.logger.error('boot: failed to list instances from DB', err);
          return [] as { name: string }[];
        });

      const dbNames = new Set(dbInstances.filter((i) => i?.name).map((i) => i.name));
      const names = [...dbNames];

      // Also scan filesystem for any remaining file-based sessions not in DB
      try {
        const dir = opendirSync(INSTANCE_DIR, { encoding: 'utf-8' });
        for await (const dirent of dir) {
          if (dirent.isDirectory() && !dbNames.has(dirent.name)) {
            const files = readdirSync(join(INSTANCE_DIR, dirent.name), {
              encoding: 'utf-8',
            });
            if (files.length === 0) {
              rmSync(join(INSTANCE_DIR, dirent.name), { recursive: true, force: true });
              continue;
            }
            names.push(dirent.name);
          }
        }
      } catch {
        // INSTANCE_DIR may not exist on a fresh container — that's fine
      }

      await this.restoreAll(names);
    } catch (error) {
      this.logger.error('boot: instance restore aborted', error);
    }
  }

  private removeInstance() {
    this.eventEmitter.on('remove.instance', async (instance: Instance) => {
      try {
        if (!instance?.name) {
          return;
        }
        // Cancel any pending reconnect so the removed instance can't resurrect
        // itself by firing connectToWhatsapp after it's gone from the map.
        this.waInstances.get(instance.name)?.stopReconnect?.();
        this.waInstances
          .get(instance.name)
          ?.client?.ev.removeAllListeners('connection.update');
        this.waInstances.get(instance.name)?.client?.ev.flush();
        this.waInstances.delete(instance.name);
      } catch (error) {
        this.logger.error('remove-instance', { error });
      }

      try {
        await this.cleaningUp(instance);
      } finally {
        this.logger.warn(`Instance "${instance?.name}" - REMOVED`);
      }
    });
  }

  private noConnection() {
    this.eventEmitter.on('no.connection', async (instance: Instance) => {
      const waInstance = this.waInstances.get(instance.name);
      const info = waInstance?.getInstance();
      if (info?.status?.state !== 'open') {
        const del = this.configService.get<InstanceExpirationTime>(
          'INSTANCE_EXPIRATION_TIME',
        );
        if (del) {
          try {
            this.cleaningUp(instance);
          } catch (error) {
            this.logger.error('no-connection', {
              warn: 'Error deleting instance from memory.',
              error,
            });
          } finally {
            this.logger.warn(`Instance "${instance.name}" - NOT CONNECTION`);
          }
        }
      } else {
        this.logger.info(`Instance ${waInstance.instanceName} already connected!`);
      }
    });
  }
}
