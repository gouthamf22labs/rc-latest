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

import { opendirSync, readdirSync, rmSync } from 'fs';
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

  public async loadInstance() {
    const set = async (name: string) => {
      const instance = await this.repository.instance.findUnique({
        where: { name },
      });
      if (!instance) {
        return this.eventEmitter.emit('remove.instance', instance);
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
      await init.connectToWhatsapp();
    };

    try {
      if (this.providerSession.ENABLED) {
        const [instances] = await this.providerFiles.allInstances();
        instances.data.forEach(async (name: string) => {
          await set(name);
        });

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
        .catch(() => []);

      const dbNames = new Set(dbInstances.filter((i) => i?.name).map((i) => i.name));

      for (const name of dbNames) {
        await set(name);
      }

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
            await set(dirent.name);
          }
        }
      } catch {
        // INSTANCE_DIR may not exist on a fresh container — that's fine
      }
    } catch (error) {
      this.logger.error(error);
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
