/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename message.model.ts                                                   │
 * │ Developed by: Cleber Wilson                                                  │
 * │ Creation date: Dez 02, 2023                                                  │
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
 * │ @class Repository                                                            │
 * │ @type {ITypebotModel}                                                        │
 * │ @type {CreateLogs}                                                           │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

export class Query<T> {
  where?: T;
  sort?: 'asc' | 'desc';
  page?: number;
  offset?: number;
}

import { PrismaClient, Webhook } from '@prisma/client';
import { WebhookEvents } from '../whatsapp/dto/webhook.dto';
import {
  BadRequestException,
  InternalServerErrorException,
  NotFoundException,
} from '../exceptions';
import { Logger } from '../config/logger.config';
import { ConfigService, Database } from '../config/env.config';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';

type CreateLogs = {
  context: string;
  description?: string;
  type: 'error' | 'info' | 'warning' | 'log';
  content: any;
};

export class Repository extends PrismaClient {
  constructor(private readonly configService: ConfigService) {
    super({
      adapter: new PrismaPg(
        new pg.Pool({
          connectionString: process.env.DATABASE_URL,
          // 20, not 100. At 100 a single process could claim the server's entire
          // max_connections budget and starve every other client. PgBouncer now
          // sits in front and absorbs bursts (hundreds of cheap client
          // connections multiplexed onto ~5-9 real ones), so a large client-side
          // pool buys nothing: it only churns connect/disconnect traffic.
          max: 20,
          // The 5s drain that used to live here was written against `max: 100`,
          // where a burst really could hold the server's whole connection budget
          // idle. That premise died with PgBouncer: at MAX_CLIENT_CONN=2000 a
          // warm pool of 20 is 1% of the client budget, and in transaction mode a
          // server slot is held only for the length of a transaction, so 20 idle
          // clients lock out nobody. What the 5s drain did buy was a connect on
          // the hot path of nearly every query - the pool emptied faster than the
          // ~8 queries/s refilled it - and each of those logins pays a
          // scram-sha-256 handshake on PgBouncer's single thread. Cron ticks that
          // open 6-8 connections in the same millisecond serialize behind that
          // and can push one handshake past the connect budget, which is how
          // `Connection terminated due to connection timeout` got raised. Staying
          // warm for 30s removes those logins entirely.
          idleTimeoutMillis: 30000,
          // One budget, two errors (pg-pool/index.js):
          //  - "Connection terminated due to connection timeout": a new login to
          //    PgBouncer did not finish in time.
          //  - "timeout exceeded when trying to connect": all `max` clients were
          //    taken and none came back in time. Clients still mid-login count
          //    toward `max`, so if PgBouncer stops answering logins, 20 hung
          //    handshakes fill the pool and every other query gets this error.
          // Seeing only the second one means the pool is saturated; seeing both
          // together means PgBouncer (or the path to it) is not accepting logins.
          //
          // Kept above PgBouncer's QUERY_WAIT_TIMEOUT (10s): while PgBouncer queues
          // our queries it holds our clients checked out for up to 10s, and a
          // caller waiting for one of them should outlast that queue, not fail
          // first. The cost is that a hung login takes 15s to be abandoned.
          connectionTimeoutMillis: 15000,
          // Idle sockets on the overlay network get dropped silently; without
          // keepalives a pool that now stays warm would hand out dead ones.
          keepAlive: true,
        }),
        {
          onConnectionError(err) {
            throw new InternalServerErrorException(err);
          },
        },
      ),
    });
  }


  private readonly logger = new Logger(this.configService, 'repository');

  public async onModuleInit() {
    await this.$connect();
    this.logger.info('repository:prisma - ON');
  }

  public async onModuleDestroy() {
    await this.$disconnect();
    this.logger.warn('repository:prisma - OFF');
  }

  public async updateWebhook(
    webhookId: number,
    data: Partial<Pick<Webhook, 'url' | 'enabled'>> & { events?: WebhookEvents },
  ) {
    const find = await this.webhook.findUnique({
      where: {
        id: webhookId,
      },
      select: {
        id: true,
        url: true,
        enabled: true,
        events: true,
        instanceId: true,
      },
    });
    if (!find) {
      throw new NotFoundException(['Webhook not found', `Webhook id: ${webhookId}`]);
    }
    try {
      // The backend re-sends the full webhook config on every poll, so this runs
      // constantly and almost never changes anything. Merge the events in memory
      // and write once, and only when something differs - it used to issue one
      // UPDATE per event key (~25 round trips per call) whether or not it changed.
      const current = (find.events ?? null) as Record<string, boolean> | null;
      let events: Record<string, boolean> | undefined;
      if (data?.events) {
        const patch: Record<string, boolean> = {};
        for (const [key, value] of Object.entries(data.events)) {
          if (value === undefined) {
            continue;
          }
          patch[key] = value === true || (value as unknown) === 'true';
        }
        const changed =
          !current || Object.entries(patch).some(([key, value]) => current[key] !== value);
        if (changed) {
          events = { ...(current ?? {}), ...patch };
        }
      }

      const url = data?.url !== undefined && data.url !== find.url ? data.url : undefined;
      const enabled =
        data?.enabled !== undefined && data.enabled !== find.enabled ? data.enabled : undefined;

      if (url === undefined && enabled === undefined && events === undefined) {
        return find;
      }

      return await this.webhook.update({
        where: {
          id: webhookId,
        },
        data: { url, enabled, events },
        select: {
          id: true,
          url: true,
          enabled: true,
          events: true,
          instanceId: true,
        },
      });
    } catch (error) {
      throw new BadRequestException([error?.message, error?.stack]);
    }
  }

  public async createLogs(instance: string, logs: CreateLogs) {
    if (!this.configService.get<Database>('DATABASE').DB_OPTIONS?.LOGS) {
      return;
    }
    return await this.activityLogs.create({
      data: {
        ...logs,
        Instance: {
          connect: {
            name: instance,
          },
        },
      },
    });
  }
}
