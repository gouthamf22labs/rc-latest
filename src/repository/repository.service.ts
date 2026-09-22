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

import { Prisma, PrismaClient, Webhook } from '@prisma/client';
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
          // 10s, matching PgBouncer's QUERY_WAIT_TIMEOUT, so a transient stall
          // queues instead of destroying the socket mid-handshake. Note this
          // covers establishing a connection, not waiting for a free slot on a
          // saturated pool - that path raises its own error and is unaffected.
          connectionTimeoutMillis: 10000,
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
    data: Partial<Webhook> & { events?: WebhookEvents },
  ) {
    const find = await this.webhook.findUnique({
      where: {
        id: webhookId,
      },
    });
    if (!find) {
      throw new NotFoundException(['Webhook not found', `Webhook id: ${webhookId}`]);
    }
    try {
      for await (const [key, value] of Object.entries(data?.events)) {
        if (value === undefined) {
          continue;
        }

        if (!find?.events) {
          break;
        }

        const k = `ARRAY['${key}']`;
        const v = `to_jsonb(${value as string}::boolean)`;

        await this.$queryRaw(
          Prisma.sql`UPDATE "Webhook" SET events = jsonb_set(events, ${Prisma.raw(
            k,
          )}, ${Prisma.raw(v)}) WHERE id = ${webhookId}`,
        );
      }

      const updated = await this.webhook.update({
        where: {
          id: webhookId,
        },
        data: {
          url: data?.url,
          enabled: data?.enabled,
          events: !find?.events ? data?.events : undefined,
        },
        select: {
          id: true,
          url: true,
          enabled: true,
          events: true,
          instanceId: true,
        },
      });

      return updated;
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
