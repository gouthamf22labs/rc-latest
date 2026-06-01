/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename logger.config.ts                                                   │
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
 * │ @function formatDateLog @param {Number} timestamp                            │
 * │ @enum {Color} @enum {Command} @enum {Level} @enum {Type} @enum {Background}  │
 * │                                                                              │
 * │ @class                                                                       │
 * │ @constructs Logger @param {String} context                                   │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import pino from 'pino';
import { ConfigService, Log } from './env.config';
import { join } from 'node:path';

/**
 * Shared root pino instance — built ONCE for the whole process.
 *
 * A pino `transport` does not log in-process; it spawns a dedicated worker
 * thread (via thread-stream). Building a fresh pino() inside the Logger
 * constructor meant every `new Logger()` / `setCtx()` leaked one worker
 * thread that was never terminated. With a new WAStartupService created per
 * instance and per `/instance/connect`, threads grew without bound (6k+ →
 * ~90 GB RSS). We now create the transport once and derive every context via
 * `.child({ context })`, which shares the single worker thread.
 *
 * @author goutham — Jun 01, 2026 — fix unbounded logger worker-thread leak
 */
let rootLogger: pino.Logger | undefined;

function getRootLogger(configService: ConfigService): pino.Logger {
  if (!rootLogger) {
    rootLogger = pino({
      level: configService.get<Log>('LOG').LEVEL,
      timestamp: pino.stdTimeFunctions.isoTime,
      transport: configService.get<boolean>('PRODUCTION')
        ? {
            target: 'pino/file',
            options: {
              destination: join(process.cwd(), 'logs', 'record'),
              mkdir: true,
              append: true,
              sync: false,
            },
          }
        : {
            target: 'pino-pretty',
            options: {
              colorize: configService.get<Log>('LOG').COLOR,
              translateTime: 'SYS:standard',
              levelFirst: true,
              singleLine: true,
              ignore: 'pid,hostname',
            },
          },
    });
  }
  return rootLogger;
}

export class Logger {
  constructor(
    private readonly configService: ConfigService,
    private readonly context = 'Logger',
  ) {
    // Reuse the single shared transport — no new worker thread per instance.
    this.logger = getRootLogger(configService);
  }

  private readonly logger: pino.Logger;

  setCtx(ctx: string) {
    return new Logger(this.configService, ctx);
  }

  get log() {
    return this.logger;
  }

  info(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).info(msg);
  }

  warn(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).warn(msg);
  }

  debug(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).debug(msg);
  }

  error(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).error(msg);
  }

  fatal(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).fatal(msg);
  }

  trace(msg: string, props?: Record<string, any>) {
    this.logger.child({ context: this.context, ...props }).trace(msg);
  }
}
