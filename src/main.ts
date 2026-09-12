/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename main.ts                                                            │
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
 * | @function bootstrap @param undefined                                         │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { ConfigService, HttpServer } from './config/env.config';
import { onUnexpectedError } from './config/error.config';
import { Logger } from './config/logger.config';
import { AppModule } from './app.module';

// Baileys registers one process.on('exit') listener per socket connection.
// With many concurrent instances this exceeds the default limit of 10.
process.setMaxListeners(0);

const context = new Map<string, any>();

export async function bootstrap() {
  await AppModule(context);

  const configService = context.get('module:config') as ConfigService;

  const logger = new Logger(configService, 'server');

  context.get('module:logger').info('initialized');
  context.set('server:logger', logger);

  const httpServer = configService.get<HttpServer>('SERVER');

  context.get('app').listen(httpServer.PORT, () => {
    logger.log.info('HTTP' + ' - ON: ' + httpServer.PORT);
    logger.log.info(
      `
        ..
        .       Swagger Docs
        . http://localhost:${httpServer.PORT}/docs
        . https://${process.env?.API_BACKEND || 'no-value'}/docs
        .. `.replace(/^ +/gm, '  '),
    );
  });

  onUnexpectedError(configService);
}

bootstrap();

/**
 * Graceful shutdown.
 *
 * SIGTERM is the signal that actually matters: it is what Docker and the process manager
 * send on a deploy, and it was not handled at all — only SIGINT was. So every deploy
 * killed the container with its WhatsApp sockets still connected, the replacement
 * reconnected the same credentials, and WhatsApp resolved the overlap by terminating the
 * older connections with `conflict: replaced` — ~60 instances inside eight seconds,
 * each alerting its user and each leaking the signal repo of the socket it churned.
 *
 * Closing the sockets first turns that into a handover. It is not a logout: sockets are
 * closed, credentials are left alone, and the replacement restores from them.
 *
 * Bounded, and exits by itself. A handler that only stops work would leave the process
 * alive — registering one suppresses Node's default exit-on-signal — so it would be
 * SIGKILLed mid-drain, which is the outage this exists to avoid.
 */
const SHUTDOWN_DEADLINE_MS = Number.parseInt(process.env.SHUTDOWN_DEADLINE_MS ?? '', 10) || 10_000;
// A close frame still has to reach WhatsApp after ws.close() returns; exiting in the same
// tick would drop it and leave the socket live on their side, which is the very overlap
// being avoided here.
const SOCKET_FLUSH_MS = Number.parseInt(process.env.SHUTDOWN_FLUSH_MS ?? '', 10) || 500;

let shuttingDown = false;

async function onShutdownSignal(signal: string) {
  // Deploys can deliver SIGTERM then SIGKILL, and a stuck drain can see two signals.
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  // Hard ceiling, armed before any awaiting: whatever happens below, this process exits
  // well inside the orchestrator's grace period rather than being killed mid-drain.
  const deadline = setTimeout(() => process.exit(0), SHUTDOWN_DEADLINE_MS);
  deadline.unref();

  try {
    context.get('server:logger')?.warn(`${signal} received - draining`);
    // Before the drain: a reconnect timer firing mid-drain must not open a new socket.
    context.get('module:socketLease')?.close?.();
    const closed = context.get('module:monitor')?.shutdown?.() ?? 0;
    context.get('server:logger')?.warn(`closed ${closed} whatsapp socket(s)`);
    await new Promise((resolve) => setTimeout(resolve, SOCKET_FLUSH_MS));
    // Sockets are closed and flushed, so the successor can take over now instead of
    // waiting out the lease TTL.
    await context.get('module:socketLease')?.release?.();
  } catch (error) {
    context.get('server:logger')?.error(['shutdown drain failed', error]);
  }

  try {
    context.get('module:provider')?.onModuleDestroy();
    context.get('module:repository')?.onModuleDestroy();
    context.get('module:logger')?.warn('APP MODULE - OFF');
    context.get('server:logger')?.warn('HTTP - OFF');
  } catch {
    /* never let teardown logging hold the exit */
  }

  clearTimeout(deadline);
  process.exit(0);
}

process.on('SIGTERM', () => void onShutdownSignal('SIGTERM'));
process.on('SIGINT', () => void onShutdownSignal('SIGINT'));
