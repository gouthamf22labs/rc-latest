/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename abstract.router.ts                                                 │
 * │ Developed by: Cleber Wilson                                                  │
 * │ Creation date: Jul 17, 2022                                                  │
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
 * │ @type {DataValidate}                                                         │
 * │ @constant logger                                                             │
 * │                                                                              │
 * │ @abstract @class RouterBroker                                                │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { InstanceDto } from '../whatsapp/dto/instance.dto';
import { JSONSchema7 } from 'json-schema';
import { Request } from 'express';
import { validate } from 'jsonschema';
import { BadRequestException, GatewayTimeoutException } from '../exceptions';
import { Logger } from '../config/logger.config';
import { GroupJid } from '../whatsapp/dto/group.dto';
import { ConfigService } from '../config/env.config';

export class DataValidate<T> {
  request: Request;
  schema: JSONSchema7;
  execute: (instance: InstanceDto, data: T, file?: Express.Multer.File) => Promise<any>;
}

const logger = new Logger(new ConfigService(), 'validate');

export function routerPath(path: string, param = true) {
  let route = '/' + path;
  if (param) {
    route += '/:instanceName';
  }

  return route;
}

export async function dataValidate<T>(args: DataValidate<T>) {
  const { request, schema, execute } = args;

  const body = request.body ?? {};
  const instance = request.params as unknown as InstanceDto;

  const isNotEmptyQuery = Object.keys(request?.query ?? {}).length > 0;

  if (isNotEmptyQuery) {
    Object.assign(instance, request.query);
  }

  if (request.originalUrl.includes('/instance/create')) {
    Object.assign(instance, body);
  }

  if (
    isNotEmptyQuery &&
    ['get', 'delete', 'patch'].includes(request.method.toLowerCase())
  ) {
    Object.assign(body, request.query);
  }

  const v = schema ? validate(body, schema) : { valid: true, errors: [] };

  if (!v.valid) {
    const message: any[] = v.errors.map(({ property, stack, schema }) => {
      let message: string;
      if (schema['description']) {
        message = schema['description'];
      } else {
        message = stack.replace('instance.', '');
      }
      return {
        property: property.replace('instance.', ''),
        message,
      };
    });
    throw new BadRequestException(...message);
  }

  return await execute(instance, body, request?.file);
}

/**
 * dataValidate with a server-side deadline.
 *
 * Without one, a handler that stalls — a socket reporting 'open' while WhatsApp
 * answers nothing — simply never responds, and the caller's own HTTP timeout
 * fires instead. That is the worst outcome available: the caller aborts with no
 * status code, cannot distinguish "never sent" from "sent and we lost the
 * receipt", and typically records a failure for a message that was delivered.
 *
 * So the API answers first, with a 504 that says so. The deadline must stay
 * below the caller's timeout to be worth anything (wa-send-later-be posts sends
 * with a 120s axios timeout, hence the 90s default). Tune with SEND_DEADLINE_MS.
 *
 * The handler is NOT cancelled — Node offers no way to unwind it — so the send
 * may still land. That is precisely what `inFlight` on the response body says,
 * and why a 504 must never be treated as proof of failure.
 */
export async function dataValidateWithDeadline<T>(args: DataValidate<T>) {
  const parsed = Number.parseInt(process.env.SEND_DEADLINE_MS ?? '', 10);
  const timeoutMs = Number.isFinite(parsed) && parsed > 0 ? parsed : 90 * 1000;

  const work = dataValidate<T>(args);
  // The race stops observing `work` once the deadline wins; without this a later
  // rejection would surface as an unhandled rejection and take the process down.
  work.catch(() => undefined);

  const timedOut = Symbol('deadline');
  let timer: NodeJS.Timeout;
  try {
    const outcome = await Promise.race([
      work,
      new Promise<typeof timedOut>((resolve) => {
        timer = setTimeout(() => resolve(timedOut), timeoutMs);
      }),
    ]);

    if (outcome !== timedOut) {
      return outcome;
    }

    logger.error(
      `deadline exceeded after ${timeoutMs}ms: ${args.request.method} ${args.request.originalUrl}`,
    );
    // Constructs and throws — see the exception classes in src/exceptions.
    new GatewayTimeoutException(
      `Request exceeded the server deadline of ${timeoutMs}ms`,
      'The operation was still running and may still complete - do not treat this as a confirmed failure',
    );
    // Unreachable: the constructor above always throws. TypeScript cannot see
    // that, so give its control-flow analysis something to terminate on.
    throw new Error(`deadline exceeded after ${timeoutMs}ms`);
  } finally {
    clearTimeout(timer);
  }
}

export async function groupValidate<T>(args: DataValidate<T>) {
  const { request, schema, execute } = args;

  const groupJid = request.query as unknown as GroupJid;

  if (!groupJid?.groupJid) {
    throw new BadRequestException(
      'The group id needs to be informed in the query',
      'ex: "groupJid=120362@g.us"',
    );
  }

  const instance = request.params as unknown as InstanceDto;
  const body = request.body ?? {};

  Object.assign(body, groupJid);

  const v = validate(body, schema);

  if (!v.valid) {
    const message: any[] = v.errors.map(({ property, stack, schema }) => {
      let message: string;
      if (schema['description']) {
        message = schema['description'];
      } else {
        message = stack.replace('instance.', '');
      }
      return {
        property: property.replace('instance.', ''),
        message,
      };
    });
    logger.trace('', message);
    throw new BadRequestException(...message);
  }

  return await execute(instance, body);
}
