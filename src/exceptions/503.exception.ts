/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @filename 503.exception.ts                                                   │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @class                                                                       │
 * │ @constructs ServiceUnavailableException                                      │
 * │ @param {any[]} objectError                                                   │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ Raised when the API cannot answer a request because a dependency it needs to │
 * │ answer it is down — not because the request itself is wrong. Callers must be │
 * │ able to tell these apart: a 400 means "stop retrying, the instance is gone", │
 * │ a 503 means "retry, we could not tell".                                      │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { HttpStatus } from '../app.module';

export class ServiceUnavailableException {
  constructor(...objectError: any[]) {
    throw {
      status: HttpStatus.SERVICE_UNAVAILABLE,
      error: 'Service Unavailable',
      message: objectError.length > 0 ? objectError : undefined,
    };
  }
}
