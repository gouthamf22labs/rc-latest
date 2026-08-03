/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @filename 504.exception.ts                                                   │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @class                                                                       │
 * │ @constructs GatewayTimeoutException                                          │
 * │ @param {any[]} objectError                                                   │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ Raised when a handler outruns the server's own deadline. Distinct from 503:  │
 * │ a 503 means the work never started, a 504 means it started and its outcome   │
 * │ is unknown — the operation may yet complete. Callers must not treat a 504 as │
 * │ proof of failure.                                                            │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { HttpStatus } from '../app.module';

export class GatewayTimeoutException {
  constructor(...objectError: any[]) {
    throw {
      status: HttpStatus.GATEWAY_TIMEOUT,
      error: 'Gateway Timeout',
      message: objectError.length > 0 ? objectError : undefined,
      // Explicit so callers can branch on it instead of parsing prose: the work
      // was still running when we answered.
      inFlight: true,
    };
  }
}
