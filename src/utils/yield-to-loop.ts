/**
 * Resolve on the next event-loop turn, after pending I/O callbacks.
 *
 * Long synchronous passes over large group payloads await this between slices so HTTP
 * requests, socket frames and timers queued behind them get to run. It changes when the
 * work happens, never what it produces.
 */
export const yieldToLoop = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
