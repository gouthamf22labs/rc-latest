/**
 * Jittered exponential backoff for per-instance reconnect retries.
 *
 * WhatsApp closes sockets with 428 (connectionClosed) when an account
 * reconnects too aggressively. A flat retry (always 3s) keeps a flapping
 * instance hammering the same account on a fixed cadence, which re-triggers
 * 428 — a self-sustaining loop. Exponential growth thins repeated failures,
 * and the random jitter de-synchronizes instances that all dropped together.
 *
 *   attempt 0 → ~base, 1 → ~2·base, 2 → ~4·base … capped at maxDelayMs,
 *   each result multiplied by a random factor in [1 - jitter, 1 + jitter].
 */
export function backoffDelay(
  attempt: number,
  baseMs = 3000,
  maxDelayMs = 60000,
  jitter = 0.3,
): number {
  const exp = Math.min(maxDelayMs, baseMs * 2 ** Math.max(0, attempt));
  const factor = 1 - jitter + Math.random() * (2 * jitter);
  return Math.round(exp * factor);
}
