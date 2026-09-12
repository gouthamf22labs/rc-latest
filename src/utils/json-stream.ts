import type { Response } from 'express';
import { yieldToLoop } from './yield-to-loop';

// Flush to the socket, and yield, roughly every this many characters of JSON.
const FLUSH_CHARS = 256 * 1024;

/**
 * res.status(status).json(body) for a large array, without one blocking serialisation pass.
 *
 * fetchAllGroups for the largest accounts is tens of MB of JSON, and res.json() builds the
 * whole string synchronously before a byte leaves. This serialises one element at a time and
 * writes in slices, yielding between them, so other requests keep being served.
 *
 * The body is byte-for-byte what res.json() sends: JSON.stringify of an array is '[', each
 * element's JSON.stringify joined by ',', then ']', with elements that serialise to undefined
 * written as null. The only difference on the wire is chunked transfer instead of a
 * Content-Length and ETag, which none of our callers read.
 *
 * Non-arrays and tiny arrays go through res.json() unchanged.
 */
export async function sendJsonChunked(res: Response, status: number, body: unknown): Promise<void> {
  if (!Array.isArray(body) || body.length < 2) {
    res.status(status).json(body);
    return;
  }

  res.status(status);
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  // Backpressure waits share ONE drain listener and ONE close listener for the whole response.
  // Adding and removing a listener per wait leaks under the compression middleware: it
  // redirects res.on('drain') to its zlib stream but does not redirect removal, so every wait
  // left a listener behind on that stream (MaxListenersExceededWarning on large responses).
  let wake: (() => void) | undefined;
  const release = () => {
    const resume = wake;
    wake = undefined;
    resume?.();
  };
  res.on('drain', release);
  res.on('close', release);

  let part = '[';
  for (let i = 0; i < body.length; i++) {
    const item = JSON.stringify(body[i]);
    part += (i === 0 ? '' : ',') + (item === undefined ? 'null' : item);
    if (part.length >= FLUSH_CHARS) {
      // The caller went away; nothing left to serialise for.
      if (res.destroyed) return;
      const flushed = res.write(part);
      part = '';
      if (flushed) {
        await yieldToLoop();
      } else if (!res.destroyed) {
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    }
  }
  if (res.destroyed) return;
  res.end(part + ']');
}
