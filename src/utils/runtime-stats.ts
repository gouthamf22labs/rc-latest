import { constants, monitorEventLoopDelay, PerformanceObserver } from 'perf_hooks';
import { getHeapStatistics } from 'v8';
import type { Logger } from '../config/logger.config';

/**
 * One log line per interval describing why the process is or is not responsive.
 *
 * codechat stalls for seconds at a time (a no-op 404 and an in-memory connectionState both
 * take 2-3.6s, back to back for ~40s, while wa-send-later-be on the same host answers in
 * 0.6s) at single-digit average CPU and 2.8 GB RSS. From outside that cannot be told apart:
 * a long synchronous handler and a V8 heap thrashing near its limit look identical. This
 * separates them:
 *
 * - loop.max / loop.p99: how long the event loop was actually blocked.
 * - gc.majorMs / gc.maxMs: whether that blocked time was garbage collection.
 * - heapUsed vs heapLimit: whether V8 is running out of room (GC thrash).
 * - external / arrayBuffers: memory outside the JS heap — the WASM signal arena lives
 *   there, and GC cannot reclaim it.
 *
 * Cheap: the delay histogram is a native timer and GC entries are pushed by V8.
 * RUNTIME_STATS_MS sets the interval (default 60000); 0 disables it.
 */
const MB = 1024 * 1024;

// By name, not by value: the numbers are not the obvious 1/2/4/8 (major is 4).
const GC_KIND: Record<number, 'minor' | 'major' | 'incremental' | 'weakcb'> = {
  [constants.NODE_PERFORMANCE_GC_MINOR]: 'minor',
  [constants.NODE_PERFORMANCE_GC_MAJOR]: 'major',
  [constants.NODE_PERFORMANCE_GC_INCREMENTAL]: 'incremental',
  [constants.NODE_PERFORMANCE_GC_WEAKCB]: 'weakcb',
};

export function startRuntimeStats(logger: Logger): void {
  const parsed = Number.parseInt(process.env.RUNTIME_STATS_MS ?? '', 10);
  const intervalMs = Number.isFinite(parsed) && parsed >= 0 ? parsed : 60_000;
  if (intervalMs === 0) {
    return;
  }

  const loop = monitorEventLoopDelay({ resolution: 20 });
  loop.enable();

  let gc = { count: 0, totalMs: 0, maxMs: 0, minor: 0, major: 0, majorMs: 0, incremental: 0, weakcb: 0 };
  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      const kind = GC_KIND[(entry as any).detail?.kind ?? (entry as any).kind] ?? 'minor';
      gc.count++;
      gc.totalMs += entry.duration;
      gc.maxMs = Math.max(gc.maxMs, entry.duration);
      gc[kind]++;
      if (kind === 'major') gc.majorMs += entry.duration;
    }
  });
  observer.observe({ entryTypes: ['gc'] });

  // Report from setImmediate, not the interval callback itself. After a long block every
  // overdue timer fires in the same pass, and this one can run before the delay monitor's
  // own timer records the late sample — reading and resetting there silently discarded
  // exactly the stall this exists to catch. The check phase runs after all of them.
  const timer = setInterval(() => setImmediate(report), intervalMs);
  timer.unref();

  function report() {
    const mem = process.memoryUsage();
    const heap = getHeapStatistics();
    const ns = (v: number) => Math.round(v / 1e6);
    const stats = {
      rssMb: Math.round(mem.rss / MB),
      heapUsedMb: Math.round(mem.heapUsed / MB),
      heapTotalMb: Math.round(mem.heapTotal / MB),
      heapLimitMb: Math.round(heap.heap_size_limit / MB),
      externalMb: Math.round(mem.external / MB),
      arrayBuffersMb: Math.round(mem.arrayBuffers / MB),
      loopP50Ms: ns(loop.percentile(50)),
      loopP99Ms: ns(loop.percentile(99)),
      loopMaxMs: ns(loop.max),
      gcCount: gc.count,
      gcTotalMs: Math.round(gc.totalMs),
      gcMaxMs: Math.round(gc.maxMs),
      gcMajor: gc.major,
      gcMajorMs: Math.round(gc.majorMs),
      gcMinor: gc.minor,
      gcIncremental: gc.incremental,
    };
    const line = `runtime rss=${stats.rssMb}MB heap=${stats.heapUsedMb}/${stats.heapLimitMb}MB external=${stats.externalMb}MB loopMax=${stats.loopMaxMs}ms gc=${stats.gcTotalMs}ms(max ${stats.gcMaxMs}ms, ${stats.gcMajor} major)`;
    if (stats.loopMaxMs >= 1000) {
      logger.warn(line, stats);
    } else {
      logger.info(line, stats);
    }
    loop.reset();
    gc = { count: 0, totalMs: 0, maxMs: 0, minor: 0, major: 0, majorMs: 0, incremental: 0, weakcb: 0 };
  }
}
