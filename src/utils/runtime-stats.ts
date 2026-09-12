import { Session } from 'inspector';
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
 *
 * Stall profile. Those lines showed a ~2.7s synchronous block every 3 minutes that no HTTP
 * request accounts for, and no timer in codechat, Baileys or the backend runs on that period.
 * So the V8 CPU profiler samples continuously (every CPU_PROFILE_INTERVAL_MS, default 10ms)
 * and is restarted each interval. When an interval's loop.max reaches STALL_MS, the longest
 * uninterrupted busy stretch in that interval's profile is summarised — by self time (where
 * the CPU actually was) and by total time (which callers it was under) — and logged as a
 * "stall profile" warning naming the functions. After CPU_PROFILE_MAX_REPORTS (default 20)
 * such reports the profiler stops for good, so it costs nothing once it has answered the
 * question. CPU_PROFILE_ON_STALL=false disables it. The in-process inspector session opens
 * no port.
 */
const MB = 1024 * 1024;
const STALL_MS = 1000;

// By name, not by value: the numbers are not the obvious 1/2/4/8 (major is 4).
const GC_KIND: Record<number, 'minor' | 'major' | 'incremental' | 'weakcb'> = {
  [constants.NODE_PERFORMANCE_GC_MINOR]: 'minor',
  [constants.NODE_PERFORMANCE_GC_MAJOR]: 'major',
  [constants.NODE_PERFORMANCE_GC_INCREMENTAL]: 'incremental',
  [constants.NODE_PERFORMANCE_GC_WEAKCB]: 'weakcb',
};

function envInt(name: string, fallback: number, min = 0): number {
  const parsed = Number.parseInt(process.env[name] ?? '', 10);
  return Number.isFinite(parsed) && parsed >= min ? parsed : fallback;
}

export function startRuntimeStats(logger: Logger): void {
  const intervalMs = envInt('RUNTIME_STATS_MS', 60_000);
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

  const profiler = startStallProfiler(logger);

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
    const stalled = stats.loopMaxMs >= STALL_MS;
    if (stalled) {
      logger.warn(line, stats);
    } else {
      logger.info(line, stats);
    }
    loop.reset();
    gc = { count: 0, totalMs: 0, maxMs: 0, minor: 0, major: 0, majorMs: 0, incremental: 0, weakcb: 0 };
    // After the reset: the profile covers the same interval the stats just described.
    void profiler?.cycle(stalled);
  }
}

type ProfileNode = {
  id: number;
  callFrame: { functionName: string; url: string; lineNumber: number };
  children?: number[];
};
type CpuProfile = {
  nodes: ProfileNode[];
  startTime: number;
  endTime: number;
  samples: number[];
  timeDeltas: number[];
};
type Ranked = { ms: number; label: string }[];

function startStallProfiler(logger: Logger) {
  if ((process.env.CPU_PROFILE_ON_STALL ?? 'true') === 'false') {
    return undefined;
  }
  const maxReports = envInt('CPU_PROFILE_MAX_REPORTS', 20);
  const samplingMs = envInt('CPU_PROFILE_INTERVAL_MS', 10, 1);
  if (maxReports === 0) {
    return undefined;
  }

  const session = new Session();
  try {
    session.connect();
  } catch (error) {
    logger.warn('cpu profiler unavailable', { error: String(error) });
    return undefined;
  }
  const post = <T = any>(method: string, params: object = {}) =>
    new Promise<T>((resolve, reject) =>
      session.post(method, params, (err, result) => (err ? reject(err) : resolve(result as T))),
    );

  let active = false;
  let cycling = false;
  let reports = 0;

  (async () => {
    await post('Profiler.enable');
    await post('Profiler.setSamplingInterval', { interval: samplingMs * 1000 });
    await post('Profiler.start');
    active = true;
  })().catch((error) => logger.warn('cpu profiler failed to start', { error: String(error) }));

  return {
    async cycle(stalled: boolean) {
      if (!active || cycling) return;
      cycling = true;
      try {
        const { profile } = await post<{ profile: CpuProfile }>('Profiler.stop');
        if (stalled) {
          const summary = summarizeLongestBusyWindow(profile);
          if (summary) {
            reports++;
            const fmt = (ranked: Ranked) => ranked.slice(0, 6).map((r) => `${r.ms}ms ${r.label}`).join(' | ');
            logger.warn(
              `stall profile busy=${summary.busyMs}ms self: ${fmt(summary.self)} || total: ${fmt(summary.total)}`,
              { ...summary, report: reports, maxReports },
            );
          }
        }
        if (reports >= maxReports) {
          active = false;
          await post('Profiler.disable');
          session.disconnect();
          logger.info(`cpu profiler stopped after ${reports} stall report(s) (CPU_PROFILE_MAX_REPORTS)`);
          return;
        }
        await post('Profiler.start');
      } catch (error) {
        logger.warn('cpu profiler cycle failed', { error: String(error) });
      } finally {
        cycling = false;
      }
    },
  };
}

/** "fn package/path.js:line", with node_modules/ and everything before dist/ trimmed. */
function frameLabel(frame: ProfileNode['callFrame']): string {
  const name = frame.functionName || '(anonymous)';
  if (!frame.url) return name; // native code, V8 builtins, (garbage collector), (program)
  let file = frame.url.replace(/^file:\/\//, '');
  const nodeModules = file.lastIndexOf('node_modules/');
  if (nodeModules >= 0) {
    file = file.slice(nodeModules + 'node_modules/'.length);
  } else {
    const dist = file.indexOf('/dist/');
    if (dist >= 0) file = file.slice(dist + 1);
  }
  return `${name} ${file}:${frame.lineNumber + 1}`;
}

/**
 * Attribute the longest run of consecutive non-idle samples — the stall itself, not the whole
 * interval's background work — by self time and by total (inclusive) time per function.
 */
export function summarizeLongestBusyWindow(profile: CpuProfile, top = 12) {
  const { samples, timeDeltas } = profile;
  if (!samples?.length) return undefined;

  const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
  const parent = new Map<number, number>();
  for (const n of profile.nodes) {
    for (const child of n.children ?? []) parent.set(child, n.id);
  }

  // Sample i was taken at at[i] and stands for the time until the next sample.
  const at: number[] = new Array(samples.length);
  let t = profile.startTime;
  for (let i = 0; i < samples.length; i++) {
    t += timeDeltas[i] ?? 0;
    at[i] = t;
  }
  const sampleMs = (i: number) => ((i + 1 < samples.length ? at[i + 1] : profile.endTime) - at[i]) / 1000;
  const idle = (i: number) => nodes.get(samples[i])?.callFrame.functionName === '(idle)';

  let best = { start: -1, end: -1, ms: 0 };
  let runStart = -1;
  let runMs = 0;
  for (let i = 0; i <= samples.length; i++) {
    if (i < samples.length && !idle(i)) {
      if (runStart < 0) {
        runStart = i;
        runMs = 0;
      }
      runMs += sampleMs(i);
    } else if (runStart >= 0) {
      if (runMs > best.ms) best = { start: runStart, end: i, ms: runMs };
      runStart = -1;
    }
  }
  if (best.start < 0) return undefined;

  const self = new Map<string, number>();
  const total = new Map<string, number>();
  for (let i = best.start; i < best.end; i++) {
    const ms = sampleMs(i);
    const leaf = nodes.get(samples[i]);
    if (!leaf) continue;
    const leafLabel = frameLabel(leaf.callFrame);
    self.set(leafLabel, (self.get(leafLabel) ?? 0) + ms);

    // A recursive function appears several times on one stack; count it once per sample.
    const onStack = new Set<string>();
    for (let id: number | undefined = leaf.id; id !== undefined; id = parent.get(id)) {
      const node = nodes.get(id);
      if (!node) break;
      const label = frameLabel(node.callFrame);
      if (label === '(root)' || onStack.has(label)) continue;
      onStack.add(label);
      total.set(label, (total.get(label) ?? 0) + ms);
    }
  }

  const rank = (m: Map<string, number>): Ranked =>
    [...m]
      .sort((a, b) => b[1] - a[1])
      .slice(0, top)
      .map(([label, ms]) => ({ ms: Math.round(ms), label }));

  return {
    busyMs: Math.round(best.ms),
    samples: best.end - best.start,
    self: rank(self),
    total: rank(total),
  };
}
