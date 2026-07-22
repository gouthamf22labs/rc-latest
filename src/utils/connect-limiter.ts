/**
 * Global concurrency gate for WhatsApp socket construction.
 *
 * Each makeWASocket() spins up a WASM Signal repository (whatsapp-rust-bridge).
 * Those Rust objects live in WebAssembly linear memory, are freed only lazily by
 * FinalizationRegistry, and the WASM arena never returns pages to the OS — so RSS
 * ratchets to the *peak* number of signal repos built at once. Bursts of connects
 * (boot-time loadInstance, backend connect-polling) are what push that peak up.
 *
 * This semaphore caps how many sockets construct concurrently, smoothing bursts so
 * GC/FinalizationRegistry can reclaim churned sockets before the next wave and the
 * WASM high-water mark stays bounded. It is process-global (shared across every
 * instance) on purpose. Tune via CONNECT_CONCURRENCY (default 5).
 */
class Semaphore {
  private queue: Array<() => void> = [];
  private active = 0;

  constructor(private readonly max: number) {}

  async acquire(): Promise<void> {
    if (this.active < this.max) {
      this.active++;
      return;
    }
    // No free slot — wait for a release() to hand one off directly.
    return new Promise<void>((resolve) => this.queue.push(resolve));
  }

  release(): void {
    const next = this.queue.shift();
    if (next) {
      // Hand the slot straight to the next waiter; active count is unchanged.
      next();
    } else {
      this.active--;
    }
  }

  get inFlight(): number {
    return this.active;
  }

  get waiting(): number {
    return this.queue.length;
  }
}

const parsed = Number.parseInt(process.env.CONNECT_CONCURRENCY ?? '', 10);
const CONNECT_CONCURRENCY = Number.isFinite(parsed) && parsed > 0 ? parsed : 5;

export const connectLimiter = new Semaphore(CONNECT_CONCURRENCY);
