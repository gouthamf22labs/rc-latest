/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @filename presence.service.ts                                                │
 * │ Presence cache + online-trigger watches for a single WhatsApp instance.      │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ WhatsApp has no "get last seen" request. The only source of a contact's      │
 * │ presence is the `presence.update` stream, and that stream only carries a     │
 * │ contact once `presenceSubscribe(jid)` has been sent on the current socket.   │
 * │ Two consequences drive this whole file:                                      │
 * │                                                                              │
 * │  1. Reads have to be served from a cache fed by the event stream — a lookup  │
 * │     is "subscribe, then wait for the first push".                            │
 * │  2. Subscriptions are socket-scoped and lapse over time, so every watch must │
 * │     be re-subscribed on reconnect and refreshed periodically, throttled so a │
 * │     freshly-opened socket isn't hit with hundreds of nodes at once.          │
 * │                                                                              │
 * │ `lastSeen` is absent whenever the contact's privacy denies it (Baileys maps  │
 * │ `last="deny"` to undefined) — that is reported as `lastSeenHidden`, not as   │
 * │ an error.                                                                    │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { WAPresence } from '@whiskeysockets/baileys';

export type PresenceSnapshot = {
  jid: string;
  online: boolean;
  lastKnownPresence: WAPresence;
  /** Unix seconds. Null when the contact hides it, or when it was never sent. */
  lastSeen: number | null;
  lastSeenHidden: boolean;
  groupOnlineCount?: number;
  /** ms epoch of the update that produced this snapshot */
  updatedAt: number;
};

export type PresenceWatch = {
  watchId: string;
  jid: string;
  number: string;
  createdAt: number;
  expiresAt: number;
  /**
   * Fire immediately when the contact is already online at registration time.
   * "Send the moment they come online" is satisfied by "they are online now",
   * so this defaults to true; set false to require an actual offline→online
   * transition.
   */
  fireIfAlreadyOnline: boolean;
};

export type PresenceWatchInput = {
  watchId: string;
  jid: string;
  number: string;
  ttlSeconds: number;
  fireIfAlreadyOnline?: boolean;
};

type WatcherLogger = {
  info: (msg: string, props?: Record<string, any>) => void;
  warn: (msg: string, props?: Record<string, any>) => void;
  error: (msg: any, props?: Record<string, any>) => void;
};

export type PresenceWatcherDeps = {
  /** Sends `presenceSubscribe` on the live socket. May reject; the watcher swallows. */
  subscribe: (jid: string) => Promise<unknown>;
  /** A watch matched: the contact is online. One-shot — the watch is already removed. */
  onOnline: (watches: PresenceWatch[], snapshot: PresenceSnapshot) => void;
  /** TTL elapsed without the contact ever coming online. */
  onExpire: (watches: PresenceWatch[]) => void;
  logger: WatcherLogger;
};

/** Gap between subscribe nodes so a reconnect doesn't burst the socket. */
const SUBSCRIBE_GAP_MS = 150;
/** WhatsApp lets subscriptions lapse; re-assert them on this cadence. */
const REFRESH_INTERVAL_MS = 5 * 60 * 1000;
/** How often expired watches are swept. */
const SWEEP_INTERVAL_MS = 30 * 1000;
/** Cap on cached snapshots per instance — presence data is disposable. */
const MAX_CACHED_SNAPSHOTS = 2000;
/** Hard cap on live watches per instance; mass-subscribing is a ban signal. */
export const MAX_WATCHES_PER_INSTANCE = 500;

export class PresenceWatcher {
  constructor(private readonly deps: PresenceWatcherDeps) {}

  private readonly snapshots = new Map<string, PresenceSnapshot>();
  private readonly watches = new Map<string, PresenceWatch>();
  /** jid → watchIds, so an incoming update resolves its watches without a scan. */
  private readonly watchesByJid = new Map<string, Set<string>>();
  /** One-shot resolvers for `waitForUpdate`. */
  private readonly waiters = new Map<string, Array<(s: PresenceSnapshot) => void>>();

  private readonly subscribeQueue = new Set<string>();
  private draining = false;
  private connected = false;
  private refreshTimer?: NodeJS.Timeout;
  private sweepTimer?: NodeJS.Timeout;

  // ─── lifecycle ──────────────────────────────────────────────────────────────

  public onConnectionOpen() {
    this.connected = true;
    // Subscriptions die with the old socket — re-assert every watch, throttled.
    for (const watch of this.watches.values()) {
      this.subscribeQueue.add(watch.jid);
    }
    void this.drain();
    this.ensureTimers();
  }

  public onConnectionClosed() {
    this.connected = false;
    // Keep the watches: they are re-subscribed on the next open. Drop only the
    // queue, whose entries would fail against a dead socket.
    this.subscribeQueue.clear();
  }

  /** Instance is going away for good — release timers and pending state. */
  public dispose() {
    this.connected = false;
    this.stopTimers();
    this.subscribeQueue.clear();
    this.watches.clear();
    this.watchesByJid.clear();
    this.snapshots.clear();
    this.waiters.clear();
  }

  // ─── reads ──────────────────────────────────────────────────────────────────

  public getSnapshot(jid: string): PresenceSnapshot | undefined {
    return this.snapshots.get(jid);
  }

  /**
   * Subscribes and resolves on the first `presence.update` for `jid`, or null on
   * timeout. A timeout is a normal outcome: WhatsApp stays silent when the
   * contact hides both last-seen and online status.
   */
  public async requestSnapshot(jid: string, timeoutMs: number): Promise<PresenceSnapshot | null> {
    const waiter = this.waitForUpdate(jid, timeoutMs);
    try {
      await this.deps.subscribe(jid);
    } catch (error) {
      this.deps.logger.warn(`presence subscribe failed for ${jid}: ${error?.message ?? error}`);
    }
    return waiter;
  }

  private waitForUpdate(jid: string, timeoutMs: number): Promise<PresenceSnapshot | null> {
    return new Promise((resolve) => {
      const list = this.waiters.get(jid) ?? [];
      const timer = setTimeout(() => {
        this.removeWaiter(jid, onUpdate);
        resolve(null);
      }, timeoutMs);

      const onUpdate = (snapshot: PresenceSnapshot) => {
        clearTimeout(timer);
        resolve(snapshot);
      };

      list.push(onUpdate);
      this.waiters.set(jid, list);
    });
  }

  private removeWaiter(jid: string, fn: (s: PresenceSnapshot) => void) {
    const list = this.waiters.get(jid);
    if (!list) {
      return;
    }
    const index = list.indexOf(fn);
    if (index >= 0) {
      list.splice(index, 1);
    }
    if (list.length === 0) {
      this.waiters.delete(jid);
    }
  }

  // ─── watches ────────────────────────────────────────────────────────────────

  public get watchCount() {
    return this.watches.size;
  }

  public listWatches(): PresenceWatch[] {
    return [...this.watches.values()];
  }

  /**
   * Registers (or refreshes) a one-shot watch. Idempotent on `watchId`, so the
   * caller can re-register on a cadence to survive a restart on either side.
   * Returns the watch, or fires it straight away when the contact is already
   * known to be online.
   */
  public watch(input: PresenceWatchInput): { watch: PresenceWatch; firedImmediately: boolean } {
    const existing = this.watches.get(input.watchId);
    if (!existing && this.watches.size >= MAX_WATCHES_PER_INSTANCE) {
      throw new Error(
        `presence watch limit reached for this instance (${MAX_WATCHES_PER_INSTANCE})`,
      );
    }

    const watch: PresenceWatch = {
      watchId: input.watchId,
      jid: input.jid,
      number: input.number,
      createdAt: existing?.createdAt ?? Date.now(),
      expiresAt: Date.now() + input.ttlSeconds * 1000,
      fireIfAlreadyOnline: input.fireIfAlreadyOnline ?? true,
    };

    // A re-registration may move the jid; clear the old index entry first.
    if (existing && existing.jid !== watch.jid) {
      this.unindex(existing);
    }

    this.watches.set(watch.watchId, watch);
    const set = this.watchesByJid.get(watch.jid) ?? new Set<string>();
    set.add(watch.watchId);
    this.watchesByJid.set(watch.jid, set);

    this.ensureTimers();

    const known = this.snapshots.get(watch.jid);
    if (watch.fireIfAlreadyOnline && known?.online && this.isFresh(known)) {
      this.fire([watch], known);
      return { watch, firedImmediately: true };
    }

    this.subscribeQueue.add(watch.jid);
    void this.drain();
    return { watch, firedImmediately: false };
  }

  public unwatch(watchId: string): boolean {
    const watch = this.watches.get(watchId);
    if (!watch) {
      return false;
    }
    this.watches.delete(watchId);
    this.unindex(watch);
    this.stopTimersIfIdle();
    return true;
  }

  /** Drops every watch aimed at a jid. Returns the ids removed. */
  public unwatchJid(jid: string): string[] {
    const ids = [...(this.watchesByJid.get(jid) ?? [])];
    for (const id of ids) {
      this.watches.delete(id);
    }
    this.watchesByJid.delete(jid);
    this.stopTimersIfIdle();
    return ids;
  }

  private unindex(watch: PresenceWatch) {
    const set = this.watchesByJid.get(watch.jid);
    if (!set) {
      return;
    }
    set.delete(watch.watchId);
    if (set.size === 0) {
      this.watchesByJid.delete(watch.jid);
    }
  }

  // ─── event ingestion ────────────────────────────────────────────────────────

  /**
   * Consumes a Baileys `presence.update` payload. Group payloads carry one entry
   * per participant, so every participant is cached under its own jid.
   */
  public handleUpdate(payload: {
    id: string;
    presences: Record<string, { lastKnownPresence?: WAPresence; lastSeen?: number; groupOnlineCount?: number }>;
  }) {
    if (!payload?.presences) {
      return;
    }

    for (const [participant, data] of Object.entries(payload.presences)) {
      const jid = participant || payload.id;
      const lastKnownPresence = data?.lastKnownPresence ?? 'unavailable';
      const previous = this.snapshots.get(jid);

      const snapshot: PresenceSnapshot = {
        jid,
        // composing/recording/paused all imply the contact has WhatsApp open.
        online: lastKnownPresence !== 'unavailable',
        lastKnownPresence,
        lastSeen: typeof data?.lastSeen === 'number' ? data.lastSeen : (previous?.lastSeen ?? null),
        // Only meaningful for an offline contact: WhatsApp omits `last` while a
        // contact is online, which is not the same as hiding it.
        lastSeenHidden:
          lastKnownPresence === 'unavailable' &&
          typeof data?.lastSeen !== 'number' &&
          previous?.lastSeen == null,
        groupOnlineCount: data?.groupOnlineCount,
        updatedAt: Date.now(),
      };

      this.store(jid, snapshot);
      this.notifyWaiters(jid, snapshot);

      if (!snapshot.online) {
        continue;
      }
      // Only a transition arms the trigger for watches that asked for one;
      // presence flaps (typing → available → typing) must not re-fire.
      const transitioned = !previous?.online;
      const due = [...(this.watchesByJid.get(jid) ?? [])]
        .map((id) => this.watches.get(id))
        .filter((w): w is PresenceWatch => !!w && (transitioned || w.fireIfAlreadyOnline));

      if (due.length > 0) {
        this.fire(due, snapshot);
      }
    }
  }

  private store(jid: string, snapshot: PresenceSnapshot) {
    // delete-then-set keeps Map insertion order as an LRU for the eviction below.
    this.snapshots.delete(jid);
    this.snapshots.set(jid, snapshot);
    while (this.snapshots.size > MAX_CACHED_SNAPSHOTS) {
      const oldest = this.snapshots.keys().next().value;
      if (oldest === undefined) {
        break;
      }
      this.snapshots.delete(oldest);
    }
  }

  private notifyWaiters(jid: string, snapshot: PresenceSnapshot) {
    const list = this.waiters.get(jid);
    if (!list?.length) {
      return;
    }
    this.waiters.delete(jid);
    for (const resolve of list) {
      try {
        resolve(snapshot);
      } catch {
        // a waiter that already timed out is not our problem
      }
    }
  }

  /** Watches are one-shot: removed before the callback so a throw can't re-fire them. */
  private fire(watches: PresenceWatch[], snapshot: PresenceSnapshot) {
    for (const watch of watches) {
      this.watches.delete(watch.watchId);
      this.unindex(watch);
    }
    this.stopTimersIfIdle();

    try {
      this.deps.onOnline(watches, snapshot);
    } catch (error) {
      this.deps.logger.error(
        `presence watch callback failed for ${snapshot.jid}: ${error?.message ?? error}`,
      );
    }
  }

  /**
   * A cached "online" older than the refresh cadence is not evidence the contact
   * is online now — it just means nothing has arrived since.
   */
  private isFresh(snapshot: PresenceSnapshot) {
    return Date.now() - snapshot.updatedAt < REFRESH_INTERVAL_MS;
  }

  // ─── subscribe throttling ───────────────────────────────────────────────────

  private async drain() {
    if (this.draining || !this.connected) {
      return;
    }
    this.draining = true;
    try {
      while (this.connected && this.subscribeQueue.size > 0) {
        const jid = this.subscribeQueue.values().next().value as string;
        this.subscribeQueue.delete(jid);
        try {
          await this.deps.subscribe(jid);
        } catch (error) {
          this.deps.logger.warn(
            `presence subscribe failed for ${jid}: ${error?.message ?? error}`,
          );
        }
        if (this.subscribeQueue.size > 0) {
          await new Promise((resolve) => setTimeout(resolve, SUBSCRIBE_GAP_MS));
        }
      }
    } finally {
      this.draining = false;
    }
  }

  // ─── timers ─────────────────────────────────────────────────────────────────

  private ensureTimers() {
    if (this.watches.size === 0) {
      return;
    }
    if (!this.refreshTimer) {
      this.refreshTimer = setInterval(() => this.refresh(), REFRESH_INTERVAL_MS);
      this.refreshTimer.unref?.();
    }
    if (!this.sweepTimer) {
      this.sweepTimer = setInterval(() => this.sweep(), SWEEP_INTERVAL_MS);
      this.sweepTimer.unref?.();
    }
  }

  private stopTimersIfIdle() {
    if (this.watches.size === 0) {
      this.stopTimers();
    }
  }

  private stopTimers() {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = undefined;
    }
  }

  private refresh() {
    if (!this.connected) {
      return;
    }
    for (const watch of this.watches.values()) {
      this.subscribeQueue.add(watch.jid);
    }
    void this.drain();
  }

  private sweep() {
    const now = Date.now();
    const expired = [...this.watches.values()].filter((w) => w.expiresAt <= now);
    if (expired.length === 0) {
      return;
    }

    for (const watch of expired) {
      this.watches.delete(watch.watchId);
      this.unindex(watch);
    }
    this.stopTimersIfIdle();

    try {
      this.deps.onExpire(expired);
    } catch (error) {
      this.deps.logger.error(`presence expiry callback failed: ${error?.message ?? error}`);
    }
  }
}
