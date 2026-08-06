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
  /** Primary jid, used for display and for the subscribe. */
  jid: string;
  /**
   * Every jid this contact's presence can arrive under. WhatsApp addresses a
   * contact by phone-number jid *or* by LID, and which one a `<presence>` node
   * carries is not ours to choose — a watch registered under only one of them
   * silently never fires.
   */
  jids: string[];
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
  /** Aliases of `jid` (typically its LID). `jid` itself is added automatically. */
  jids?: string[];
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
  /**
   * Announces the socket as `available`. WhatsApp only pushes presence to a
   * socket it considers online: without this, subscribes are accepted and
   * nothing is ever pushed back. Baileys' own markOnlineOnConnect announce is
   * not sufficient — it is skipped when the creds carry no pushName, and the
   * server stops honouring it well before a long-lived watch expires.
   */
  announceAvailable: () => Promise<unknown>;
  /** A watch matched: the contact is online. One-shot — the watch is already removed. */
  onOnline: (watches: PresenceWatch[], snapshot: PresenceSnapshot) => void;
  /** TTL elapsed without the contact ever coming online. */
  onExpire: (watches: PresenceWatch[]) => void;
  logger: WatcherLogger;
};

/** Gap between subscribe nodes so a reconnect doesn't burst the socket. */
const SUBSCRIBE_GAP_MS = 150;
/**
 * How long an `available` announce is assumed to hold. Re-announced before a
 * batch of subscribes once this has elapsed, which also covers the refresh tick.
 */
const AVAILABLE_TTL_MS = 60 * 1000;
/** WhatsApp lets subscriptions lapse; re-assert them on this cadence. */
const REFRESH_INTERVAL_MS = 5 * 60 * 1000;
/** How often expired watches are swept. */
const SWEEP_INTERVAL_MS = 30 * 1000;
/** Cap on cached snapshots per instance — presence data is disposable. */
const MAX_CACHED_SNAPSHOTS = 2000;
/**
 * How long a snapshot is treated as describing the contact *now*.
 *
 * Presence changes in seconds, and WhatsApp only pushes while we hold a live
 * subscription — once the last watch on a contact fires, updates stop and the
 * last thing we heard ("online") sits in the cache indefinitely. Acting on that
 * sent messages to contacts who had closed WhatsApp minutes earlier, so anything
 * older than this is reported as stale rather than as current state.
 */
export const PRESENCE_FRESH_MS = 45 * 1000;
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
  private lastAnnouncedAt = 0;
  private refreshTimer?: NodeJS.Timeout;
  private sweepTimer?: NodeJS.Timeout;

  // ─── lifecycle ──────────────────────────────────────────────────────────────

  public onConnectionOpen() {
    this.connected = true;
    // The previous socket's announce died with it.
    this.lastAnnouncedAt = 0;
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

  /** Freshest snapshot across a contact's jids (phone-number jid and LID). */
  public getSnapshot(...jids: string[]): PresenceSnapshot | undefined {
    let best: PresenceSnapshot | undefined;
    for (const jid of jids) {
      const found = this.snapshots.get(jid);
      if (found && (!best || found.updatedAt > best.updatedAt)) {
        best = found;
      }
    }
    return best;
  }

  /**
   * Subscribes and resolves on the first `presence.update` for any of the
   * contact's jids, or null on timeout. A timeout is a normal outcome: WhatsApp
   * stays silent when the contact hides both last-seen and online status.
   */
  public async requestSnapshot(jids: string[], timeoutMs: number): Promise<PresenceSnapshot | null> {
    const waiter = this.waitForUpdate(jids, timeoutMs);
    // This path subscribes directly rather than through the queue, so it has to
    // do its own announce — otherwise the lookup waits out its timeout against a
    // socket WhatsApp will not push to.
    await this.ensureAvailable();
    for (const jid of jids) {
      try {
        await this.deps.subscribe(jid);
      } catch (error) {
        this.deps.logger.warn(`presence subscribe failed for ${jid}: ${error?.message ?? error}`);
      }
    }
    return waiter;
  }

  private waitForUpdate(jids: string[], timeoutMs: number): Promise<PresenceSnapshot | null> {
    return new Promise((resolve) => {
      let settled = false;

      const timer = setTimeout(() => {
        settled = true;
        for (const jid of jids) {
          this.removeWaiter(jid, onUpdate);
        }
        resolve(null);
      }, timeoutMs);

      const onUpdate = (snapshot: PresenceSnapshot) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        // The other aliases' waiters are left to be cleared by their own
        // resolution or by notifyWaiters; `settled` makes them harmless.
        resolve(snapshot);
      };

      for (const jid of jids) {
        const list = this.waiters.get(jid) ?? [];
        list.push(onUpdate);
        this.waiters.set(jid, list);
      }
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
      jids: [...new Set([input.jid, ...(input.jids ?? [])])].filter(Boolean),
      number: input.number,
      createdAt: existing?.createdAt ?? Date.now(),
      expiresAt: Date.now() + input.ttlSeconds * 1000,
      fireIfAlreadyOnline: input.fireIfAlreadyOnline ?? true,
    };

    // A re-registration may move the jids; clear the old index entries first.
    if (existing) {
      this.unindex(existing);
    }

    this.watches.set(watch.watchId, watch);
    for (const jid of watch.jids) {
      const set = this.watchesByJid.get(jid) ?? new Set<string>();
      set.add(watch.watchId);
      this.watchesByJid.set(jid, set);
    }

    this.ensureTimers();

    const known = this.getSnapshot(...watch.jids);
    if (watch.fireIfAlreadyOnline && known?.online && this.isFresh(known)) {
      this.fire([watch], known);
      return { watch, firedImmediately: true };
    }

    for (const jid of watch.jids) {
      this.subscribeQueue.add(jid);
    }
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
    this.forgetIfUnwatched(watch.jids);
    this.stopTimersIfIdle();
    return true;
  }

  /** Drops every watch aimed at any of these jids. Returns the ids removed. */
  public unwatchJid(...jids: string[]): string[] {
    const ids = new Set<string>();
    for (const jid of jids) {
      for (const id of this.watchesByJid.get(jid) ?? []) {
        ids.add(id);
      }
    }
    for (const id of ids) {
      const watch = this.watches.get(id);
      this.watches.delete(id);
      // Unindex by the watch's own aliases, not just the jids passed in.
      if (watch) {
        this.unindex(watch);
        this.forgetIfUnwatched(watch.jids);
      }
    }
    this.stopTimersIfIdle();
    return [...ids];
  }

  private unindex(watch: PresenceWatch) {
    for (const jid of watch.jids) {
      const set = this.watchesByJid.get(jid);
      if (!set) {
        continue;
      }
      set.delete(watch.watchId);
      if (set.size === 0) {
        this.watchesByJid.delete(jid);
      }
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
      // presence flaps (typing → available → typing) must not re-fire. "We had
      // no state" is not a transition either — since the cache is dropped once a
      // contact is unwatched, treating unknown→online as one would fire watches
      // that explicitly asked to wait for the contact to go offline first.
      const transitioned = previous ? !previous.online : false;
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

  /**
   * Drops cached presence for contacts nobody is watching any more.
   *
   * WhatsApp only pushes while a subscription is live, and there is no
   * unsubscribe — once the last watch on a contact goes, updates simply stop and
   * whatever was last heard would sit in the cache forever. Keeping "online"
   * around is worse than keeping nothing: the next watch would act on it. A
   * contact still watched by another message is left alone, since the refresh
   * loop keeps that entry honest.
   */
  private forgetIfUnwatched(jids: string[]) {
    for (const jid of jids) {
      if (!this.watchesByJid.has(jid)) {
        this.snapshots.delete(jid);
      }
    }
  }

  /** Watches are one-shot: removed before the callback so a throw can't re-fire them. */
  private fire(watches: PresenceWatch[], snapshot: PresenceSnapshot) {
    for (const watch of watches) {
      this.watches.delete(watch.watchId);
      this.unindex(watch);
    }
    for (const watch of watches) {
      this.forgetIfUnwatched(watch.jids);
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
   * A cached "online" past PRESENCE_FRESH_MS is not evidence the contact is
   * online now — it only means nothing has arrived since, which is the normal
   * state once the subscription lapses.
   */
  public isFresh(snapshot: PresenceSnapshot) {
    return Date.now() - snapshot.updatedAt < PRESENCE_FRESH_MS;
  }

  // ─── subscribe throttling ───────────────────────────────────────────────────

  /**
   * WhatsApp pushes presence only to a socket it considers online, so every
   * batch of subscribes is preceded by an announce. Cheap and idempotent —
   * rate-limited to one per AVAILABLE_TTL_MS.
   */
  private async ensureAvailable() {
    if (!this.connected || Date.now() - this.lastAnnouncedAt < AVAILABLE_TTL_MS) {
      return;
    }
    try {
      await this.deps.announceAvailable();
      this.lastAnnouncedAt = Date.now();
    } catch (error) {
      this.deps.logger.warn(`presence available announce failed: ${error?.message ?? error}`);
    }
  }

  private async drain() {
    if (this.draining || !this.connected) {
      return;
    }
    this.draining = true;
    try {
      if (this.subscribeQueue.size > 0) {
        await this.ensureAvailable();
      }
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
    // Same reasoning as fire(): an expired watch also leaves the entry unrefreshed.
    for (const watch of expired) {
      this.forgetIfUnwatched(watch.jids);
    }
    this.stopTimersIfIdle();

    try {
      this.deps.onExpire(expired);
    } catch (error) {
      this.deps.logger.error(`presence expiry callback failed: ${error?.message ?? error}`);
    }
  }
}
