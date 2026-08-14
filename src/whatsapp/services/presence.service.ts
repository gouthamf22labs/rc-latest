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
  /**
   * Where this signal came from. A contact's presence reaches us either directly
   * (a 1:1 `<presence>` node) or as chat-state inside a group they share with us
   * — WhatsApp reports the group's jid but keys the state to the member, so a
   * group is a genuine second channel for the same person. Recorded because a
   * trigger that fired on group typing behaves differently from one that fired
   * on a direct signal, and after the fact there is otherwise no way to tell.
   */
  source: 'direct' | 'group';
  /** The group the signal arrived through, when `source` is 'group'. */
  viaJid?: string;
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
  /**
   * Only fire on a typing signal (`composing` / `recording`), never on a plain
   * `available`.
   *
   * "Send when this member types" has to mean typing. Presence for a person
   * reaches us from anywhere — them opening WhatsApp in a private chat included
   * — and a watch armed by a group member firing on that would post to the
   * group when nobody had said anything, which is not what was asked for.
   */
  requireTyping: boolean;
  /**
   * ms epoch before which this watch must not fire. Undefined means "armed from
   * the moment it is registered", which is every watch that existed before this
   * field.
   *
   * "Send it when they come online on Monday after 9am" is a different trigger
   * from "send it when they come online": the contact appearing on Friday
   * evening must be ignored, not acted on. The watch is still registered ahead
   * of the window so the subscription is warm and WhatsApp is already pushing by
   * the time it opens — only the *firing* is held back.
   */
  notBefore?: number;
};

export type PresenceWatchInput = {
  watchId: string;
  jid: string;
  /** Aliases of `jid` (typically its LID). `jid` itself is added automatically. */
  jids?: string[];
  number: string;
  ttlSeconds: number;
  fireIfAlreadyOnline?: boolean;
  /** See PresenceWatch.requireTyping. */
  requireTyping?: boolean;
  /** See PresenceWatch.notBefore. ms epoch. */
  notBefore?: number;
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
  /**
   * Re-resolves a contact's jids (phone-number jid plus LID). The LID lookup at
   * registration is a bounded USync round-trip that is allowed to fail, and a
   * watch left without its LID is deaf to everything WhatsApp delivers under it
   * — including group chat-state, which is always LID-keyed. Retried on the
   * refresh cycle so a watch repairs itself instead of staying half-blind for
   * its whole life. Optional: without it the watcher behaves as before.
   */
  expandJids?: (number: string) => Promise<string[]>;
  logger: WatcherLogger;
};

/**
 * Ceiling on LID repairs attempted per refresh cycle. Each is a USync
 * round-trip; the rest are picked up on the next cycle, so a large backlog
 * drains steadily instead of saturating the socket.
 */
const MAX_LID_REPAIRS_PER_CYCLE = 25;

/**
 * WhatsApp reports typing as `composing`, and a voice note being recorded as
 * `recording`; Baileys maps `paused` to `available`, so a person who stopped
 * typing is not still typing.
 */
function isTypingPresence(presence?: WAPresence | null): boolean {
  return presence === 'composing' || presence === 'recording';
}

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
  private repairing = false;
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
    // Every alias, not just the primary: presence commonly arrives under the
    // LID, so re-subscribing the phone-number jid alone silently loses it.
    for (const watch of this.watches.values()) {
      for (const jid of watch.jids) {
        this.subscribeQueue.add(jid);
      }
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
      requireTyping: input.requireTyping ?? false,
      ...(typeof input.notBefore === 'number' && Number.isFinite(input.notBefore)
        ? { notBefore: input.notBefore }
        : {}),
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
    if (
      watch.fireIfAlreadyOnline &&
      this.isArmed(watch) &&
      known?.online &&
      this.isFresh(known) &&
      (!watch.requireTyping || isTypingPresence(known.lastKnownPresence))
    ) {
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
      // A 1:1 node addresses the contact itself, so payload.id and the
      // participant are the same jid. They differ only when the state arrived
      // through a chat the contact is in — i.e. a group.
      const isGroup = !!payload.id && payload.id !== jid;

      const snapshot: PresenceSnapshot = {
        jid,
        // composing/recording/paused all imply the contact has WhatsApp open.
        online: lastKnownPresence !== 'unavailable',
        lastKnownPresence,
        source: isGroup ? 'group' : 'direct',
        ...(isGroup ? { viaJid: payload.id } : {}),
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
        .filter((w): w is PresenceWatch => {
          if (!w) return false;
          // Outside its window this watch is deaf on purpose — the contact being
          // online today says nothing about the Monday morning that was asked
          // for. It stays registered, so the subscription is live and warm when
          // the window does open.
          if (!this.isArmed(w)) return false;
          // A typing-only watch ignores a bare "available": the contact opening
          // WhatsApp is not them saying something.
          if (w.requireTyping && !isTypingPresence(lastKnownPresence)) return false;
          return transitioned || w.fireIfAlreadyOnline;
        });

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

  /** A watch is armed once its `notBefore` window has opened (or it has none). */
  private isArmed(watch: PresenceWatch) {
    return watch.notBefore === undefined || Date.now() >= watch.notBefore;
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
    // All aliases — see onConnectionOpen. Re-asserting only the primary jid lets
    // a LID-delivered contact's subscription lapse for good after the first
    // registration, which looks exactly like the contact never coming online.
    for (const watch of this.watches.values()) {
      for (const jid of watch.jids) {
        this.subscribeQueue.add(jid);
      }
    }
    void this.drain();
    void this.repairMissingLids();
  }

  /**
   * A watch whose LID lookup failed at registration carries only the phone-number
   * jid, so nothing WhatsApp routes by LID can ever match it. Retry those here:
   * the lookup is cheap, and one success converts a permanently deaf watch into a
   * working one.
   */
  private async repairMissingLids() {
    const expand = this.deps.expandJids;
    // Each repair is a USync round-trip. One cycle at a time, and a cap per
    // cycle, so a large backlog cannot outrun REFRESH_INTERVAL_MS and stack
    // overlapping sweeps on the socket. The remainder is picked up next cycle.
    if (!expand || this.repairing) {
      return;
    }
    this.repairing = true;
    try {
      await this.repairSweep(expand);
    } finally {
      this.repairing = false;
    }
  }

  private async repairSweep(expand: (number: string) => Promise<string[]>) {
    const incomplete = [...this.watches.values()]
      .filter((w) => !w.jids.some((j) => j.endsWith('@lid')))
      .slice(0, MAX_LID_REPAIRS_PER_CYCLE);
    for (const watch of incomplete) {
      try {
        const jids = await expand(watch.number);
        const added = jids.filter(Boolean).filter((j) => !watch.jids.includes(j));
        if (added.length === 0) {
          continue;
        }
        // The watch may have fired or expired while the lookup was in flight.
        if (this.watches.get(watch.watchId) !== watch) {
          continue;
        }
        watch.jids = [...watch.jids, ...added];
        for (const jid of added) {
          const set = this.watchesByJid.get(jid) ?? new Set<string>();
          set.add(watch.watchId);
          this.watchesByJid.set(jid, set);
          this.subscribeQueue.add(jid);
        }
        this.deps.logger.info(
          `presence watch ${watch.watchId} repaired with ${added.join(', ')}`,
        );
      } catch (error) {
        // Next refresh tries again — a failed repair must never break the sweep.
        this.deps.logger.warn(
          `presence jid repair failed for ${watch.number}: ${error?.message ?? error}`,
        );
      }
    }
    void this.drain();
  }

  /**
   * Releases windowed watches at the moment their window opens.
   *
   * Firing is driven by `presence.update`, and WhatsApp only sends one when
   * something changes. Someone who opened WhatsApp at 08:55 and left it sitting
   * there produces no event at 09:00, so a watch that waited purely for the next
   * update would sleep through its whole window and go out at its backstop
   * instead — the one outcome "send it Monday when they're online" must not have.
   *
   * Two cases, and each watch is handled exactly once:
   *  - a fresh online snapshot is already on hand: fire immediately;
   *  - otherwise the window is simply dropped, which turns this into an ordinary
   *    armed watch, and the contact is re-subscribed so WhatsApp answers with
   *    their current presence within seconds rather than at the next refresh.
   */
  private fireWindowsJustOpened() {
    let resubscribe = false;
    for (const watch of [...this.watches.values()]) {
      if (watch.notBefore === undefined || !this.isArmed(watch)) {
        continue;
      }
      const known = this.getSnapshot(...watch.jids);
      if (
        watch.fireIfAlreadyOnline &&
        known?.online &&
        this.isFresh(known) &&
        (!watch.requireTyping || isTypingPresence(known.lastKnownPresence))
      ) {
        this.fire([watch], known);
        continue;
      }
      // The window has done its job; from here this is a plain watch. Clearing it
      // also keeps this loop off the watch on every subsequent sweep.
      watch.notBefore = undefined;
      for (const jid of watch.jids) {
        this.subscribeQueue.add(jid);
      }
      resubscribe = true;
    }
    if (resubscribe) {
      void this.drain();
    }
  }

  private sweep() {
    this.fireWindowsJustOpened();

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
