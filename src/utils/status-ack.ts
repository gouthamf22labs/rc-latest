import { createHash } from 'crypto';

/**
 * Acks top-level <status> stanzas (contacts' Status updates, which WhatsApp now sends as their own
 * stanza, rolled out account by account). Baileys 7.0.0-rc13 has no handler for them, so they are
 * never acked: the server holds the account's incoming events and ends the stream ~50 minutes
 * later ("Stream Errored (ack)", ack class="status"). Upstream: WhiskeySockets/Baileys#2858.
 *
 * The unacked one arrives in the reconnect backlog (the drop comes 50 min after each connect), so
 * the offline ones are what matter:
 *  - a live <status> (no `offline` attribute) is acked at once, as Baileys does for live stanzas;
 *  - an offline one is held briefly so it doesn't jump ahead of the backlog messages Baileys is
 *    still decrypting (acking them at once coincided with a decrypt-failure spike, Nov 19 00:50),
 *    then acked in arrival order. Held until WhatsApp says the backlog is delivered
 *    (`receivedPendingNotifications`) or `maxHoldMs` has passed, whichever is first, then until
 *    incoming messages go quiet (at most `quietMaxMs` more). Never held indefinitely: a held one
 *    that is never acked is exactly the 50-minute drop (Nov 19 02:16, when release waited only
 *    for that signal).
 * Content is never read (we don't use Status updates). The first few arrivals per socket are
 * logged with their attributes, so a miss can be told apart from a hold.
 */

type Node = { tag?: string; attrs?: Record<string, string | undefined> };
type Log = { info: (msg: string) => void; warn: (msg: string) => void };
type SocketLike = {
  ws: { on: (event: string, listener: (node: Node) => void) => unknown };
  ev: { on: (event: string, listener: (arg: any) => void) => unknown };
  sendMessageAck: (node: any) => Promise<void>;
};

/**
 * Which instances ack <status>. Off unless switched on:
 *  - ACK_STATUS_INSTANCES (comma list): on for these.
 *  - ACK_STATUS_PERCENT (0-100, default 0): on for that share of all instances, picked by a stable
 *    hash of the instance name (a partial rollout keeps the same accounts across restarts).
 *    100 = everyone.
 */
export function shouldAckStatus(instanceName: string, env: NodeJS.ProcessEnv = process.env): boolean {
  const listed = String(env.ACK_STATUS_INSTANCES ?? '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
  if (listed.includes(instanceName)) return true;
  const parsed = Number(String(env.ACK_STATUS_PERCENT ?? '').trim() || 0);
  const percent = Number.isFinite(parsed) ? Math.min(100, Math.max(0, parsed)) : 0;
  if (percent >= 100) return true;
  if (percent <= 0) return false;
  const bucket = createHash('sha1').update(instanceName).digest().readUInt16BE(0) % 100;
  return bucket < percent;
}

export type StatusAckOptions = {
  maxHoldMs?: number;
  quietMs?: number;
  quietMaxMs?: number;
  pollMs?: number;
  logArrivals?: number;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function ackStatusStanzas(socket: SocketLike, log: Log, opts: StatusAckOptions = {}) {
  const maxHoldMs = opts.maxHoldMs ?? 15_000;
  const quietMs = opts.quietMs ?? 3_000;
  const quietMaxMs = opts.quietMaxMs ?? 15_000;
  const pollMs = opts.pollMs ?? 250;
  const logArrivals = opts.logArrivals ?? 10;
  const stats = { arrived: 0, live: 0, offline: 0, held: 0, failed: 0 };

  let offlineDone = false;
  let flushing = false;
  let lastIncoming = 0;
  const held: Node[] = [];

  // Cheap logging with every account on: the first few arrivals, the first ack, the first failures.
  const ack = async (node: Node, how: 'live' | 'offline') => {
    try {
      await socket.sendMessageAck(node);
      stats[how]++;
      if (stats.live + stats.offline === 1) {
        log.info(`acked <status> (${how}, ${node?.attrs?.type ?? '-'}, ${node?.attrs?.id ?? '-'}); later acks not logged`);
      }
    } catch (error) {
      stats.failed++;
      if (stats.failed <= 3) {
        log.warn(`<status> ack failed (${node?.attrs?.id ?? '-'}): ${(error as Error)?.message}`);
      }
    }
  };

  const flush = async () => {
    if (flushing || !held.length) return;
    flushing = true;
    try {
      const heldAt = Date.now();
      while (!offlineDone && Date.now() - heldAt < maxHoldMs) await sleep(pollMs);
      const quietFrom = Date.now();
      while (Date.now() - lastIncoming < quietMs && Date.now() - quietFrom < quietMaxMs) await sleep(pollMs);
      const n = held.length;
      // One at a time, in arrival order; anything that arrives meanwhile joins the end.
      while (held.length) await ack(held.shift()!, 'offline');
      log.info(
        `released ${n} held <status> (${offlineDone ? 'backlog delivered' : `after ${maxHoldMs / 1000}s`}, ${Math.round((Date.now() - heldAt) / 1000)}s)`,
      );
    } finally {
      flushing = false;
      if (held.length) void flush();
    }
  };

  socket.ws.on('CB:status', (node: Node) => {
    stats.arrived++;
    const a = node?.attrs ?? {};
    if (stats.arrived <= logArrivals) {
      // `from` reduced to its server part: a contact's number doesn't belong in the logs.
      const server = String(a.from ?? '').split('@')[1] ?? '-';
      log.info(
        `<status> in #${stats.arrived}: offline=${a.offline ?? '-'} type=${a.type ?? '-'} from=@${server} id=${a.id ?? '-'} backlogDone=${offlineDone}`,
      );
    }
    if (a.offline && !offlineDone) {
      held.push(node);
      stats.held++;
      void flush();
      return;
    }
    void ack(node, 'live');
  });

  // Quiet = no incoming messages for a moment (Baileys has caught up with the backlog).
  socket.ev.on('messages.upsert', () => {
    lastIncoming = Date.now();
  });

  socket.ev.on('connection.update', (update: { receivedPendingNotifications?: boolean }) => {
    if (update?.receivedPendingNotifications === true) offlineDone = true;
  });

  return { stats, heldCount: () => held.length };
}
