/**
 * Acks top-level <status> stanzas (contacts' Status updates, which WhatsApp now sends as their own
 * stanza, rolled out account by account). Baileys 7.0.0-rc13 has no handler for them, so they are
 * never acked: the server holds the account's incoming events and ends the stream ~50 minutes
 * later ("Stream Errored (ack)", ack class="status"). Upstream: WhiskeySockets/Baileys#2858.
 *
 * Order matters. Acking redelivered (offline) <status> stanzas immediately, ahead of the offline
 * messages Baileys was still processing, coincided with a spike of "failed to decrypt message"
 * (Nov 19, 00:50 IST; gone within minutes of rolling back). So:
 *  - a live <status> (no `offline` attribute) is acked at once, as Baileys does for live stanzas;
 *  - offline ones are held until WhatsApp says the offline backlog is delivered
 *    (`receivedPendingNotifications`) AND incoming messages have gone quiet (Baileys' offline queue
 *    drained), then acked in the order they arrived.
 * Content is never read (we don't use Status updates).
 */

type Node = { tag?: string; attrs?: Record<string, string | undefined> };
type Log = { info: (msg: string) => void; warn: (msg: string) => void };
type SocketLike = {
  ws: { on: (event: string, listener: (node: Node) => void) => unknown };
  ev: { on: (event: string, listener: (arg: any) => void) => unknown };
  sendMessageAck: (node: any) => Promise<void>;
};

export type StatusAckOptions = { quietMs?: number; maxWaitMs?: number; pollMs?: number };

export function ackStatusStanzas(socket: SocketLike, log: Log, opts: StatusAckOptions = {}) {
  const quietMs = opts.quietMs ?? 3_000;
  const maxWaitMs = opts.maxWaitMs ?? 60_000;
  const pollMs = opts.pollMs ?? 500;
  const stats = { live: 0, offline: 0, held: 0, failed: 0 };

  let offlineDone = false;
  let flushing = false;
  let lastIncoming = Date.now();
  const held: Node[] = [];

  const ack = async (node: Node, how: 'live' | 'offline') => {
    try {
      await socket.sendMessageAck(node);
      stats[how]++;
      log.info(`acked <status> (${how}, ${node?.attrs?.type ?? '-'}, ${node?.attrs?.id ?? '-'})`);
    } catch (error) {
      stats.failed++;
      log.warn(`<status> ack failed (${node?.attrs?.id ?? '-'}): ${(error as Error)?.message}`);
    }
  };

  const flushWhenQuiet = async () => {
    if (flushing) return;
    flushing = true;
    const startedAt = Date.now();
    while (Date.now() - lastIncoming < quietMs && Date.now() - startedAt < maxWaitMs) {
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    // One at a time, in arrival order; anything that arrives meanwhile joins the end.
    while (held.length) await ack(held.shift()!, 'offline');
    flushing = false;
  };

  socket.ws.on('CB:status', (node: Node) => {
    if (node?.attrs?.offline && !offlineDone) {
      held.push(node);
      stats.held++;
      return;
    }
    void ack(node, 'live');
  });

  // Offline processing shows up as incoming messages; "quiet" means Baileys' queue has drained.
  socket.ev.on('messages.upsert', () => {
    lastIncoming = Date.now();
  });

  socket.ev.on('connection.update', (update: { receivedPendingNotifications?: boolean }) => {
    if (update?.receivedPendingNotifications === true && !offlineDone) {
      offlineDone = true;
      void flushWhenQuiet();
    }
  });

  return { stats, heldCount: () => held.length };
}
