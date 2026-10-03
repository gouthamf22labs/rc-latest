/**
 * A call worker: a child process of CodeChat that runs the call engines for the instances it is
 * given and carries each answered call's media to and from one browser.
 *
 * Kept out of the main process on purpose. The media path is native WebRTC, SRTP and a WASM
 * codec; if any of it crashes, this process dies, its calls end, and the supervisor starts a
 * fresh one. Messaging on the main process never notices.
 *
 * Concurrency: many instances per worker, several calls per instance; each call has its own
 * relay connection, keys and codec state inside the engine, and its own browser socket here.
 * A per-worker ceiling (VOIP_MAX_CALLS_PER_WORKER) refuses answers beyond what one process
 * should carry rather than degrading every call on it.
 */
import { WebSocketServer, type WebSocket } from 'ws';
import { InstanceHost } from './voip.host';
import {
  type VoipNode,
  FRAME_AUDIO,
  FRAME_VIDEO,
  readCallTicket,
  type FromWorker,
  type HostRequest,
  type ToWorker,
} from './voip.protocol';

/** 0: any free port; it is reported to the main process in 'ready'. */
const PORT = Number(process.env.VOIP_WORKER_PORT || 0);
const SECRET = process.env.VOIP_TOKEN_SECRET || '';
const MAX_CALLS = Number(process.env.VOIP_MAX_CALLS_PER_WORKER || 20);
const MAX_CALLS_PER_INSTANCE = Number(process.env.VOIP_MAX_CALLS_PER_INSTANCE || 6);
const LOG_LEVEL = process.env.VOIP_LOG_LEVEL || 'warn';
/**
 * No media from the caller for this long while active: the call is gone (a lost terminate).
 * Generous, because WhatsApp sends almost nothing while a caller is silent or muted.
 */
const SILENCE_MS = 45_000;
/** A call we place rings this long before we give up (WhatsApp's own ring is about a minute). */
const RING_MS = 60_000;
/** A browser that stops reading: drop video (then audio) rather than buffer without bound. */
const VIDEO_BACKLOG = 1_500_000;
const AUDIO_BACKLOG = 4_000_000;
const RPC_TIMEOUT_MS = 15_000;

const send = (message: FromWorker) => process.send?.(message);
const log = (msg: string, extra: Record<string, unknown> = {}) =>
  process.stderr.write(
    JSON.stringify({ level: 'warn', context: 'voip-worker', msg, ...extra }) + '\n',
  );

// ── RPC to the main process ────────────────────────────────────────────────────

let nextId = 1;
const pending = new Map<
  number,
  { resolve: (v: any) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }
>();

function rpc(instance: string, request: HostRequest): Promise<any> {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${request.op} timed out`));
    }, RPC_TIMEOUT_MS);
    pending.set(id, { resolve, reject, timer });
    send({ kind: 'request', id, instance, request });
  });
}

/**
 * The customer declined a call we placed. The engine ignores a <reject> (it only expects one on
 * calls it receives), so the call would ring on here until the no-answer timeout: end it now.
 */
async function endDeclinedOutgoing(host: InstanceHost, node: VoipNode) {
  if (node.tag !== 'call' || !Array.isArray(node.content)) return;
  const reject = node.content.find((child) => child?.tag === 'reject');
  const callId = reject?.attrs?.['call-id'];
  if (!callId) return;
  const call = host.engine.getCall(callId);
  if (!call || call.isEnded || call.direction !== 'outgoing') return;
  if (call.stateData?.state === 'active') return;
  // Tell the browser why first: the engine reports its own generic reason for the end.
  closeBridge(callId, 'rejected');
  await host.engine.endCall(callId, 'rejected').catch(() => undefined);
}

/** An H.264 Annex-B access unit a decoder can start from: it carries an IDR slice or an SPS. */
function isKeyFrame(data: Uint8Array): boolean {
  for (let i = 0; i + 3 < data.length; i++) {
    if (data[i] !== 0 || data[i + 1] !== 0) continue;
    const start =
      data[i + 2] === 1 ? i + 3 : data[i + 2] === 0 && data[i + 3] === 1 ? i + 4 : -1;
    if (start < 0 || start >= data.length) continue;
    const type = data[start] & 0x1f;
    if (type === 5 || type === 7) return true;
    i = start;
  }
  return false;
}

// ── Instances ──────────────────────────────────────────────────────────────────

const hosts = new Map<string, InstanceHost>();
/** Per instance, the tail of its stanza chain (see the 'node' handler). */
const queues = new Map<string, Promise<unknown>>();

function hostFor(instance: string): InstanceHost {
  let host = hosts.get(instance);
  if (host) return host;
  host = new InstanceHost(instance, (r) => rpc(instance, r), {
    maxConcurrentCalls: MAX_CALLS_PER_INSTANCE,
    logLevel: LOG_LEVEL,
  });
  wireHost(host);
  hosts.set(instance, host);
  return host;
}

// ── Browser sessions (one per answered call) ───────────────────────────────────

type Bridge = {
  ws: WebSocket;
  host: InstanceHost;
  callId: string;
  lastInbound: number;
  /** The caller muted: they send nothing, which must not look like a dead call. */
  peerMuted: boolean;
  /** After a dropped video frame, wait for a key frame before sending video again. */
  needKeyFrame: boolean;
  /** Placed from the CRM (rings on the customer's phone) rather than answered. */
  outgoing: boolean;
  startedAt: number;
};
const bridges = new Map<string, Bridge>();

const control = (ws: WebSocket, message: Record<string, unknown>) => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(message));
};

function closeBridge(callId: string, reason: string) {
  const bridge = bridges.get(callId);
  if (!bridge) return;
  bridges.delete(callId);
  control(bridge.ws, { t: 'ended', reason });
  try {
    bridge.ws.close(1000, reason.slice(0, 100));
  } catch {
    // already closed
  }
}

function wireHost(host: InstanceHost) {
  host.on('voip_call_state', (call: any) => {
    const bridge = bridges.get(call.callId);
    if (!bridge) return;
    bridge.lastInbound = Date.now();
    control(bridge.ws, { t: 'state', state: call.stateData?.state });
  });
  host.on('voip_call_peer_mute', ({ call, muted }: { call: any; muted: boolean }) => {
    const bridge = bridges.get(call.callId);
    if (!bridge) return;
    bridge.peerMuted = Boolean(muted);
    bridge.lastInbound = Date.now();
    control(bridge.ws, { t: 'peerMute', muted: Boolean(muted) });
  });
  host.on('voip_call_ended', (call: any) => {
    closeBridge(call.callId, call.stateData?.reason || 'ended');
  });
  host.on(
    'voip_call_inbound_audio',
    ({ call, pcm }: { call: any; pcm: Float32Array }) => {
      const bridge = bridges.get(call.callId);
      if (!bridge) return;
      bridge.lastInbound = Date.now();
      if (
        bridge.ws.readyState !== bridge.ws.OPEN ||
        bridge.ws.bufferedAmount > AUDIO_BACKLOG
      )
        return;
      const frame = Buffer.allocUnsafe(1 + pcm.byteLength);
      frame[0] = FRAME_AUDIO;
      Buffer.from(pcm.buffer, pcm.byteOffset, pcm.byteLength).copy(frame, 1);
      bridge.ws.send(frame);
    },
  );
  host.on(
    'voip_call_inbound_video',
    ({
      call,
      frame,
    }: {
      call: any;
      frame: { keyFrame: boolean; timestamp: number; data: Uint8Array };
    }) => {
      const bridge = bridges.get(call.callId);
      if (!bridge) return;
      bridge.lastInbound = Date.now();
      if (bridge.ws.readyState !== bridge.ws.OPEN) return;
      // The engine's own flag misses most key frames (it marked 1 of 9 in a test call, so a
      // viewer waited ~40 s for video): read the access unit itself.
      const keyFrame = isKeyFrame(frame.data);
      if (bridge.ws.bufferedAmount > VIDEO_BACKLOG) {
        bridge.needKeyFrame = true;
        return;
      }
      if (bridge.needKeyFrame && !keyFrame) return;
      bridge.needKeyFrame = false;
      const out = Buffer.allocUnsafe(10 + frame.data.byteLength);
      out[0] = FRAME_VIDEO;
      out[1] = keyFrame ? 1 : 0;
      out.writeDoubleLE(frame.timestamp, 2);
      Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength).copy(
        out,
        10,
      );
      bridge.ws.send(out);
    },
  );
  host.on('voip_call_error', (error: Error) => {
    log('call engine error', { instance: host.instance, error: error?.message });
  });
}

/** Joins a browser to a call: media both ways, hang-up and mute, and the call ends with it. */
function attachBridge(
  ws: WebSocket,
  host: InstanceHost,
  callId: string,
  outgoing: boolean,
): Bridge {
  const bridge: Bridge = {
    ws,
    host,
    callId,
    outgoing,
    startedAt: Date.now(),
    lastInbound: Date.now(),
    peerMuted: false,
    needKeyFrame: false,
  };
  bridges.set(callId, bridge);

  ws.on('message', (data: Buffer, isBinary: boolean) => {
    try {
      if (!isBinary) {
        const msg = JSON.parse(data.toString());
        if (msg.t === 'hangup') {
          log('hangup from browser', { callId, state: host.engine.getCall(callId)?.stateData?.state });
          void host.engine
            .endCall(callId)
            .then(() => log('call ended from browser', { callId }))
            .catch((error: Error) => log('end call failed', { callId, error: error?.message }));
        }
        else if (msg.t === 'mute') host.engine.setMute(callId, Boolean(msg.muted));
        return;
      }
      if (data[0] === FRAME_AUDIO && data.length > 1) {
        // Copy into an aligned buffer: Float32Array needs a 4-byte-aligned offset.
        const bytes = new Uint8Array(data.length - 1);
        bytes.set(data.subarray(1));
        host.engine.feedLiveAudio(callId, new Float32Array(bytes.buffer));
      } else if (data[0] === FRAME_VIDEO && data.length > 9) {
        host.engine.feedLiveVideo(
          callId,
          new Uint8Array(data.subarray(9)),
          data.readDoubleLE(1),
        );
      }
    } catch (error) {
      log('bad frame from browser', { callId, error: (error as Error).message });
    }
  });
  ws.on('close', () => {
    if (bridges.get(callId)?.ws === ws) {
      bridges.delete(callId);
      void host.engine.endCall(callId).catch(() => undefined);
    }
  });
  return bridge;
}

const refuse = (ws: WebSocket, code: string, closeCode: number) => {
  control(ws, { t: 'error', code });
  ws.close(closeCode, code);
};

async function answer(ws: WebSocket, instance: string, callId: string) {
  const host = hosts.get(instance);
  const call = host?.engine.getCall(callId);
  // The ring never reached this connection, or it is already over.
  if (!host || !call || call.isEnded) return refuse(ws, 'not_ringing', 4004);
  if (bridges.has(callId)) return refuse(ws, 'taken', 4009);
  if (bridges.size >= MAX_CALLS) return refuse(ws, 'busy', 4029);

  attachBridge(ws, host, callId, false);
  control(ws, {
    t: 'call',
    callId,
    video: call.mediaType === 'video',
    peer: call.callerPn || call.peerJid,
  });

  try {
    // Before accepting: the browser starts sending as soon as the call is live, and the engine
    // ignores live audio until this is on.
    host.engine.setExternalAudioMode(callId, true);
    if (call.stateData?.state === 'incoming_ringing')
      await host.engine.acceptCall(callId);
    control(ws, { t: 'state', state: host.engine.getCall(callId)?.stateData?.state });
  } catch (error) {
    log('accept failed', { instance, callId, error: (error as Error).message });
    // Half-accepted: end it here so it doesn't hang in 'connecting' (the phone can still answer
    // a call that hasn't reached accept).
    void host.engine.endCall(callId).catch(() => undefined);
    bridges.delete(callId);
    refuse(ws, 'failed', 4000);
  }
}

/** Calls the customer from the CRM: the call rings on their phone until they pick up. */
async function place(ws: WebSocket, instance: string, peerJid: string, video: boolean) {
  if (bridges.size >= MAX_CALLS) return refuse(ws, 'busy', 4029);
  const host = hostFor(instance);
  try {
    // A number that hasn't had a call yet has no credentials here: ask for them.
    host.setCredentials(await rpc(instance, { op: 'credentials' }));
    const callId = await host.engine.startCall({ peerJid, isVideo: video });
    if (ws.readyState !== ws.OPEN) {
      void host.engine.endCall(callId).catch(() => undefined);
      return;
    }
    attachBridge(ws, host, callId, true);
    host.engine.setExternalAudioMode(callId, true);
    control(ws, { t: 'call', callId, video, peer: peerJid, outgoing: true });
    control(ws, { t: 'state', state: host.engine.getCall(callId)?.stateData?.state });
  } catch (error) {
    log('placing call failed', { instance, error: (error as Error).message });
    refuse(ws, 'failed', 4000);
  }
}

// ── Browser WebSocket ──────────────────────────────────────────────────────────

const wss = new WebSocketServer({
  host: '127.0.0.1',
  port: PORT,
  maxPayload: 2 * 1024 * 1024,
});
wss.on('connection', (ws, req) => {
  const token = new URL(req.url || '/', 'http://local').searchParams.get('token') || '';
  const ticket = readCallTicket(token, SECRET);
  if (!ticket) {
    ws.close(4001, 'unauthorized');
    return;
  }
  const start = ticket.p
    ? place(ws, ticket.i, ticket.p, Boolean(ticket.v))
    : answer(ws, ticket.i, ticket.c);
  void start.catch((error) => {
    log('call start crashed', { error: (error as Error).message });
    ws.close(1011, 'error');
  });
});
wss.on('listening', () => {
  const address = wss.address();
  send({
    kind: 'ready',
    port: typeof address === 'object' && address ? address.port : PORT,
  });
});
// Can't listen (port taken, …): exit so the supervisor starts a fresh worker.
wss.on('error', (error) => {
  log('call socket server failed', { error: error.message });
  process.exit(1);
});

// Calls whose hang-up never arrived: the caller's media stops, so end them. And calls we placed
// that nobody picked up.
setInterval(() => {
  const now = Date.now();
  for (const bridge of bridges.values()) {
    const state = bridge.host.engine.getCall(bridge.callId)?.stateData?.state;
    if (bridge.outgoing && state !== 'active' && now - bridge.startedAt > RING_MS) {
      void bridge.host.engine.endCall(bridge.callId).catch(() => undefined);
      closeBridge(bridge.callId, 'no_answer');
      continue;
    }
    if (
      state === 'active' &&
      !bridge.peerMuted &&
      now - bridge.lastInbound > SILENCE_MS
    ) {
      log('ending silent call', { callId: bridge.callId });
      void bridge.host.engine.endCall(bridge.callId).catch(() => undefined);
      closeBridge(bridge.callId, 'connection_lost');
    }
  }
}, 3_000).unref();

setInterval(
  () => send({ kind: 'stats', calls: bridges.size, instances: hosts.size }),
  30_000,
).unref();

// ── From the main process ──────────────────────────────────────────────────────

process.on('message', (message: ToWorker) => {
  try {
    if (message.kind === 'reply') {
      const waiting = pending.get(message.id);
      if (!waiting) return;
      pending.delete(message.id);
      clearTimeout(waiting.timer);
      if ('error' in message) waiting.reject(new Error(message.error));
      else waiting.resolve(message.value);
    } else if (message.kind === 'node') {
      const host = hostFor(message.instance);
      host.setCredentials(message.credentials);
      // One instance's stanzas in arrival order: a terminate handled while its offer is still
      // being set up would be lost and leave the call ringing here.
      const node = message.node;
      const previous = queues.get(message.instance) ?? Promise.resolve();
      const next = previous
        .then(() => host.dispatch(node))
        .then(() => endDeclinedOutgoing(host, node))
        .catch((error) =>
          log('call node failed', {
            instance: message.instance,
            tag: node?.tag,
            error: (error as Error).message,
          }),
        );
      queues.set(message.instance, next);
      void next.then(() => {
        if (queues.get(message.instance) === next) queues.delete(message.instance);
      });
    } else if (message.kind === 'instanceGone') {
      const host = hosts.get(message.instance);
      if (!host) return;
      for (const [callId, bridge] of bridges)
        if (bridge.host === host) closeBridge(callId, 'disconnected');
      host.dispose();
      hosts.delete(message.instance);
    }
  } catch (error) {
    log('message handling failed', {
      kind: (message as any)?.kind,
      error: (error as Error).message,
    });
  }
});

// The main process going away takes this worker with it.
process.on('disconnect', () => process.exit(0));
// One bad call must not end the others: log and carry on. A native crash still exits, and the
// supervisor restarts the worker.
process.on('uncaughtException', (error) =>
  log('uncaught exception', { error: error.message, stack: error.stack }),
);
process.on('unhandledRejection', (reason) =>
  log('unhandled rejection', { error: String(reason) }),
);
