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
  FRAME_AUDIO,
  FRAME_VIDEO,
  readCallTicket,
  type FromWorker,
  type HostRequest,
  type ToWorker,
} from './voip.protocol';

const PORT = Number(process.env.VOIP_WORKER_PORT);
const SECRET = process.env.VOIP_TOKEN_SECRET || '';
const MAX_CALLS = Number(process.env.VOIP_MAX_CALLS_PER_WORKER || 20);
const MAX_CALLS_PER_INSTANCE = Number(process.env.VOIP_MAX_CALLS_PER_INSTANCE || 3);
const LOG_LEVEL = process.env.VOIP_LOG_LEVEL || 'warn';
/** No media from the caller for this long while active: the call is gone (a lost terminate). */
const SILENCE_MS = 20_000;
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

// ── Instances ──────────────────────────────────────────────────────────────────

const hosts = new Map<string, InstanceHost>();

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
  /** After a dropped video frame, wait for a key frame before sending video again. */
  needKeyFrame: boolean;
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
    if (bridge) control(bridge.ws, { t: 'state', state: call.stateData?.state });
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
      if (bridge.ws.bufferedAmount > VIDEO_BACKLOG) {
        bridge.needKeyFrame = true;
        return;
      }
      if (bridge.needKeyFrame && !frame.keyFrame) return;
      bridge.needKeyFrame = false;
      const out = Buffer.allocUnsafe(10 + frame.data.byteLength);
      out[0] = FRAME_VIDEO;
      out[1] = frame.keyFrame ? 1 : 0;
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

async function answer(ws: WebSocket, instance: string, callId: string) {
  const host = hosts.get(instance);
  const call = host?.engine.getCall(callId);
  if (!host || !call || call.isEnded) {
    // The ring never reached this connection, or it is already over.
    control(ws, { t: 'error', code: 'not_ringing' });
    ws.close(4004, 'not_ringing');
    return;
  }
  if (bridges.has(callId)) {
    control(ws, { t: 'error', code: 'taken' });
    ws.close(4009, 'taken');
    return;
  }
  if (bridges.size >= MAX_CALLS) {
    control(ws, { t: 'error', code: 'busy' });
    ws.close(4029, 'busy');
    return;
  }

  const bridge: Bridge = {
    ws,
    host,
    callId,
    lastInbound: Date.now(),
    needKeyFrame: false,
  };
  bridges.set(callId, bridge);
  control(ws, {
    t: 'call',
    video: call.mediaType === 'video',
    peer: call.callerPn || call.peerJid,
  });

  ws.on('message', (data: Buffer, isBinary: boolean) => {
    try {
      if (!isBinary) {
        const msg = JSON.parse(data.toString());
        if (msg.t === 'hangup') void host.engine.endCall(callId).catch(() => undefined);
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

  try {
    if (call.stateData?.state === 'incoming_ringing')
      await host.engine.acceptCall(callId);
    host.engine.setExternalAudioMode(callId, true);
    control(ws, { t: 'state', state: host.engine.getCall(callId)?.stateData?.state });
  } catch (error) {
    log('accept failed', { instance, callId, error: (error as Error).message });
    control(ws, { t: 'error', code: 'accept_failed' });
    bridges.delete(callId);
    ws.close(4000, 'accept_failed');
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
  void answer(ws, ticket.i, ticket.c).catch((error) => {
    log('answer crashed', { error: (error as Error).message });
    ws.close(1011, 'error');
  });
});
wss.on('listening', () => send({ kind: 'ready', port: PORT }));

// Calls whose hang-up never arrived: the caller's media stops, so end them.
setInterval(() => {
  const now = Date.now();
  for (const bridge of bridges.values()) {
    const state = bridge.host.engine.getCall(bridge.callId)?.stateData?.state;
    if (state === 'active' && now - bridge.lastInbound > SILENCE_MS) {
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
      void host.dispatch(message.node).catch((error) =>
        log('call node failed', {
          instance: message.instance,
          tag: message.node?.tag,
          error: (error as Error).message,
        }),
      );
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
