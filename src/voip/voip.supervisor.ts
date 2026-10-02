/**
 * Calls in the browser, main-process side.
 *
 * Starts the call workers and keeps them running, hands each instance's call stanzas to the
 * worker that owns it, answers the workers' requests with that instance's Baileys socket, and
 * passes the browser's media WebSocket (/voip/ws) straight through to the right worker.
 *
 * Only instances with the callUpsert webhook event on take part: the CRM turns it on for its
 * numbers, so every other number on CodeChat behaves exactly as before. Without
 * VOIP_TOKEN_SECRET nothing here starts at all.
 */
import { fork, type ChildProcess } from 'child_process';
import { createHash } from 'crypto';
import type { IncomingMessage, Server } from 'http';
import { connect, type Socket } from 'net';
import { join } from 'path';
import {
  peekTicketInstance,
  type FromWorker,
  type HostRequest,
  type ToWorker,
  type VoipCredentials,
  type VoipNode,
} from './voip.protocol';

/** What we need from a WAStartupService: its name, its socket, and whether the CRM wants calls. */
export type VoipInstance = {
  readonly instanceName: string;
  readonly client?: any;
  // `events` is stored as JSON; we only read `callUpsert` off it.
  readonly webhook?: { enabled?: boolean; events?: any };
};

const SECRET = process.env.VOIP_TOKEN_SECRET || '';
const WORKERS = Math.max(1, Number(process.env.VOIP_WORKERS || 2));
/** Receipts that belong to a call (the rest are message receipts and stay with Baileys). */
const CALL_RECEIPT_TAGS = new Set([
  'offer',
  'accept',
  'preaccept',
  'reject',
  'terminate',
]);

const log = (
  level: 'info' | 'warn' | 'error',
  msg: string,
  extra: Record<string, unknown> = {},
) =>
  process.stderr.write(JSON.stringify({ level, context: 'voip', msg, ...extra }) + '\n');

type Slot = {
  index: number;
  port: number;
  child: ChildProcess | null;
  ready: boolean;
  restarts: number;
  startedAt: number;
};

class VoipSupervisor {
  private readonly slots: Slot[] = [];
  private readonly instances = new Map<string, VoipInstance>();
  private stopping = false;

  get enabled() {
    return Boolean(SECRET);
  }

  start() {
    if (!this.enabled || this.slots.length) return;
    for (let index = 0; index < WORKERS; index++) {
      const slot: Slot = {
        index,
        port: 0, // the worker picks a free one and reports it
        child: null,
        ready: false,
        restarts: 0,
        startedAt: 0,
      };
      this.slots.push(slot);
      this.spawn(slot);
    }
    log('info', 'call workers starting', { workers: WORKERS });
  }

  stop() {
    this.stopping = true;
    for (const slot of this.slots) slot.child?.kill();
  }

  /** A new Baileys socket for an instance: listen for its call stanzas. */
  attach(instance: VoipInstance) {
    if (!this.enabled) return;
    const sock = instance.client;
    if (!sock?.ws) return;
    this.instances.set(instance.instanceName, instance);

    const wantsCalls = () =>
      Boolean(instance.webhook?.enabled && instance.webhook?.events?.callUpsert);
    const forward = (node: VoipNode) => {
      // Only this instance's current socket, and only if the CRM has it.
      if (instance.client !== sock || !wantsCalls()) return;
      this.toWorker(instance.instanceName, {
        kind: 'node',
        instance: instance.instanceName,
        node,
        credentials: this.credentials(sock),
      });
    };

    sock.ws.on('CB:call', forward);
    sock.ws.on('CB:ack,class:call', forward);
    sock.ws.on('CB:receipt', (node: VoipNode) => {
      const first = Array.isArray(node.content) ? node.content[0] : undefined;
      if (first && CALL_RECEIPT_TAGS.has(first.tag)) forward(node);
    });
    // A socket closing is usually a reconnect; the calls' media doesn't ride it, so they carry
    // on (signalling resumes on the new socket). Calls whose number is really gone end through
    // the worker's silence watchdog.
  }

  /** The browser's media socket: hand the raw connection to the worker that owns the call. */
  attachUpgrade(server: Server) {
    if (!this.enabled) return;
    server.on('upgrade', (req: IncomingMessage, socket: Socket, head: Buffer) => {
      const url = new URL(req.url || '/', 'http://local');
      if (!url.pathname.startsWith('/voip/')) return;
      if (url.pathname !== '/voip/ws') {
        socket.destroy();
        return;
      }
      const instance = peekTicketInstance(url.searchParams.get('token') || '');
      const slot = instance ? this.slotFor(instance) : null;
      if (!slot?.ready) {
        socket.end('HTTP/1.1 503 Service Unavailable\r\n\r\n');
        return;
      }
      const upstream = connect(slot.port, '127.0.0.1', () => {
        const headers: string[] = [];
        for (let i = 0; i < req.rawHeaders.length; i += 2)
          headers.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
        upstream.write(
          `${req.method} ${req.url} HTTP/1.1\r\n${headers.join('\r\n')}\r\n\r\n`,
        );
        if (head?.length) upstream.write(head);
        socket.pipe(upstream).pipe(socket);
      });
      const close = () => {
        socket.destroy();
        upstream.destroy();
      };
      upstream.on('error', close);
      socket.on('error', close);
    });
  }

  // ── Workers ──────────────────────────────────────────────────────────────────

  private spawn(slot: Slot) {
    const ts = __filename.endsWith('.ts');
    const child = fork(join(__dirname, ts ? 'voip.worker.ts' : 'voip.worker.js'), [], {
      env: { ...process.env, VOIP_WORKER_PORT: '0' },
      execArgv: ts ? ['-r', 'ts-node/register/transpile-only'] : [],
      serialization: 'advanced',
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });
    slot.child = child;
    slot.ready = false;
    slot.startedAt = Date.now();

    child.on('message', (message: FromWorker) => this.fromWorker(slot, message));
    child.on('error', (error) =>
      log('error', 'call worker error', { worker: slot.index, error: error.message }),
    );
    child.on('exit', (code, signal) => {
      slot.child = null;
      slot.ready = false;
      if (this.stopping) return;
      // Quick back-off for a worker that keeps dying; a long-lived one restarts at once.
      if (Date.now() - slot.startedAt > 60_000) slot.restarts = 0;
      const delay = Math.min(30_000, 1000 * 2 ** slot.restarts++);
      log('error', 'call worker exited; restarting', {
        worker: slot.index,
        code,
        signal,
        inMs: delay,
      });
      setTimeout(() => this.spawn(slot), delay).unref();
    });
  }

  private slotFor(instance: string): Slot {
    const h = createHash('sha1').update(instance).digest().readUInt32BE(0);
    return this.slots[h % this.slots.length];
  }

  private toWorker(instance: string, message: ToWorker) {
    const slot = this.slotFor(instance);
    if (!slot?.child?.connected) return;
    try {
      slot.child.send(message);
    } catch (error) {
      log('warn', 'could not reach call worker', {
        worker: slot.index,
        error: (error as Error).message,
      });
    }
  }

  private fromWorker(slot: Slot, message: FromWorker) {
    if (message.kind === 'ready') {
      slot.port = message.port;
      slot.ready = true;
      log('info', 'call worker ready', { worker: slot.index, port: message.port });
    } else if (message.kind === 'stats') {
      if (message.calls)
        log('info', 'call worker load', {
          worker: slot.index,
          calls: message.calls,
          instances: message.instances,
        });
    } else if (message.kind === 'request') {
      // Reply to the worker that asked: after a restart, its replacement reuses request ids.
      const child = slot.child;
      const reply = (m: ToWorker) =>
        child === slot.child && child?.connected && child.send(m);
      void this.serve(message.instance, message.request).then(
        (value) => reply({ kind: 'reply', id: message.id, ok: true, value }),
        (error) =>
          reply({
            kind: 'reply',
            id: message.id,
            ok: false,
            error: (error as Error)?.message || String(error),
          }),
      );
    }
  }

  // ── Requests from a worker, served with the instance's socket ──────────────────

  private credentials(sock: any): VoipCredentials {
    return {
      meJid: sock?.user?.id,
      meLid: sock?.user?.lid,
      signedIdentity: sock?.authState?.creds?.account,
    };
  }

  /** Baileys keeps Signal sessions under the LID: map a phone-number jid first, as it does. */
  private async sessionJid(sock: any, jid: string): Promise<string> {
    if (!jid.endsWith('@s.whatsapp.net')) return jid;
    const lid = await sock.signalRepository?.lidMapping
      ?.getLIDForPN(jid)
      .catch(() => null);
    return lid || jid;
  }

  private async serve(instanceName: string, request: HostRequest): Promise<unknown> {
    const sock = this.instances.get(instanceName)?.client;
    if (!sock) throw new Error('instance not connected');
    switch (request.op) {
      case 'sendNode':
        return sock.sendNode(toBaileysNode(request.node));
      case 'credentials':
        return this.credentials(sock);
      case 'decrypt':
        return sock.signalRepository.decryptMessage({
          jid: await this.sessionJid(sock, request.jid),
          type: request.type,
          ciphertext: Buffer.from(request.ciphertext),
        });
      case 'devices': {
        const devices: {
          jid?: string;
          user: string;
          device?: number;
          server?: string;
        }[] = await sock.getUSyncDevices(request.jids, true, false);
        return request.jids.map((jid) => {
          const user = jid.split('@')[0].split(':')[0];
          return {
            jid,
            deviceJids: devices
              .filter((d) => d.user === user)
              .map(
                (d) =>
                  d.jid ||
                  `${d.user}${d.device ? `:${d.device}` : ''}@${d.server || jid.split('@')[1]}`,
              ),
          };
        });
      }
      case 'lidForPn':
        return Promise.all(
          request.jids.map(async (phoneJid) => ({
            phoneJid,
            lidJid: await sock.signalRepository?.lidMapping
              ?.getLIDForPN(phoneJid)
              .catch(() => null),
          })),
        );
      case 'encrypt':
        return sock.signalRepository.encryptMessage({
          jid: await this.sessionJid(sock, request.jid),
          data: Buffer.from(request.data),
        });
      case 'assertSession':
        return sock.assertSessions([await this.sessionJid(sock, request.jid)]);
      case 'assertSessions':
        return sock.assertSessions(
          await Promise.all(request.jids.map((jid) => this.sessionJid(sock, jid))),
        );
      case 'encryptBatch': {
        const out: { type: string; ciphertext: Uint8Array }[] = [];
        for (const item of request.items) {
          out.push(
            await sock.signalRepository.encryptMessage({
              jid: await this.sessionJid(sock, item.jid),
              data: Buffer.from(item.data),
            }),
          );
        }
        return out;
      }
      case 'tcToken': {
        // Stored per user under the LID, as Baileys does for its own 1:1 sends.
        const user = request.jid.replace(/:\d+@/, '@');
        const key = await this.sessionJid(sock, user);
        const entry = (await sock.authState?.keys?.get('tctoken', [key]))?.[key];
        return entry?.token?.length ? entry.token : null;
      }
      default:
        throw new Error('unknown request');
    }
  }
}

/** Baileys' encoder wants Buffers for binary content; IPC delivers Uint8Arrays. */
function toBaileysNode(node: VoipNode): any {
  const content = Array.isArray(node.content)
    ? node.content.map(toBaileysNode)
    : node.content instanceof Uint8Array
      ? Buffer.from(node.content)
      : node.content;
  return {
    tag: node.tag,
    attrs: node.attrs || {},
    ...(content === undefined ? {} : { content }),
  };
}

export const voipSupervisor = new VoipSupervisor();
