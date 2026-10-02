/**
 * Runs zapo's call engine (@zapo-js/voip) on a Baileys connection it never touches directly.
 *
 * The engine is written as a plugin for zapo's own WhatsApp client and asks that client for a
 * handful of things: send a node, our own jids, decrypt the call key with the caller's Signal
 * session, the caller's device list. This builds a stand-in for that client whose every answer
 * comes from the instance's Baileys socket, in the main process, over IPC. One InstanceHost per
 * CodeChat instance; the calls themselves (relays, SRTP, codec) live in the engine it hosts.
 */
import { EventEmitter } from 'events';
import { voipPlugin } from '@zapo-js/voip';
import type { HostRequest, VoipCredentials, VoipNode } from './voip.protocol';

/** zapo's Logger contract, enough for the engine. */
type ZapoLogger = {
  readonly level: string;
  trace(message: string, context?: Record<string, unknown>): void;
  debug(message: string, context?: Record<string, unknown>): void;
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  child(bindings: Record<string, unknown>, options?: { level?: string }): ZapoLogger;
};

const LEVELS = ['trace', 'debug', 'info', 'warn', 'error'] as const;

/** Writes engine logs at or above `level` as one JSON line to stderr (picked up with the rest). */
function makeLogger(level: string, bindings: Record<string, unknown> = {}): ZapoLogger {
  const min = Math.max(0, LEVELS.indexOf(level as (typeof LEVELS)[number]));
  const at =
    (lvl: (typeof LEVELS)[number]) =>
    (message: string, context?: Record<string, unknown>) => {
      if (LEVELS.indexOf(lvl) < min) return;
      process.stderr.write(
        JSON.stringify({
          level: lvl,
          context: 'voip',
          msg: message,
          ...bindings,
          ...context,
        }) + '\n',
      );
    };
  return {
    level,
    trace: at('trace'),
    debug: at('debug'),
    info: at('info'),
    warn: at('warn'),
    error: at('error'),
    child: (more, options) =>
      makeLogger(options?.level ?? level, { ...bindings, ...more }),
  };
}

type Rpc = (request: HostRequest) => Promise<any>;
type Handler = {
  tag: string;
  prepend?: boolean;
  handler: (node: VoipNode) => Promise<boolean> | boolean;
};

/** The engine's public surface we drive (WaVoipCoordinator). */
export type VoipEngine = {
  startCall(options: { peerJid: string; isVideo?: boolean }): Promise<string>;
  acceptCall(callId: string): Promise<void>;
  rejectCall(callId: string, reason?: string): Promise<void>;
  endCall(callId: string, reason?: string): Promise<void>;
  setMute(callId: string, muted: boolean): void;
  setExternalAudioMode(callId: string, enabled: boolean): void;
  feedLiveAudio(callId: string, data: Float32Array): number;
  feedLiveVideo(callId: string, data: Uint8Array, timestampUs: number): number;
  getCall(callId: string): any;
  getCalls(): readonly any[];
  dispose(): void;
};

/** "user:device@server" from zapo's Signal address. */
const addressToJid = (a: { user: string; server?: string; device: number }) =>
  `${a.user}${a.device ? `:${a.device}` : ''}@${a.server || 's.whatsapp.net'}`;

export class InstanceHost extends EventEmitter {
  readonly engine: VoipEngine;
  private credentials: VoipCredentials | null = null;
  private readonly handlers: Handler[] = [];

  constructor(
    readonly instance: string,
    private readonly rpc: Rpc,
    options: { maxConcurrentCalls: number; logLevel: string },
  ) {
    super();
    const logger = makeLogger(options.logLevel, { instance });
    const ctx = {
      logger,
      deps: this.deps(),
      stores: {
        // The customer's privacy token, which WhatsApp wants on a call we place.
        privacyToken: {
          getByJid: async (jid: string) => {
            const tcToken = await rpc({ op: 'tcToken', jid }).catch(() => null);
            return tcToken ? { tcToken } : null;
          },
        },
      },
      emit: (event: string | symbol, ...args: unknown[]) => this.emit(event, ...args),
      on: () => undefined,
      off: () => undefined,
      once: () => undefined,
      registerIncomingHandler: (registration: Handler) => {
        if (registration.prepend) this.handlers.unshift(registration);
        else this.handlers.push(registration);
        return () => {
          const i = this.handlers.indexOf(registration);
          if (i >= 0) this.handlers.splice(i, 1);
        };
      },
      registerIncomingStanzaFilter: () => () => undefined,
      registerDispose: () => undefined,
    };
    this.engine = voipPlugin({
      maxConcurrentCalls: options.maxConcurrentCalls,
      // WhatsApp's relays answer on the port they advertise (3478); the engine's default,
      // WhatsApp Web's 3480, timed out from servers in testing.
      useOriginalRelayPort: true,
    }).setup(ctx as any) as unknown as VoipEngine;
  }

  setCredentials(credentials: VoipCredentials) {
    this.credentials = credentials;
  }

  /** A call-related node from this instance's socket: the engine's handlers, first taker wins. */
  async dispatch(node: VoipNode): Promise<void> {
    for (const h of [...this.handlers]) {
      if (h.tag !== node.tag) continue;
      if (await h.handler(node)) return;
    }
  }

  dispose() {
    try {
      this.engine.dispose();
    } catch {
      // already torn down
    }
    this.removeAllListeners();
  }

  private deps() {
    const rpc = this.rpc;
    return {
      authClient: { getCurrentCredentials: () => this.credentials },
      lowLevelCoordinator: {
        sendNode: async (node: VoipNode) => {
          // Baileys already acknowledges every <call> and <receipt> it receives; a second ack
          // from the engine would be a duplicate on the wire.
          if (
            node.tag === 'ack' &&
            (node.attrs?.class === 'call' || node.attrs?.class === 'receipt')
          )
            return;
          await rpc({ op: 'sendNode', node });
        },
      },
      signalProtocol: {
        decryptMessage: (
          address: any,
          message: { type: 'pkmsg' | 'msg'; ciphertext: Uint8Array },
        ) =>
          rpc({
            op: 'decrypt',
            jid: addressToJid(address),
            type: message.type,
            ciphertext: message.ciphertext,
          }),
        // Answering re-encrypts the call key to the caller's device.
        encryptMessage: (address: any, data: Uint8Array) =>
          rpc({ op: 'encrypt', jid: addressToJid(address), data }),
        // Placing a call encrypts the call key to each of the customer's devices.
        encryptMessagesBatch: (items: { address: any; plaintext: Uint8Array }[]) =>
          rpc({
            op: 'encryptBatch',
            items: items.map((item) => ({
              jid: addressToJid(item.address),
              data: item.plaintext,
            })),
          }),
      },
      signalDeviceSync: {
        syncDeviceList: (jids: string[]) => rpc({ op: 'devices', jids }),
        queryLidsByPhoneJids: (jids: string[]) => rpc({ op: 'lidForPn', jids }),
      },
      sessionResolver: {
        // Baileys keeps the sessions; these entries only line up with the devices.
        ensureSessionsBatch: async (jids: string[]) => {
          await rpc({ op: 'assertSessions', jids });
          return jids.map(() => ({ address: null, session: null }));
        },
      },
      messageDispatch: {
        // The engine asks before accepting; the session already exists (we decrypted the key).
        syncSignalSession: (jid: string) =>
          rpc({ op: 'assertSession', jid }).catch(() => undefined),
      },
    };
  }
}
