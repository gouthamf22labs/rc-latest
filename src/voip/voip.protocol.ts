/**
 * Calls answered in the browser (CRM). WhatsApp's call signalling rides each number's Baileys
 * socket in the main process; the media (relays, SRTP, codec) runs in separate call workers so
 * a failure there can never take messaging down. This file is what the two sides say to each
 * other over IPC, and the token the CRM backend signs for one browser to join one call.
 */
import { createHmac, timingSafeEqual } from 'crypto';

/** A WhatsApp binary node, as both Baileys and zapo shape it. */
export type VoipNode = {
  tag: string;
  attrs: Record<string, string>;
  content?: VoipNode[] | Uint8Array | string;
};

/** The number's own jids, as the engine needs them on every call. */
export type VoipCredentials = {
  meJid?: string;
  meLid?: string;
  /** ADVSignedDeviceIdentity (Baileys creds.account), sent with a pkmsg accept. */
  signedIdentity?: unknown;
};

/** Main → worker. */
export type ToWorker =
  | { kind: 'node'; instance: string; node: VoipNode; credentials: VoipCredentials }
  | { kind: 'reply'; id: number; ok: true; value: unknown }
  | { kind: 'reply'; id: number; ok: false; error: string }
  | { kind: 'instanceGone'; instance: string };

/** What a worker may ask the main process to do with an instance's socket. */
export type HostRequest =
  | { op: 'sendNode'; node: VoipNode }
  | { op: 'credentials' }
  | { op: 'decrypt'; jid: string; type: 'pkmsg' | 'msg'; ciphertext: Uint8Array }
  | { op: 'encrypt'; jid: string; data: Uint8Array }
  | { op: 'devices'; jids: string[] }
  | { op: 'lidForPn'; jids: string[] }
  | { op: 'assertSession'; jid: string }
  // Placing a call
  | { op: 'encryptBatch'; items: { jid: string; data: Uint8Array }[] }
  | { op: 'assertSessions'; jids: string[] }
  | { op: 'tcToken'; jid: string };

/** Worker → main. */
export type FromWorker =
  | { kind: 'request'; id: number; instance: string; request: HostRequest }
  | { kind: 'ready'; port: number }
  | { kind: 'stats'; calls: number; instances: number };

// ── Call token ─────────────────────────────────────────────────────────────────

/** Who may join which call, until when. Signed by the CRM backend with VOIP_TOKEN_SECRET. */
export type CallTicket = {
  /** CodeChat instance (the number's current connection). */
  i: string;
  /** WhatsApp call id, to answer a ringing call ('' when placing one). */
  c: string;
  /** To place a call: who to call (phone or LID jid), and whether with video. */
  p?: string;
  v?: boolean;
  /** CRM member, for logs. */
  m: string;
  /** Expiry, unix ms. */
  e: number;
};

const b64url = (b: Buffer) => b.toString('base64url');

export function signCallTicket(ticket: CallTicket, secret: string): string {
  const body = b64url(Buffer.from(JSON.stringify(ticket)));
  return `${body}.${b64url(createHmac('sha256', secret).update(body).digest())}`;
}

/** The ticket, or null when it is malformed, forged or expired. */
export function readCallTicket(token: string, secret: string): CallTicket | null {
  if (!secret || typeof token !== 'string') return null;
  const [body, sig] = token.split('.');
  if (!body || !sig) return null;
  const expected = createHmac('sha256', secret).update(body).digest();
  const given = Buffer.from(sig, 'base64url');
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  try {
    const t = JSON.parse(Buffer.from(body, 'base64url').toString()) as CallTicket;
    // An answer names the call (c); a new call names who to call (p).
    if (!t?.i || !(t?.c || t?.p) || typeof t.e !== 'number' || t.e < Date.now())
      return null;
    return t;
  } catch {
    return null;
  }
}

/** The instance a ticket is for, without checking it (routing only; the worker verifies). */
export function peekTicketInstance(token: string): string | null {
  try {
    const t = JSON.parse(
      Buffer.from(token.split('.')[0], 'base64url').toString(),
    ) as CallTicket;
    return typeof t?.i === 'string' ? t.i : null;
  } catch {
    return null;
  }
}

// ── Browser media frames ───────────────────────────────────────────────────────
// Binary WebSocket frames between the browser and a worker; the first byte says what follows.
//   0x01 audio  — float32 little-endian PCM, 16 kHz mono (both directions)
//   0x02 video  — worker→browser: [keyFrame u8][timestamp f64, RTP 90 kHz ticks][H.264 Annex-B AU]
//                 browser→worker: [timestamp f64 µs][H.264 Annex-B access unit]
// Text frames carry JSON control messages ({ t: 'state' | 'ended' | 'error' | 'hangup' | 'mute' ... }).
export const FRAME_AUDIO = 0x01;
export const FRAME_VIDEO = 0x02;
