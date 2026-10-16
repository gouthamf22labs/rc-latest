import { aesDecryptGCM, proto } from '@whiskeysockets/baileys';
import { editKey } from './event-edit';

/**
 * WhatsApp now edits a message with a secretEncryptedMessage of type MESSAGE_EDIT (the plain
 * protocolMessage edit is what older phones, and this device, send): the edit as a Message,
 * encrypted with a key derived from the original message's own messageSecret, the way an event's
 * edit is (event-edit): HKDF-SHA256 over the secret, info = message id + its sender's jid + the
 * editor's jid + "Message Edit" (whatsmeow EncSecretMessageEdit), AES-256-GCM, no additional data.
 * So every message keeps its secret (under POLL_SECRET_KEY, stripped before anything leaves).
 */
export const MESSAGE_EDIT_INFO = 'Message Edit';

/** Stored content's own field: when (unix seconds) the newest edit kept on it was made. */
export const EDITED_AT_KEY = 'editedAt';

/**
 * A message's secret as base64, the shape it is stored in: in its messageContextInfo, or (history)
 * on the message itself.
 */
export const messageSecretOf = (
  m?: { message?: proto.IMessage | null; messageSecret?: Uint8Array | null } | null,
): string | undefined => {
  const secret = m?.message?.messageContextInfo?.messageSecret ?? m?.messageSecret;
  return secret?.length ? Buffer.from(secret).toString('base64') : undefined;
};

/** Bytes as protobuf objects hold them, or as stored JSON (base64). */
const bytes = (v: unknown) =>
  typeof v === 'string' ? Buffer.from(v, 'base64') : Buffer.from((v ?? []) as Uint8Array);

const isEdit = (type: unknown, name: 'MESSAGE_EDIT', value: number) => type === name || type === value;

/** Whether a protocolMessage (live, or stored with its type as a number) is an edit. */
export const isProtocolEdit = (pm?: { type?: unknown } | null) =>
  isEdit(pm?.type, 'MESSAGE_EDIT', proto.Message.ProtocolMessage.Type.MESSAGE_EDIT);

/** Whether a secretEncryptedMessage is a message's edit (not an event's). */
export const isSecretMessageEdit = (enc?: { secretEncType?: unknown } | null) =>
  isEdit(enc?.secretEncType, 'MESSAGE_EDIT', proto.Message.SecretEncryptedMessage.SecretEncType.MESSAGE_EDIT);

/**
 * The words an edit leaves a message with: its text, or a photo's, video's or document's caption.
 * Reads the edit however it comes (the new message itself, or wrapped in a protocolMessage edit or
 * history's editedMessage), from a protobuf object or its stored JSON alike.
 */
export const editedTextOf = (message: any, depth = 0): string | undefined => {
  if (!message || typeof message !== 'object' || depth > 4) return undefined;
  const inner =
    message.editedMessage?.message ??
    message.protocolMessage?.editedMessage ??
    message.ephemeralMessage?.message ??
    message.documentWithCaptionMessage?.message;
  if (inner) return editedTextOf(inner, depth + 1);
  const text =
    message.conversation ??
    message.extendedTextMessage?.text ??
    message.imageMessage?.caption ??
    message.videoMessage?.caption ??
    message.documentMessage?.caption;
  return typeof text === 'string' && text ? text : undefined;
};

/** When an edit says it was made (unix seconds), if it does. */
export const editedAtOf = (message: any): number | undefined => {
  const ms = Number(
    message?.protocolMessage?.timestampMs ?? message?.editedMessage?.message?.protocolMessage?.timestampMs ?? 0,
  );
  return ms > 0 ? Math.floor(ms / 1000) : undefined;
};

/** Which field of a stored message an edit rewrites: its text, or its caption. */
export const EDITED_FIELD: Record<string, string> = {
  conversation: 'text',
  extendedTextMessage: 'text',
  imageMessage: 'caption',
  videoMessage: 'caption',
  documentMessage: 'caption',
};

/**
 * Decrypts a message's edit. As with event edits, which jid form (number or LID) the sender and
 * editor were named by isn't on the wire, so each pair is tried; GCM's tag rejects the wrong ones.
 */
export const readMessageEdit = (
  enc: { encPayload?: unknown; encIv?: unknown },
  ctx: {
    secret: Uint8Array;
    messageId: string;
    creators: (string | null | undefined)[];
    editors: (string | null | undefined)[];
  },
): proto.IMessage | undefined => {
  if (!enc?.encPayload || !enc?.encIv) return undefined;
  const unique = (jids: (string | null | undefined)[]) => [...new Set(jids.filter(Boolean))];
  for (const creator of unique(ctx.creators)) {
    for (const editor of unique(ctx.editors)) {
      try {
        const plaintext = aesDecryptGCM(
          bytes(enc.encPayload),
          editKey({
            eventSecret: ctx.secret,
            eventMsgId: ctx.messageId,
            eventCreatorJid: creator,
            editorJid: editor,
            info: MESSAGE_EDIT_INFO,
          }),
          bytes(enc.encIv),
          Buffer.alloc(0),
        );
        return proto.Message.decode(plaintext);
      } catch {
        // not this pair
      }
    }
  }
  return undefined;
};

