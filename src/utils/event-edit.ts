import { aesDecryptGCM, aesEncryptGCM, proto } from '@whiskeysockets/baileys';
import { hkdfSync, randomBytes } from 'crypto';
import { PollAddressing } from './poll-vote';

/**
 * WhatsApp edits (and cancels) an event with a secretEncryptedMessage of type EVENT_EDIT: the
 * whole new event, as a Message holding an eventMessage, encrypted with a key derived from the
 * event's own messageSecret. The derivation is WhatsApp's "use case secret" (zapo-js
 * createUseCaseSecret, whatsmeow generateMsgSecretKey): HKDF-SHA256 over the secret, no salt,
 * info = event id + event creator jid + editor jid + "Event Edit", 32 bytes. AES-256-GCM with a
 * 12-byte IV and, unlike poll votes and event responses, no additional data.
 */
export const EVENT_EDIT_INFO = 'Event Edit';

export const isEventType = (messageType?: string | null) => messageType === 'eventMessage';

type EditContext = {
  eventSecret: Uint8Array;
  eventMsgId: string;
  eventCreatorJid: string;
  editorJid: string;
};

const editKey = ({ eventSecret, eventMsgId, eventCreatorJid, editorJid }: EditContext) =>
  Buffer.from(
    hkdfSync(
      'sha256',
      eventSecret,
      Buffer.alloc(0),
      Buffer.from(eventMsgId + eventCreatorJid + editorJid + EVENT_EDIT_INFO),
      32,
    ),
  );

/** Encrypts the whole edited event, as WhatsApp's own edit does. */
export const encryptEventEdit = (
  event: proto.Message.IEventMessage,
  ctx: EditContext,
): { encPayload: Buffer; encIv: Buffer } => {
  const plaintext = proto.Message.encode({ eventMessage: event }).finish();
  const encIv = randomBytes(12);
  return {
    encPayload: aesEncryptGCM(plaintext, editKey(ctx), encIv, Buffer.alloc(0)),
    encIv,
  };
};

/**
 * Decrypts an event edit. As with votes, which jid form (phone number or LID) the editor used is
 * not on the wire, so each candidate pair is tried; GCM's tag rejects the wrong ones.
 */
export const readEventEdit = (
  enc: proto.Message.ISecretEncryptedMessage,
  ctx: {
    eventSecret: Uint8Array;
    eventMsgId: string;
    creators: (string | null | undefined)[];
    editors: (string | null | undefined)[];
  },
):
  | { event: proto.Message.IEventMessage; editorJid: string; addressing: PollAddressing }
  | undefined => {
  if (!enc?.encPayload || !enc?.encIv) {
    return undefined;
  }
  const unique = (jids: (string | null | undefined)[]) => [...new Set(jids.filter(Boolean))];
  for (const eventCreatorJid of unique(ctx.creators)) {
    for (const editorJid of unique(ctx.editors)) {
      try {
        const plaintext = aesDecryptGCM(
          Buffer.from(enc.encPayload),
          editKey({ eventSecret: ctx.eventSecret, eventMsgId: ctx.eventMsgId, eventCreatorJid, editorJid }),
          Buffer.from(enc.encIv),
          Buffer.alloc(0),
        );
        const event = proto.Message.decode(plaintext).eventMessage;
        if (!event) {
          return undefined;
        }
        return {
          event,
          editorJid,
          addressing: eventCreatorJid.endsWith('@lid') ? 'lid' : 'pn',
        };
      } catch {
        // not this pair
      }
    }
  }
  return undefined;
};

/**
 * The event's fields as plain JSON, the shape stored and sent in webhooks: times as unix seconds,
 * location as its name.
 */
export const eventFields = (event: proto.Message.IEventMessage) => {
  const seconds = (value: unknown) => {
    const n = Number(value?.toString?.() ?? value);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  };
  return {
    name: event.name ?? undefined,
    description: event.description || undefined,
    startTime: seconds(event.startTime),
    endTime: seconds(event.endTime),
    location: event.location?.name || undefined,
    joinLink: event.joinLink || undefined,
    isCanceled: !!event.isCanceled,
    extraGuestsAllowed: !!event.extraGuestsAllowed,
    isScheduleCall: !!event.isScheduleCall,
    hasReminder: event.hasReminder ?? undefined,
    reminderOffsetSec: seconds(event.reminderOffsetSec),
  };
};
