import {
  aesEncryptGCM,
  decryptPollVote,
  hmacSign,
  proto,
  sha256,
} from '@whiskeysockets/baileys';
import { randomBytes } from 'crypto';

/**
 * Poll votes travel end-to-end encrypted with a key derived from the poll's own messageSecret
 * (its messageContextInfo), which WhatsApp never sends again. So the secret is kept inside the
 * poll's stored content under these keys, next to which jid form ("pn" or "lid") the chat's votes
 * are encrypted with, and stripped again before the content leaves through a webhook or response.
 */
export const POLL_SECRET_KEY = 'messageSecret';
export const POLL_ADDRESSING_KEY = 'voteAddressing';

export type PollAddressing = 'pn' | 'lid';

const POLL_TYPES = new Set([
  'pollCreationMessage',
  'pollCreationMessageV2',
  'pollCreationMessageV3',
]);

export const isPollType = (messageType?: string | null) => POLL_TYPES.has(messageType ?? '');

/** The poll's messageSecret as base64, the shape it is stored in. */
export const pollSecretOf = (message?: proto.IMessage | null): string | undefined => {
  const secret = message?.messageContextInfo?.messageSecret;
  return secret?.length ? Buffer.from(secret).toString('base64') : undefined;
};

/** A copy of stored content without the poll's secret, for anything that leaves CodeChat. */
export const withoutPollSecret = <T>(content: T): T => {
  if (!content || typeof content !== 'object' || !(POLL_SECRET_KEY in content)) {
    return content;
  }
  const copy = { ...(content as Record<string, unknown>) };
  delete copy[POLL_SECRET_KEY];
  delete copy[POLL_ADDRESSING_KEY];
  return copy as T;
};

/** WhatsApp names a chosen option by the SHA-256 of its text. */
export const pollOptionHash = (name: string) => sha256(Buffer.from(name));

type VoteContext = {
  pollEncKey: Uint8Array;
  pollMsgId: string;
  pollCreatorJid: string;
  voterJid: string;
};

/**
 * The inverse of Baileys' decryptPollVote, step for step: the same HMAC key derivation and
 * AES-256-GCM additional data, with a fresh 12-byte IV.
 */
export const encryptPollVote = (
  options: string[],
  { pollEncKey, pollMsgId, pollCreatorJid, voterJid }: VoteContext,
): proto.Message.IPollEncValue => {
  const sign = Buffer.concat([
    Buffer.from(pollMsgId),
    Buffer.from(pollCreatorJid),
    Buffer.from(voterJid),
    Buffer.from('Poll Vote'),
    new Uint8Array([1]),
  ]);
  const key0 = hmacSign(pollEncKey, new Uint8Array(32), 'sha256');
  const encKey = hmacSign(sign, key0, 'sha256');
  const aad = Buffer.from(`${pollMsgId}\u0000${voterJid}`);
  const plaintext = proto.Message.PollVoteMessage.encode({
    selectedOptions: options.map(pollOptionHash),
  }).finish();
  const encIv = randomBytes(12);
  return { encPayload: aesEncryptGCM(plaintext, encKey, encIv, aad), encIv };
};

/**
 * Decrypts a vote and names its choices. Which jid form the voter's phone used (phone number or
 * LID) is not on the wire, so each candidate pair is tried; GCM's tag rejects the wrong ones.
 */
export const readPollVote = (
  vote: proto.Message.IPollEncValue,
  ctx: {
    pollEncKey: Uint8Array;
    pollMsgId: string;
    creators: (string | null | undefined)[];
    voters: (string | null | undefined)[];
    optionNames: string[];
  },
): { options: string[]; voterJid: string; addressing: PollAddressing } | undefined => {
  const unique = (jids: (string | null | undefined)[]) => [...new Set(jids.filter(Boolean))];
  const names = new Map(ctx.optionNames.map((name) => [pollOptionHash(name).toString('hex'), name]));
  for (const pollCreatorJid of unique(ctx.creators)) {
    for (const voterJid of unique(ctx.voters)) {
      try {
        const decoded = decryptPollVote(vote, {
          pollEncKey: ctx.pollEncKey,
          pollMsgId: ctx.pollMsgId,
          pollCreatorJid,
          voterJid,
        });
        const options = (decoded.selectedOptions ?? [])
          .map((hash) => names.get(Buffer.from(hash).toString('hex')))
          .filter((name): name is string => !!name);
        return {
          options,
          voterJid,
          addressing: pollCreatorJid.endsWith('@lid') ? 'lid' : 'pn',
        };
      } catch {
        // not this pair
      }
    }
  }
  return undefined;
};
