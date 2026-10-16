import { WAMessageKey } from '@whiskeysockets/baileys';

const isJid = (v: string) => {
  // Older groups' ids have a hyphen ("<creator>-<created>@g.us"): without it in the pattern, every
  // message of such a group lost its chat id (and was dropped downstream).
  const regexp = new RegExp(/^[\w-]+@(s\.whatsapp\.net|g\.us|broadcast|newsletter)$/i);
  return regexp.test(v);
};

const isLid = (v: string) => v.endsWith('@lid');

const extractUser = (...values: (string | undefined)[]) => {
  let jid: string | undefined;
  let lid: string | undefined;

  for (const v of values) {
    if (!v) continue;

    if (!lid && isLid(v)) {
      lid = v;
    }

    if (!jid && isJid(v)) {
      jid = v;
    }
  }

  return { jid, lid };
};

export const getJidUser = (key: WAMessageKey) => {
  return extractUser(key?.remoteJid, key?.remoteJid, key?.remoteJidAlt);
};

export const getUserGroup = (key: WAMessageKey, participant?: string) => {
  return extractUser(key?.participant, key?.participantAlt, participant);
};
