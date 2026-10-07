/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename chat.dto.ts                                                        │
 * │ Developed by: Cleber Wilson                                                  │
 * │ Creation date: Nov 27, 2022                                                  │
 * │ Contact: contato@codechat.dev                                                │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @copyright © Cleber Wilson 2022. All rights reserved.                        │
 * │ Licensed under the Apache License, Version 2.0                               │
 * │                                                                              │
 * │  @license "https://github.com/code-chat-br/whatsapp-api/blob/main/LICENSE"   │
 * │                                                                              │
 * │ You may not use this file except in compliance with the License.             │
 * │ You may obtain a copy of the License at                                      │
 * │                                                                              │
 * │    http://www.apache.org/licenses/LICENSE-2.0                                │
 * │                                                                              │
 * │ Unless required by applicable law or agreed to in writing, software          │
 * │ distributed under the License is distributed on an "AS IS" BASIS,            │
 * │ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     │
 * │                                                                              │
 * │ See the License for the specific language governing permissions and          │
 * │ limitations under the License.                                               │
 * │                                                                              │
 * │ @class                                                                       │
 * │ @constructs OnWhatsAppDto                                                    │
 * │ @param {String} jid @param {Boolean} exists @param {String} name             │
 * │                                                                              │
 * │ @class WhatsAppNumberDto @class NumberDto @class Key @class ReadMessageDto   │
 * │ @class LastMessage @class ArchiveChatDto @class DeleteMessage                │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import { WAPresence } from '@whiskeysockets/baileys';

export class OnWhatsAppDto {
  constructor(
    public readonly exists: boolean,
    public readonly jid: string,
    public readonly lid?: string,
    public readonly name?: string,
  ) {}
}

export class WhatsAppNumberDto {
  numbers: string[];
}

export class NumberDto {
  number: string;
}

export class UpdatePresenceDto extends NumberDto {
  presence: WAPresence;
}

export class FetchPresenceDto extends NumberDto {
  /** Bound on how long to wait for WhatsApp's first push on a cache miss. */
  waitMs?: number;
}

export class WatchPresenceDto extends NumberDto {
  /** Caller-owned id; re-registering the same id refreshes the watch. */
  watchId: string;
  ttlSeconds?: number;
  fireIfAlreadyOnline?: boolean;
  /** Fire only on a typing signal — see PresenceWatch.requireTyping. */
  requireTyping?: boolean;
  /**
   * ms epoch before which the watch must not fire. Omit for a watch that is
   * armed as soon as it is registered, which is the default.
   */
  notBefore?: number;
}

export class UnwatchPresenceDto {
  watchId?: string;
  number?: string;
}

class Key {
  id: string;
  fromMe: boolean;
  remoteJid: string;
  /** Group messages only: the sender's jid, needed for the read receipt. */
  participant?: string;
}
export class ReadMessageDto {
  readMessages: Key[];
}

export class ReadMessageIdDto {
  messageId: number[];
}

class LastMessage {
  key: Key;
  messageTimestamp?: number;
}

export class ArchiveChatDto {
  lastMessage: LastMessage;
  archive: boolean;
}

export class MessageId {
  id: string;
}

export class DeleteMessage extends MessageId {
  everyOne?: 'true' | 'false';
}

export class RejectCallDto {
  callId: string;
  callFrom: string;
}

export class EditMessage extends MessageId {
  text: string;
}

/** Forwards a stored message to other chats on this number, as WhatsApp does (up to 5). */
export class ForwardMessage extends MessageId {
  to: string[];
}

/** Pins (or unpins) a message in the chat for both sides, as WhatsApp does: for 24 h, 7 or 30 days. */
export class PinMessage extends MessageId {
  pin: boolean;
  time?: 86400 | 604800 | 2592000;
}

/**
 * Edits (or cancels) an event this number sent, as WhatsApp's Edit event does: the whole new
 * event. Times are unix seconds; `call` adds, swaps or (`none`) drops the WhatsApp call link,
 * left out keeps it; `isCanceled` cancels the event and keeps everything else.
 */
export class EditEvent extends MessageId {
  event: {
    name?: string;
    description?: string;
    startTime?: number;
    endTime?: number | null;
    location?: string | null;
    joinLink?: string | null;
    call?: 'audio' | 'video' | 'none';
    isCanceled?: boolean;
  };
}

/** Votes in a poll as this number: `id` is the poll's WhatsApp id, `options` our whole choice. */
export class PollVote extends MessageId {
  options: string[];
}
