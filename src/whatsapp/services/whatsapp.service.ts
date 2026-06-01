/**
 * ┌──────────────────────────────────────────────────────────────────────────────┐
 * │ @author jrCleber                                                             │
 * │ @filename whatsapp.service.ts                                                │
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
 * │ @constructs WAStartupService                                                 │
 * │ @param {ConfigService} configService                                         │
 * | @param {EventEmitter2} eventEmitter                                          │
 * │ @param {RepositoryBroker} repository                                         │
 * │ @param {RedisCache} cache                                                    │
 * ├──────────────────────────────────────────────────────────────────────────────┤
 * │ @important                                                                   │
 * │ For any future changes to the code in this file, it is recommended to        │
 * │ contain, together with the modification, the information of the developer    │
 * │ who changed it and the date of modification.                                 │
 * └──────────────────────────────────────────────────────────────────────────────┘
 */

import makeWASocket, {
  AnyMessageContent,
  BaileysEventMap,
  Browsers,
  BufferedEventData,
  CacheStore,
  Chat,
  ConnectionState,
  Contact,
  delay,
  DisconnectReason,
  downloadMediaMessage,
  generateWAMessageFromContent,
  getBinaryNodeChild,
  getContentType,
  getDevice,
  GroupMetadata,
  GroupParticipant,
  isJidGroup,
  isJidNewsletter,
  isLidUser,
  makeCacheableSignalKeyStore,
  MessageUpsertType,
  ParticipantAction,
  prepareWAMessageMedia,
  proto,
  useMultiFileAuthState,
  UserFacingSocketConfig,
  USyncQuery,
  USyncUser,
  WABrowserDescription,
  WACallEvent,
  WAConnectionState,
  WAMediaUpload,
  WAMessage,
  WAMessageUpdate,
  WASocket,
} from '@whiskeysockets/baileys';
import {
  ConfigService,
  ConfigSessionPhone,
  Database,
  GlobalWebhook,
  QrCode,
  ProviderSession,
  EnvProxy,
  LogLevel,
} from '../../config/env.config';
import { Logger } from '../../config/logger.config';
import { INSTANCE_DIR, ROOT_DIR } from '../../config/path.config';
import { join, normalize } from 'path';
import axios, { AxiosError } from 'axios';
import qrcode, { QRCodeToDataURLOptions } from 'qrcode';
import qrcodeTerminal from 'qrcode-terminal';
import { Boom } from '@hapi/boom';
import EventEmitter2 from 'eventemitter2';
import { release, tmpdir } from 'os';
import P from 'pino';
import {
  AudioMessageFileDto,
  ContactMessage,
  MediaFileDto,
  MediaMessage,
  Options,
  SendAudioDto,
  SendContactDto,
  SendEventDto,
  SendLinkDto,
  SendLocationDto,
  SendMediaDto,
  SendPollDto,
  SendQuizDto,
  SendReactionDto,
  SendStickerDto,
  SendTextDto,
} from '../dto/sendMessage.dto';
import { isArray, isBase64, isInt, isNotEmpty, isURL } from 'class-validator';
import {
  ArchiveChatDto,
  DeleteMessage,
  EditMessage,
  OnWhatsAppDto,
  ReadMessageDto,
  ReadMessageIdDto,
  RejectCallDto,
  UpdatePresenceDto,
  WhatsAppNumberDto,
} from '../dto/chat.dto';
import { BadRequestException, InternalServerErrorException } from '../../exceptions';
import {
  CreateGroupDto,
  GroupJid,
  GroupPictureDto,
  GroupUpdateParticipantDto,
} from '../dto/group.dto';
import NodeCache from 'node-cache';
import {
  AuthState,
  AuthStateProvider,
} from '../../utils/use-multi-file-auth-state-provider-files';
import mime from 'mime-types';
import { Instance, Webhook } from '@prisma/client';
import { WebhookEvents, WebhookEventsEnum, WebhookEventsType } from '../dto/webhook.dto';
import { Query, Repository } from '../../repository/repository.service';
import PrismType from '@prisma/client';
import * as s3Service from '../../integrations/minio/minio.utils';
import { ProviderFiles } from '../../provider/sessions';
import { Websocket } from '../../websocket/server';
import { ulid } from 'ulid';
import { isValidUlid } from '../../validate/ulid';
import sharp from 'sharp';
import ffmpeg from 'fluent-ffmpeg';
import { PassThrough } from 'stream';
import {
  accessSync,
  constants,
  existsSync,
  readFileSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from 'fs';
import { randomBytes, createHash } from 'crypto';
import { useDatabaseAuthState } from '../../utils/use-database-auth-state';
import { createProxyAgents } from '../../utils/proxy';
import { fetchLatestBaileysVersionV2 } from '../../utils/wa-version';
import { getJidUser, getUserGroup } from '../../utils/extract-id';
import { getObjectUrl } from '../../integrations/minio/minio.utils';
import { encodeProps } from '../../utils/encode.props';

export const MessageSubtype = () => [
  'ephemeralMessage',
  'documentWithCaptionMessage',
  'viewOnceMessage',
  'viewOnceMessageV2',
];

export const TypeMediaMessage = () => [
  'imageMessage',
  'documentMessage',
  'audioMessage',
  'videoMessage',
  'stickerMessage',
  'ptvMessage',
];

type InstanceQrCode = {
  count: number;
  paringCode?: string;
  code?: string;
  base64?: string;
};

type InstanceStateConnection = {
  state: 'refused' | WAConnectionState;
  statusReason?: number;
};

export class WAStartupService {
  constructor(
    private readonly configService: ConfigService,
    private readonly eventEmitter: EventEmitter2,
    private readonly repository: Repository,
    private readonly providerFiles: ProviderFiles,
    private readonly ws: Websocket,
  ) {
    this.authStateProvider = new AuthStateProvider(
      this.configService,
      this.providerFiles,
    );
  }

  private logger = new Logger(this.configService, 'wa-startup-service');
  private readonly instance: Partial<Instance> = {};
  private readonly webhook: Partial<Webhook> & { events?: WebhookEvents } = {};
  private readonly msgRetryCounterCache: CacheStore = new NodeCache({ stdTTL: 3600, maxKeys: 10000 });
  private readonly userDevicesCache: CacheStore = new NodeCache({ stdTTL: 1800, maxKeys: 50000 });
  private readonly instanceQr: InstanceQrCode = { count: 0 };
  private readonly stateConnection: InstanceStateConnection = { state: 'close' };
  private readonly databaseOptions: Database =
    this.configService.get<Database>('DATABASE');

  private endSession = false;
  public client: WASocket;
  private authState: Partial<AuthState> = {};
  private authStateProvider: AuthStateProvider;
  private phoneNumber: string;

  public async setPhoneNumber(v: string) {
    this.phoneNumber = v;
    try {
      if (this.configService.get<ProviderSession>('PROVIDER')?.ENABLED) {
        await this.providerFiles.removeSession(this.instance.name);
      } else {
        // Clear DB session so re-pairing starts a fresh login.
        // Guard against undefined id (Partial<Instance>) — an undefined filter would wipe all sessions.
        if (this.instance?.id) {
          await this.repository.session
            .deleteMany({ where: { instanceId: this.instance.id } })
            .catch((err) => this.logger.warn('session-cleanup-failed', err));
        }
        rmSync(join(INSTANCE_DIR, this.instance.name), { recursive: true, force: true });
      }
    } catch {
      //
    }
  }

  public async setInstanceName(name: string) {
    const i = await this.repository.instance.findUnique({
      where: { name },
    });

    Object.assign(this.instance, i);
    this.sendDataWebhook('statusInstance', {
      instance: this.instance.name,
      status: 'loaded',
    });

    this.logger = this.logger.setCtx(`${i.id}:${i.name}`);
  }

  public get instanceName() {
    return this.instance.name;
  }

  public get ownerJid() {
    return this.instance.ownerJid;
  }

  public get profilePictureUrl() {
    return this.instance.profilePicUrl;
  }

  public get qrCode(): Partial<InstanceQrCode> {
    return this.instanceQr;
  }

  public async loadWebhook() {
    const data = await this.repository.webhook.findFirst({
      where: { instanceId: this.instance.id },
    });
    if (!data) {
      return;
    }

    this.webhook.url = data?.url;
    this.webhook.enabled = data?.enabled;
    this.webhook.events = data?.events;
  }

  public async setWebhook(data: typeof this.webhook) {
    const find = await this.repository.webhook.findUnique({
      where: { instanceId: this.instance.id },
    });

    let update: typeof this.webhook;

    if (find) {
      update = await this.repository.updateWebhook(find.id, data);
    } else {
      update = await this.repository.webhook.create({
        data: {
          url: data.url,
          enabled: data.enabled,
          events: data?.events,
          instanceId: this.instance.id,
        },
        select: {
          id: true,
          url: true,
          enabled: true,
          events: true,
          instanceId: true,
        },
      });
    }
    Object.assign(this.webhook, update);

    return update;
  }

  public async findWebhook() {
    return await this.repository.webhook.findUnique({
      where: { instanceId: this.instance.id },
    });
  }

  private async sendDataWebhook<T = any>(event: WebhookEventsType, data: T) {
    const eventDesc = WebhookEventsEnum[event];

    try {
      if (this.webhook?.enabled) {
        if (this.webhook?.events && this.webhook?.events[event]) {
          await axios.post(
            this.webhook.url,
            {
              event: eventDesc,
              instance: this.instance,
              data,
            },
            { headers: { 'Resource-Owner': this.instance.ownerJid } },
          );
        }
        if (!this.webhook?.events) {
          await axios.post(
            this.webhook.url,
            {
              event: eventDesc,
              instance: this.instance,
              data,
            },
            { headers: { 'Resource-Owner': this.instance.ownerJid } },
          );
        }
      }
    } catch (error) {
      const axiosError = error as AxiosError;
      this.logger.error('sendDataWebhook-local', {
        message: axiosError?.message,
        hostName: error?.hostname,
        code: axiosError?.code,
        headers: JSON.stringify(axiosError?.request?.headers || {}),
        data: JSON.stringify(axiosError?.response?.data || {}),
        stack: error?.stack,
        name: error?.name,
      });
    }

    try {
      const globalWebhook = this.configService.get<GlobalWebhook>('GLOBAL_WEBHOOK');
      if (globalWebhook?.ENABLED && isURL(globalWebhook.URL)) {
        await axios.post(
          globalWebhook.URL,
          {
            event: eventDesc,
            instance: this.instance,
            data,
          },
          { headers: { 'Resource-owner': this.instance.ownerJid } },
        );
      }
    } catch (error) {
      const axiosError = error as AxiosError;
      this.logger.error('sendDataWebhook-global', {
        message: axiosError?.message,
        hostName: error?.hostname,
        code: axiosError?.code,
        headers: JSON.stringify(axiosError?.request?.headers || {}),
        data: JSON.stringify(axiosError?.response?.data || {}),
        stack: error?.stack,
        name: error?.name,
      });
    }

    data = undefined;
  }

  private async connectionUpdate({
    qr,
    connection,
    lastDisconnect,
  }: Partial<ConnectionState>) {
    if (qr) {
      if (this.qrCode.count === this.configService.get<QrCode>('QRCODE').LIMIT) {
        this.sendDataWebhook('qrcodeUpdated', {
          message: 'QR code limit reached, please login again',
          statusCode: DisconnectReason.badSession,
        });

        this.stateConnection.state = 'refused';
        this.stateConnection.statusReason = DisconnectReason.connectionClosed;

        this.sendDataWebhook('connectionUpdated', {
          instance: this.instance.name,
          ...this.stateConnection,
        });

        this.sendDataWebhook('statusInstance', {
          instance: this.instance.name,
          status: 'removed',
        });

        this.endSession = true;

        return this.eventEmitter.emit('no.connection', this.instance);
      }

      this.instanceQr.count++;

      const qrCodeOptions: QrCode = this.configService.get<QrCode>('QRCODE');
      const optsQrcode: QRCodeToDataURLOptions = {
        margin: 3,
        scale: 4,
        errorCorrectionLevel: 'H',
        color: { light: qrCodeOptions.LIGHT_COLOR, dark: qrCodeOptions.DARK_COLOR },
      };

      if (this.phoneNumber && !this.client?.authState?.creds?.registered) {
        this.instanceQr.paringCode = await this.client.requestPairingCode(
          this.phoneNumber,
        );
      }

      let toQrcode = qr;
      if (qr.startsWith('https://wa.me/settings/linked_devices#')) {
        const values = qr.split('#');
        toQrcode = values[1];
      }

      qrcode.toDataURL(toQrcode, optsQrcode, (error, base64) => {
        if (error) {
          this.logger.error('Qrcode generate failed:' + error.toString());
          return;
        }

        this.instanceQr.code = qr;
        this.instanceQr.base64 = base64;

        this.ws.send(this.instance.name, 'qrcode.updated', { ...this.instanceQr });

        this.sendDataWebhook('qrcodeUpdated', {
          qrcode: { instance: this.instance.name, ...this.instanceQr },
        });

        this.eventEmitter.emit('qrcode.updated', { ...this.instanceQr });
      });

      if (process.env.NODE_ENV === 'development') {
        qrcodeTerminal.generate(qr, { small: true }, (display) => {
          this.logger.info(`qrcode[${this.instanceName}]`, {
            count: this.instanceQr.count,
            paringCode: this.instanceQr?.paringCode,
          });
          console.log(display);
        });
      }
    }

    if (connection) {
      this.stateConnection.state = connection;
      this.stateConnection.statusReason =
        (lastDisconnect?.error as Boom)?.output?.statusCode ?? 200;

      const data = {
        instance: this.instance.name,
        ...this.stateConnection,
      };
      this.ws.send(this.instance.name, 'connection.update', data);
      this.sendDataWebhook('connectionUpdated', data);
    }

    if (connection === 'close') {
      const shouldReconnect = (lastDisconnect.error as Boom)?.output?.statusCode !== 401;
      if (shouldReconnect) {
        await this.connectToWhatsapp();
      } else {
        this.sendDataWebhook('statusInstance', {
          instance: this.instance.name,
          status: 'removed',
        });
        this.eventEmitter.emit('remove.instance', this.instance, 'inner');
        this.client?.ws?.close();
        this.client.end(new Error('Close connection'));
      }
    }

    if (connection === 'open') {
      this.instance.ownerJid = this.client.user.id.replace(/:\d+/, '');
      this.instance.profilePicUrl = (
        await this.profilePicture(this.instance.ownerJid)
      ).profilePictureUrl;
      this.instance.connectionStatus = 'ONLINE';

      this.repository.instance
        .update({
          where: { id: this.instance.id },
          data: {
            ownerJid: this.instance.ownerJid,
            profilePicUrl: this.instance.profilePicUrl,
            connectionStatus: this.instance.connectionStatus,
          },
        })
        .catch((err) => this.logger.error(err));

      // Subscribe to live updates for all known newsletter channels.
      // This causes WhatsApp to push recent activity for each channel,
      // which flows through upsertMessage → processMessage → chats.update
      // → our handler saves/enriches them in DB automatically.
      this.repository.chat
        .findMany({
          where: { instanceId: this.instance.id, remoteJid: { contains: '@newsletter' } },
          select: { remoteJid: true },
          distinct: ['remoteJid'],
        })
        .then(async (chats) => {
          for (const { remoteJid } of chats) {
            try {
              await this.client.subscribeNewsletterUpdates(remoteJid);
            } catch { /* best-effort */ }
          }
        })
        .catch(() => {});

      this.instanceQr.base64 = undefined;
      this.instanceQr.code = undefined;

      this.logger.info('instance started');
    }
  }

  private async getMessage(key: PrismType.Message, full = false) {
    try {
      key.instanceId = this.instance.id;
      const message = await this.repository.message.findFirst({
        where: key as any,
      });
      const webMessageInfo: Partial<proto.WebMessageInfo> = {
        key: {
          id: message.keyId,
          remoteJid: message.keyRemoteJid,
          fromMe: message.keyFromMe,
        },
        message: {
          [message.messageType]: message.content,
        },
      };
      if (full) {
        return webMessageInfo;
      }
      return webMessageInfo.message;
    } catch (error) {
      return { conversation: '' };
    }
  }

  private async defineAuthState() {
    if (this.configService.get<ProviderSession>('PROVIDER')?.ENABLED) {
      return await this.authStateProvider.authStateProvider(this.instance.name);
    }

    // Store session in PostgreSQL so it survives Docker restarts/crashes.
    // Falls back to file system only when instance.id is unavailable (should not happen in practice).
    if (this.instance?.id) {
      return await useDatabaseAuthState(this.repository, this.instance.id);
    }

    return await useMultiFileAuthState(join(INSTANCE_DIR, this.instance.name));
  }

  private async setSocket() {
    this.endSession = false;

    this.authState = await this.defineAuthState();

    const { version } = fetchLatestBaileysVersionV2();
    const session = this.configService.get<ConfigSessionPhone>('CONFIG_SESSION_PHONE');
    const browser: WABrowserDescription = !this.phoneNumber
      ? [session.CLIENT, session.NAME, release()]
      : Browsers.macOS('Chrome');

    let { EXPIRATION_TIME } = this.configService.get<QrCode>('QRCODE');
    const CONNECTION_TIMEOUT = this.configService.get<number>('CONNECTION_TIMEOUT');

    if (this.phoneNumber) {
      EXPIRATION_TIME = CONNECTION_TIMEOUT;
    }

    const proxy = this.configService.get<EnvProxy>('PROXY');
    const agents = createProxyAgents(proxy?.WS, proxy?.FETCH);

    const log = this.logger.log.child({ context: 'baileys' });
    log.level = this.configService.get<LogLevel>('BAILEYS_LOG_LEVEL');

    const socketConfig: UserFacingSocketConfig = {
      auth: {
        creds: this.authState.state.creds,
        keys: makeCacheableSignalKeyStore(
          this.authState.state.keys,
          log.child({ 'auth-creds': true }),
        ),
      },
      agent: agents?.wsAgent,
      fetchAgent: agents?.fetchAgent,
      logger: log,
      browser,
      version,
      connectTimeoutMs: CONNECTION_TIMEOUT * 1000,
      qrTimeout: EXPIRATION_TIME * 1000,
      emitOwnEvents: true,
      msgRetryCounterCache: this.msgRetryCounterCache,
      retryRequestDelayMs: 5 * 1000,
      maxMsgRetryCount: 1000,
      getMessage: this.getMessage as any,
      generateHighQualityLinkPreview: true,
      syncFullHistory: false,
      userDevicesCache: this.userDevicesCache,
      transactionOpts: { maxCommitRetries: 5, delayBetweenTriesMs: 50 },
    };

    return makeWASocket(socketConfig);
  }

  public async reloadConnection(): Promise<WASocket> {
    try {
      await new Promise((resolve) => {
        this.client.ws?.once('close', resolve);
        this.client.ws?.['socket']?.['terminate']?.();
      });
      this.client.ws['socket'] = null;
      await this.client.ws.connect();

      return this.client;
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  public async connectToWhatsapp(): Promise<WASocket> {
    try {
      this.instanceQr.count = 0;
      await this.loadWebhook();
      this.client = await this.setSocket();
      this.eventHandler();

      return this.client;
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  private readonly chatHandle = {
    'chats.upsert': async (chats: Chat[]) => {
      for (const chat of chats) {
        try {
          let item: any = { ...chat };
          delete item.id;

          const isNewsletter = isJidNewsletter(chat.id);

          // Always process newsletter chats regardless of CHATS flag
          if (isNewsletter) {
            try {
              const meta = await this.client.newsletterMetadata('jid', chat.id);
              if (meta) item = meta;
            } catch { /* best-effort */ }
          } else if (!this.databaseOptions.DB_OPTIONS.CHATS) {
            continue;
          }

          const list: PrismType.Chat[] = [];
          const find = await this.repository.chat.findFirst({
            where: {
              remoteJid: chat.id,
              instanceId: this.instance.id,
            },
          });
          if (!find) {
            const create = await this.repository.chat.create({
              data: {
                remoteJid: chat.id,
                content: item as any,
                instanceId: this.instance.id,
              },
            });
            list.push(create);
          } else {
            const update = await this.repository.chat.update({
              where: {
                id: find.id,
              },
              data: {
                content: item as any,
                updatedAt: new Date(),
              },
            });
            list.push(update);
          }
          this.ws.send(this.instance.name, 'chats.upsert', list);

          await this.sendDataWebhook('chatsUpsert', list);
        } catch (error) {
          this.logger.error(error);
        }
      }
    },

    'chats.update': async (
      chats: Partial<
        proto.IConversation & {
          lastMessageRecvTimestamp?: number;
        } & {
          conditional: (bufferedData: BufferedEventData) => boolean;
        }
      >[],
    ) => {
      if (!this.databaseOptions.DB_OPTIONS.CHATS) return;

      const chatsRaw: PrismType.Chat[] = chats.map((chat) => {
        const item = { ...chat };
        delete item.id;
        return {
          remoteJid: chat.id,
          instanceId: this.instance.id,
          content: item,
        } as PrismType.Chat;
      });
      this.ws.send(this.instance.name, 'chats.update', chatsRaw);
      await this.sendDataWebhook('chatsUpdated', chatsRaw);
      chatsRaw.forEach((chat) => {
        this.repository.chat
          .findFirst({
            where: {
              instanceId: this.instance.id,
              remoteJid: chat.remoteJid,
            },
          })
          .then((result) => {
            if (result?.id) {
              this.repository.chat
                .update({
                  where: {
                    id: result.id,
                  },
                  data: {
                    content: chat.content,
                    updatedAt: new Date(),
                  },
                })
                .catch(null);
            } else {
              this.repository.chat
                .create({
                  data: {
                    remoteJid: chat.remoteJid,
                    content: chat.content,
                    updatedAt: new Date(),
                    instanceId: this.instance.id,
                  },
                })
                .catch((err) => this.logger.error(err));
            }
          })
          .catch((err) => this.logger.error(err));
      });
    },

    'chats.delete': async (chats: string[]) => {
      await this.sendDataWebhook('chatsDeleted', [...chats]);
      for (const chat of chats) {
        const c = await this.repository.chat.findFirst({
          where: {
            remoteJid: chat,
            instanceId: this.instance.id,
          },
        });
        if (c) {
          await this.repository.chat.delete({
            where: {
              id: c.id,
            },
          });
        }
      }
    },
  };

  private readonly contactHandle = {
    'contacts.upsert': async (contacts: Contact[]) => {
      if (!this.databaseOptions.DB_OPTIONS.CONTACTS) return;

      for (const contact of contacts) {
        const list: PrismType.Contact[] = [];
        try {
          const find = await this.repository.contact.findFirst({
            where: { remoteJid: contact.id, instanceId: this.instance.id },
          });
          if (!find) {
            const create = await this.repository.contact.create({
              data: {
                remoteJid: contact.id,
                pushName: contact?.name || contact.id,
                profilePicUrl: null,
                instanceId: this.instance.id,
              },
            });
            list.push(create);
          } else {
            list.push(find);
          }
          this.ws.send(this.instance.name, 'contacts.upsert', list);
          await this.sendDataWebhook('contactsUpsert', list);
        } catch (error) {
          this.logger.error(error);
        }
      }
    },

    'contacts.update': async (contacts: Partial<Contact>[]) => {
      if (!this.databaseOptions.DB_OPTIONS.CONTACTS) return;

      for (const contact of contacts) {
        const list: PrismType.Contact[] = [];
        try {
          const find = await this.repository.contact.findFirst({
            where: { remoteJid: contact.id, instanceId: this.instance.id },
          });
          if (!find) {
            const create = await this.repository.contact.create({
              data: {
                remoteJid: contact.id,
                pushName: contact?.name || contact.id,
                profilePicUrl: null,
                instanceId: this.instance.id,
              },
            });
            list.push(create);
          } else {
            list.push(find);
          }
          this.ws.send(this.instance.name, 'contacts.upsert', list);
          await this.sendDataWebhook('contactsUpsert', list);
        } catch (error) {
          this.logger.error(error);
        }
      }
    },
  };

  private async syncMessage(messages: PrismType.Message[]) {
    if (messages.length === 0) return;

    const keyIds = messages.map((m) => m.keyId);
    const existing = await this.repository.message.findMany({
      where: { instanceId: this.instance.id, keyId: { in: keyIds } },
      select: { keyId: true },
    });
    const existingSet = new Set(existing.map((m) => m.keyId));
    const toCreate = messages.filter((m) => !existingSet.has(m.keyId));
    if (toCreate.length > 0) {
      await this.repository.message.createMany({ data: toCreate });
    }
  }

  private async enrichNewsletterChats(jids: string[]) {
    for (const jid of jids) {
      try {
        const meta = await this.client.newsletterMetadata('jid', jid);
        if (meta) {
          const existing = await this.repository.chat.findFirst({
            where: { instanceId: this.instance.id, remoteJid: jid },
          });
          if (existing) {
            await this.repository.chat.update({
              where: { id: existing.id },
              data: { content: meta as any },
            });
          } else {
            await this.repository.chat.create({
              data: { remoteJid: jid, instanceId: this.instance.id, content: meta as any },
            });
          }
        }
      } catch { /* best-effort */ }
    }
  }

  private readonly messageHandle = {
    'messaging-history.set': async ({
      messages,
      chats,
      isLatest,
    }: BaileysEventMap['messaging-history.set']) => {
      if (chats && chats.length > 0) {
        const chatsRaw: PrismType.Chat[] = chats.map((chat) => {
          const { id, ...item } = chat as any;
          return {
            remoteJid: id,
            instanceId: this.instance.id,
            content: item,
          } as PrismType.Chat;
        });
        await this.sendDataWebhook('chatsSet', chatsRaw);
        await this.repository.chat.createMany({ data: chatsRaw, skipDuplicates: true });

        // Enrich newsletter chats from this chunk
        const chunkNewsletterJids = chatsRaw
          .map((c) => c.remoteJid)
          .filter((jid) => jid?.includes('@newsletter'));
        if (chunkNewsletterJids.length) {
          this.enrichNewsletterChats(chunkNewsletterJids).catch(() => {});
        }
      }

      // On final sync chunk, re-enrich ALL newsletter chats in DB to catch any stale content
      if (isLatest) {
        const allNewsletterChats = await this.repository.chat.findMany({
          where: { instanceId: this.instance.id, remoteJid: { contains: '@newsletter' } },
          select: { remoteJid: true },
        });
        const allJids = allNewsletterChats.map((c) => c.remoteJid);
        if (allJids.length) {
          this.enrichNewsletterChats(allJids).catch(() => {});
        }
      }

      if (messages && messages?.length > 0) {
        const messagesRaw: PrismType.Message[] = [];
        for (const [, m] of Object.entries(messages)) {
          if (
            m.message?.protocolMessage ||
            m.message?.senderKeyDistributionMessage ||
            !m.message
          ) {
            continue;
          }

          let timestamp = m?.messageTimestamp;

          if (
            timestamp &&
            typeof timestamp === 'object' &&
            typeof timestamp.toNumber === 'function'
          ) {
            timestamp = timestamp.toNumber();
          } else if (
            timestamp &&
            typeof timestamp === 'object' &&
            'low' in timestamp &&
            'high' in timestamp
          ) {
            timestamp = Number(timestamp.low) || 0;
          } else if (typeof timestamp !== 'number') {
            timestamp = 0;
          }

          const messageType = getContentType(m.message);

          if (!messageType) {
            continue;
          }

          const user = getJidUser(m.key);
          const group = getUserGroup(m.key, m?.participant);

          messagesRaw.push({
            keyId: m.key.id,
            keyFromMe: m.key.fromMe,
            pushName: m?.pushName || m.key.remoteJid.split('@')[0],
            keyRemoteJid: user?.jid,
            keyLid: user?.lid,
            keyParticipant: group?.jid,
            keyParticipantLid: group?.lid,
            messageType,
            content: m.message[messageType] as PrismType.Prisma.JsonValue,
            messageTimestamp: timestamp,
            instanceId: this.instance.id,
            device: getDevice(m.key.id),
          } as PrismType.Message);
        }

        this.sendDataWebhook('messagesSet', messagesRaw).catch(() => null);

        if (this.databaseOptions.DB_OPTIONS.SYNC_MESSAGES) {
          await this.syncMessage(messagesRaw);
        }

        messages = undefined;
        messagesRaw.length = 0;
      }
    },

    'messages.upsert': async ({
      messages,
      type,
    }: {
      messages: WAMessage[];
      type: MessageUpsertType;
    }) => {
      for (const received of messages) {
        if (!received?.message) {
          await this.client.waitForMessage(received.key.id);
          continue;
        }

        await this.client.sendPresenceUpdate('unavailable');

        let timestamp = received?.messageTimestamp;

        if (
          timestamp &&
          typeof timestamp === 'object' &&
          typeof timestamp.toNumber === 'function'
        ) {
          timestamp = timestamp.toNumber();
        } else if (
          timestamp &&
          typeof timestamp === 'object' &&
          'low' in timestamp &&
          'high' in timestamp
        ) {
          timestamp = Number(timestamp.low) || 0;
        } else if (typeof timestamp !== 'number') {
          timestamp = 0;
        }

        const messageType = getContentType(received.message);
        if (!messageType) {
          return;
        }

        if (typeof received.message[messageType] === 'string') {
          received.message[messageType] = {
            text: received.message[messageType],
          } as any;
        }

        if (received.message?.protocolMessage) {
          const m = received.message.protocolMessage;
          if (typeof m?.type === 'number') {
            const typeName =
              proto.Message.ProtocolMessage.Type[m.type as any] ?? 'UNKNOWN_TYPE';
            m.type = typeName as any;
            received.message.protocolMessage = m;
          }
        }

        const user = getJidUser(received.key);
        const group = getUserGroup(received.key, received?.participant);

        const messageRaw = {
          keyId: received.key.id,
          keyFromMe: received.key.fromMe,
          pushName: received.pushName,
          keyRemoteJid: user?.jid,
          keyLid: user?.lid,
          keyParticipant: group?.jid,
          keyParticipantLid: group?.lid,
          messageType,
          content: JSON.parse(
            JSON.stringify(received.message[messageType]),
          ) as PrismType.Prisma.JsonValue,
          messageTimestamp: timestamp,
          instanceId: this.instance.id,
          device: (() => {
            if (isValidUlid(received.key.id)) {
              return 'web';
            }
            return getDevice(received.key.id);
          })(),
          isGroup: isJidGroup(received.key.remoteJid),
        } as PrismType.Message;

        if (this.databaseOptions.DB_OPTIONS.NEW_MESSAGE) {
          const { id } = await this.repository.message.create({ data: messageRaw });
          messageRaw.id = id;
        }

        if (type === 'append') {
          const find = await this.repository.message.findFirst({
            where: {
              keyId: messageRaw.keyId,
              instanceId: messageRaw.instanceId,
            },
          });

          if (find?.id) {
            messageRaw.id = find.id;
          }
        }

        messageRaw['info'] = { type };

        if (s3Service.BUCKET?.ENABLE) {
          try {
            const media = await this.getMediaMessage(messageRaw, true);
            if (media) {
              const { stream, mediaType, fileName, size } = media;
              const { id, name } = this.instance;
              const mimetype = mime.lookup(fileName).toString();
              const fullName = join(
                `${id}_${name}`,
                messageRaw.keyRemoteJid,
                mediaType,
                fileName,
              );
              await s3Service.uploadFile(fullName, stream, size.fileLength, {
                'Content-Type': mimetype,
                'custom-header-fromMe': String(!!received.key?.fromMe),
                'custom-header-keyRemoteJid': received.key.remoteJid,
                'custom-header-pushName': received?.pushName,
                'custom-header-mediaType': mediaType,
                'custom-header-messageId': messageRaw.keyId,
              });

              const created = await this.repository.media.create({
                data: {
                  messageId: messageRaw.id,
                  type: mediaType,
                  fileName: fullName,
                  mimetype,
                },
              });
              messageRaw.content['mediaUrl'] = await getObjectUrl(created.fileName);
            }
          } catch (error) {
            this.logger.error(error, { desc: 'Error on upload file to s3' });
          }
        }

        const media = await this.isMediaMessage(messageRaw);
        if (media?.mediaMessage) {
          const q = encodeProps({
            mediaKey: messageRaw.content['mediaKey'],
            directPath: messageRaw.content['directPath'],
            url: messageRaw.content['url'],
            mimetype: messageRaw.content['mimetype'],
          });

          /**
           * Add this path to your application's URL for media download
           */
          messageRaw['media'] = {
            path: `/chat/public/${messageType.replace('Message', '')}/download?q=${q}`,
          };
        }

        this.ws.send(this.instance.name, 'messages.upsert', messageRaw);
        await this.sendDataWebhook('messagesUpsert', messageRaw);

        this.logger.trace(`type[${type}] - received`, messageRaw);
      }
    },

    'messages.update': async (args: WAMessageUpdate[]) => {
      const status = {
        0: 'ERROR',
        1: 'PENDING',
        2: 'SERVER_ACK',
        3: 'DELIVERY_ACK',
        4: 'READ',
        5: 'PLAYED',
      };
      for await (const { key, update } of args) {
        if (update.status === proto.WebMessageInfo.Status.READ && key?.remoteJid) {
          key.remoteJid = key.remoteJid.replace(/:\d+(?=@)/, '');
        }

        if (key.remoteJid !== 'status@broadcast' && !key?.remoteJid?.match(/(:\d+)/)) {
          const message = {
            ...key,
            status: status[update.status],
            dateTime: new Date(),
            instanceId: this.instance.id,
          };

          this.ws.send(this.instance.name, 'messages.update', message);

          await this.sendDataWebhook('messagesUpdated', message);
          if (this.databaseOptions.DB_OPTIONS.MESSAGE_UPDATE) {
            this.repository.message
              .findFirst({
                where: {
                  instanceId: this.instance.id,
                  keyId: key.id,
                },
              })
              .then(async (result) => {
                if (result) {
                  await this.repository.messageUpdate.create({
                    data: {
                      messageId: result.id,
                      status: status[update?.status || 3],
                      dateTime: new Date(),
                    },
                  });
                }
              })
              .catch((error) => this.logger.error(error));
          }
        }
      }
    },
  };

  private readonly groupHandler = {
    'groups.upsert': (groupMetadata: GroupMetadata[]) => {
      this.ws.send(this.instance.name, 'groups.upsert', groupMetadata);
      this.sendDataWebhook('groupsUpsert', groupMetadata);
    },

    'groups.update': (groupMetadataUpdate: Partial<GroupMetadata>[]) => {
      this.ws.send(this.instance.name, 'groups.update', groupMetadataUpdate);
      this.sendDataWebhook('groupsUpdated', groupMetadataUpdate);
    },

    'group-participants.update': (participantsUpdate: {
      id: string;
      author: string;
      authorPn?: string;
      participants: GroupParticipant[];
      action: ParticipantAction;
    }) => {
      this.ws.send(this.instance.name, 'group-participants.update', participantsUpdate);
      this.sendDataWebhook('groupsParticipantsUpdated', participantsUpdate);
    },
  };

  private readonly callHandler = {
    'call.upsert': (call: WACallEvent[]) => {
      call.forEach((c) => {
        this.ws.send(this.instance.name, 'call.upsert', c);
        this.sendDataWebhook('callUpsert', c);
      });
    },
  };

  private readonly onLabel = {
    'labels.association': async (args: BaileysEventMap['labels.association']) => {
      this.sendDataWebhook('labelsAssociation', args);
    },
    'labels.edit': async (args: BaileysEventMap['labels.edit']) => {
      this.sendDataWebhook('labelsEdit', args);
    },
  };

  private eventHandler() {
    // Baileys discards NotificationNewsletterJoin as "unhandled" without emitting any event.
    // We intercept the raw mex notification on the same ws event bus Baileys itself uses.
    //
    // CB: event format: `CB:${tag},${attrKey}:${attrVal},${firstChildTag}`
    // For mex notifications: tag=notification, attrs.type=mex → fires 'CB:notification,type:mex'
    //
    // The legacy mex handler (which handles Join/Leave) stores operation in JSON content body,
    // not in attrs.op_name. Payload node is either child 'mex' (with content) or child 'update'.
    this.client.ws.on('CB:notification,type:mex', async (node: any) => {
      try {
        // Mirror Baileys' handleLegacyMexNewsletterNotification node-parsing exactly
        const mexNode = getBinaryNodeChild(node, 'mex');
        const updateNode = mexNode?.content
          ? null
          : getBinaryNodeChild(node, 'update') ?? node;
        const payloadNode = mexNode?.content ? mexNode : updateNode;
        if (!payloadNode?.content || Array.isArray(payloadNode.content)) return;

        const contentBuf = Buffer.isBuffer(payloadNode.content)
          ? payloadNode.content
          : Buffer.from(payloadNode.content as any);
        const data = JSON.parse(contentBuf.toString());

        // operation is in data.operation or payloadNode.attrs.op_name (newer format)
        const operation: string = data?.operation ?? payloadNode?.attrs?.op_name ?? '';
        if (operation !== 'NotificationNewsletterJoin') return;

        const updates: { jid?: string }[] = data?.updates ?? [];
        for (const update of updates) {
          const jid = update?.jid;
          if (!jid || !isJidNewsletter(jid)) continue;

          const existing = await this.repository.chat.findFirst({
            where: { instanceId: this.instance.id, remoteJid: jid },
          });
          if (!existing) {
            let content: any = undefined;
            try {
              content = await this.client.newsletterMetadata('jid', jid);
            } catch { /* best-effort */ }
            await this.repository.chat.create({
              data: { remoteJid: jid, instanceId: this.instance.id, content: content ?? undefined },
            });
            this.logger.info(`newsletter joined/created from phone saved: ${jid}`);
          }
        }
      } catch (err) {
        this.logger.warn('newsletter-join-sync failed', err);
      }
    });

    this.client.ev.process(async (events) => {
      if (!this.endSession) {
        if (events?.['connection.update']) {
          this.connectionUpdate(events['connection.update']);
        }

        if (events?.['creds.update']) {
          await this.authState.saveCreds();
        }

        if (events?.['messaging-history.set']) {
          const payload = events['messaging-history.set'];
          this.messageHandle['messaging-history.set'](payload);
        }

        if (events?.['messages.upsert']) {
          const payload = events['messages.upsert'];
          this.messageHandle['messages.upsert'](payload);
        }

        if (events?.['messages.update']) {
          const payload = events['messages.update'];
          this.messageHandle['messages.update'](payload);
        }

        if (events?.['presence.update']) {
          const payload = events['presence.update'];
          this.ws.send(this.instance.name, 'presence.update', payload);
          this.sendDataWebhook('presenceUpdated', payload);
        }

        if (events?.['groups.upsert']) {
          const payload = events['groups.upsert'];
          this.groupHandler['groups.upsert'](payload);
        }

        if (events?.['groups.update']) {
          const payload = events['groups.update'];
          this.groupHandler['groups.update'](payload);
        }

        if (events?.['group-participants.update']) {
          const payload = events['group-participants.update'];
          this.groupHandler['group-participants.update'](payload as any);
        }

        if (events?.['chats.upsert']) {
          const payload = events['chats.upsert'];
          this.chatHandle['chats.upsert'](payload);
        }

        if (events?.['chats.update']) {
          const payload = events['chats.update'];
          this.chatHandle['chats.update'](payload);
        }

        if (events?.['chats.delete']) {
          const payload = events['chats.delete'];
          this.chatHandle['chats.delete'](payload);
        }

        if (events?.['contacts.upsert']) {
          const payload = events['contacts.upsert'];
          this.contactHandle['contacts.upsert'](payload);
        }

        if (events?.['contacts.update']) {
          const payload = events['contacts.update'];
          this.contactHandle['contacts.update'](payload);
        }

        if (events?.['call']) {
          const payload = events['call'];
          this.callHandler['call.upsert'](payload);
        }

        if (events?.['labels.association']) {
          const payload = events['labels.association'];
          this.onLabel['labels.association'](payload);
        }

        if (events?.['labels.edit']) {
          const payload = events['labels.edit'];
          this.onLabel['labels.edit'](payload);
        }
      }
    });
  }

  // Check if the number is MX or AR
  private formatMXOrARNumber(jid: string): string {
    const regexp = new RegExp(/^(\d{2})(\d{2})\d{1}(\d{8})$/);
    if (regexp.test(jid)) {
      const match = regexp.exec(jid);
      if (match && (match[1] === '52' || match[1] === '54')) {
        const joker = Number.parseInt(match[3][0]);
        const ddd = Number.parseInt(match[2]);
        if (joker < 7 || ddd < 11) {
          return match[0];
        }
        return match[1] === '52' ? '52' + match[3] : '54' + match[3];
      }
    }
    return jid;
  }

  // Check if the number is br
  private formatBRNumber(jid: string) {
    const regexp = new RegExp(/^(\d{2})(\d{2})\d{1}(\d{8})$/);
    if (regexp.test(jid)) {
      const match = regexp.exec(jid);
      if (match && match[1] === '55') {
        const joker = Number.parseInt(match[3][0]);
        const ddd = Number.parseInt(match[2]);
        if (joker < 7 || ddd < 31) {
          return match[0];
        }
        return match[1] + match[2] + match[3];
      }
    } else {
      return jid;
    }
  }

  private createJid(number: string): string {
    const regexp = new RegExp(/^\w+@(s.whatsapp.net|g.us|lid|broadcast|newsletter)$/i);
    if (regexp.test(number)) {
      return number;
    }

    const formattedBRNumber = this.formatBRNumber(number);
    if (formattedBRNumber !== number) {
      return `${formattedBRNumber}@s.whatsapp.net`;
    }

    const formattedMXARNumber = this.formatMXOrARNumber(number);
    if (formattedMXARNumber !== number) {
      return `${formattedMXARNumber}@s.whatsapp.net`;
    }

    if (number.includes('-')) {
      return `${number}@g.us`;
    }

    return `${number}@s.whatsapp.net`;
  }

  public async profilePicture(number: string) {
    const jid = this.createJid(number);
    try {
      return {
        wuid: jid,
        profilePictureUrl: await this.client.profilePictureUrl(jid, 'image'),
      };
    } catch (error) {
      return {
        wuid: jid,
        profilePictureUrl: null,
      };
    }
  }

  public async updatePresence(data: UpdatePresenceDto) {
    const jid = this.createJid(data.number);
    const isWA = (await this.whatsappNumber({ numbers: [jid] }))[0];
    if (!isWA.exists && !isJidGroup(isWA.jid)) {
      throw new BadRequestException(isWA);
    }

    const recipient = isJidGroup(jid) ? jid : isWA.jid;

    if (isJidGroup(recipient)) {
      try {
        await this.client.groupMetadata(recipient);
      } catch (error) {
        throw new BadRequestException('Group not found');
      }
    }

    await this.client.presenceSubscribe(recipient);
    await this.client.sendPresenceUpdate(data.presence, recipient);

    return { message: 'success' };
  }

  private async sendMessageWithTyping<T = proto.IMessage>(
    number: string,
    message: T,
    options?: Options,
  ) {
    let quoted: PrismType.Message = options?.quotedMessage;
    if (options?.quotedMessageId) {
      if (!this.databaseOptions?.DB_OPTIONS?.NEW_MESSAGE) {
        throw new BadRequestException(
          'The DATABASE_SAVE_DATA_NEW_MESSAGE environment variable is disabled',
        );
      }

      quoted = await this.repository.message.findUnique({
        where: { id: options.quotedMessageId },
      });

      if (!quoted) {
        throw new BadRequestException('Quoted message not found');
      }
    }

    const jid = this.createJid(number);
    const isWA = (await this.whatsappNumber({ numbers: [jid] }))[0];
    if (!isWA.exists && !isJidGroup(isWA.jid) && !isJidNewsletter(isWA.jid)) {
      throw new BadRequestException(isWA);
    }

    const recipient = isJidGroup(jid) ? jid : isLidUser(jid) ? jid : isJidNewsletter(jid) ? jid : isWA.jid;

    if (isJidGroup(recipient)) {
      try {
        await this.client.groupMetadata(recipient);
      } catch (error) {
        throw new BadRequestException('Group not found');
      }
    }

    try {
      if (options?.delay) {
        await this.client.presenceSubscribe(recipient);
        await this.client.sendPresenceUpdate(options?.presence ?? 'composing', jid);
        await delay(options.delay);
        await this.client.sendPresenceUpdate('paused', recipient);
      }

      const messageSent: Partial<PrismType.Message> = await (async () => {
        let q: WAMessage;
        if (quoted) {
          if (quoted.messageType === 'conversation') {
            quoted.messageType = 'extendedTextMessage';
          }

          q = {
            key: {
              id: quoted.keyId,
              fromMe: quoted.keyFromMe,
              remoteJid: quoted.keyRemoteJid,
            },
            message: {
              [quoted.messageType]: {
                contextInfo: {},
                ...(quoted.content as any),
              },
            },
            messageTimestamp: quoted.messageTimestamp,
          };

          q.message = proto.Message.decode(proto.Message.encode(q.message).finish());
        }

        let m: proto.IWebMessageInfo;

        const messageId = options?.messageId || ulid(Date.now());

        if (message?.['react'] || message?.['edit'] || message?.['text'] || message?.['poll'] || isJidNewsletter(recipient)) {
          // Newsletters: client.sendMessage internally calls prepareWAMessageMedia with the
          // newsletter JID, which uses the correct unencrypted upload path for newsletters.
          // relayMessage() skips this and sends the pre-built proto which WhatsApp rejects for media.
          m = await this.client.sendMessage(recipient, message as AnyMessageContent, {
            quoted: q,
            messageId,
          });
          if (isJidNewsletter(recipient)) {
            const imgMsg = m?.message?.['imageMessage'];
            this.logger.info(`[newsletter-media] sendMessage returned keyId=${m?.key?.id} msgType=${Object.keys(m?.message || {}).join(',')} imgUrl=${imgMsg?.url} imgDirectPath=${imgMsg?.directPath} thumbDirectPath=${imgMsg?.thumbnailDirectPath}`);
          }
        } else {
          m = generateWAMessageFromContent(recipient, message, {
            timestamp: new Date(),
            userJid: this.instance.ownerJid,
            messageId,
            quoted: q,
          });

          // Polls and events need a meta node in the wire stanza.
          // client.sendMessage adds these automatically; we must inject them manually
          // when building proto directly and calling relayMessage.
          const isPollProto =
            message?.['pollCreationMessage'] ||
            message?.['pollCreationMessageV2'] ||
            message?.['pollCreationMessageV3'];

          const additionalNodes = isPollProto
            ? [{ tag: 'meta' as const, attrs: { polltype: 'creation' }, content: undefined }]
            : message?.['eventMessage']
              ? [{ tag: 'meta' as const, attrs: { event_type: 'creation' }, content: undefined }]
              : undefined;

          const id = await this.client.relayMessage(recipient, m.message, {
            messageId,
            additionalNodes,
          });

          m.key = {
            id: id,
            remoteJid: jid,
            participant: isLidUser(jid) ? jid : undefined,
            fromMe: true,
          };

          for (const [key, value] of Object.entries(m)) {
            if (!value || (isArray(value) && value.length) === 0) {
              delete m[key];
            }
          }
        }

        let timestamp = m?.messageTimestamp;

        if (
          timestamp &&
          typeof timestamp === 'object' &&
          typeof timestamp.toNumber === 'function'
        ) {
          timestamp = timestamp.toNumber();
        } else if (
          timestamp &&
          typeof timestamp === 'object' &&
          'low' in timestamp &&
          'high' in timestamp
        ) {
          timestamp = Number(timestamp.low) || 0;
        } else if (typeof timestamp !== 'number') {
          timestamp = 0;
        }

        return {
          keyId: m.key.id,
          keyFromMe: m.key.fromMe,
          keyRemoteJid: m.key?.remoteJid || m.key?.['lid'],
          keyParticipant: m?.participant,
          pushName: m?.pushName,
          messageType: getContentType(m.message),
          content: JSON.parse(
            JSON.stringify(m.message[getContentType(m.message)]),
          ) as PrismType.Prisma.JsonValue,
          messageTimestamp: timestamp,
          instanceId: this.instance.id,
          device: 'web',
          isGroup: isJidGroup(m.key.remoteJid),
        };
      })();
      if (this.databaseOptions.DB_OPTIONS.NEW_MESSAGE) {
        const { id } = await this.repository.message.create({
          data: messageSent as PrismType.Message,
        });
        messageSent.id = id;
      }

      messageSent['externalAttributes'] = options?.externalAttributes;

      this.ws.send(this.instance.name, 'send.message', messageSent);
      this.ws.send(this.instance.name, 'messages.upsert', messageSent);

      this.sendDataWebhook('sendMessage', messageSent).catch((error) =>
        this.logger.error(error),
      );
      this.sendDataWebhook('messagesUpsert', messageSent).catch((error) =>
        this.logger.error(error),
      );

      return messageSent;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Instance Controller
  public getInstance() {
    const i: Partial<Instance> & { status: InstanceStateConnection } = {
      ...this.instance,
      status: this.stateConnection,
    };
    return i;
  }

  // Send Message Controller
  public async textMessage(data: SendTextDto) {
    let mentions: string[] | undefined;
    if (data.textMessage.mentionAll) {
      const groupId = this.createJid(data.number);
      const meta = await this.client.groupMetadata(groupId);
      // WhatsApp's mentionedJid requires @s.whatsapp.net format — use phoneNumber for LID groups
      mentions = meta.participants.map((p) => p.phoneNumber ?? p.id);
    } else {
      mentions = data.textMessage.mentions?.map((m) => this.createJid(m));
    }
    return await this.sendMessageWithTyping<AnyMessageContent>(
      data.number,
      { text: data.textMessage.text, mentions },
      data?.options,
    );
  }

  private async generateVideoThumbnailFromStream<T = string>(
    video: T,
    timeInSeconds = '0',
  ): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const thumbnailStream = new PassThrough();
      const chunks = [];

      let input: PassThrough | T = video;

      if (Buffer.isBuffer(video)) {
        input = new PassThrough();
        input.end(video);
      }

      ffmpeg(input as any)
        .inputOptions(['-ss', timeInSeconds])
        .outputOptions('-frames:v 1')
        .outputFormat('image2pipe')
        .on('start', () => {
          thumbnailStream.on('data', (chunk) => chunks.push(chunk));
        })
        .on('error', (err) => {
          reject(new Error(`Error generating thumbnail: ${err.message}`));
        })
        .on('end', () => {
          resolve(Buffer.concat(chunks));
        })
        .pipe(thumbnailStream, { end: true });
    });
  }

  private async convertAudioToWH(
    inputPath: string,
    format: { input?: string; to?: string } = { input: 'mp3', to: 'aac' },
  ) {
    return new Promise<Buffer>((resolve, reject) => {
      if (!existsSync(inputPath)) {
        reject(new Error(`Input file not found: ${inputPath}`));
        return;
      }

      try {
        accessSync(inputPath, constants.R_OK);
      } catch (error) {
        reject(new Error(`No read permissions for file: ${inputPath}`));
        return;
      }

      const chunks: Buffer[] = [];
      const audioStream = new PassThrough();
      const normalizedPath = normalize(inputPath);

      const inputFormat = ['mpga', 'bin'].includes(format?.input)
        ? 'mp3'
        : format.input === 'oga'
          ? 'ogg'
          : format.input;
      const audioCodec = format.to === 'ogg' ? 'libvorbis' : 'aac';
      const outputFormat = format.to === 'ogg' ? 'ogg' : 'adts';

      const command = ffmpeg(normalizedPath)
        .inputFormat(inputFormat)
        .audioCodec(audioCodec)
        .outputFormat(outputFormat);

      command
        .on('start', (commandLine) => {
          console.log('FFmpeg started with command:', commandLine);
          audioStream.on('data', (chunk) => chunks.push(chunk));
        })
        .on('error', (err, stdout, stderr) => {
          console.error('FFmpeg error:', err.message);
          console.error('FFmpeg stderr:', stderr);

          ffmpeg(normalizedPath)
            .inputFormat(inputFormat)
            .outputFormat('wav')
            .on('end', () => {
              console.log('Converted to WAV, retrying final conversion...');
              const intermediatePath = normalizedPath.replace(/\.[^/.]+$/, '.wav');
              const secondCommand = ffmpeg(intermediatePath)
                .audioCodec(audioCodec)
                .outputFormat(outputFormat);

              secondCommand
                .on('error', (err2, stdout2, stderr2) => {
                  console.error('Second FFmpeg error:', err2.message);
                  reject(
                    new Error(
                      `Final conversion failed: ${err2.message}\nFFmpeg stderr: ${stderr2}`,
                    ),
                  );
                })
                .on('end', () => {
                  console.log('Final conversion to target format successful');
                  resolve(Buffer.concat(chunks));
                })
                .pipe(audioStream, { end: true });
            })
            .on('error', (err1) =>
              reject(new Error(`WAV conversion failed: ${err1.message}`)),
            )
            .pipe(audioStream, { end: true });
        })
        .on('end', () => {
          console.log('FFmpeg processing finished');
          resolve(Buffer.concat(chunks));
        })
        .pipe(audioStream, { end: true });
    });
  }

  private async prepareMediaMessage(
    mediaMessage: MediaMessage & { mimetype?: string; convert?: boolean },
  ) {
    const uploadPath = join(ROOT_DIR, 'uploads');
    let fileName = join(uploadPath, mediaMessage?.fileName || '');

    try {
      let preview: Buffer;
      let media: Buffer;
      let mimetype = mediaMessage.mimetype;

      let ext = mediaMessage.extension;

      const isURL = /http(s?):\/\//.test(mediaMessage.media as string);

      if (isURL) {
        const response = await axios.get(mediaMessage.media as string, {
          responseType: 'arraybuffer',
        });

        mimetype = response.headers['content-type'] as string;
        if (!ext) {
          ext = mime.extension(mimetype) as string;
        }

        if (!mediaMessage?.fileName) {
          fileName = join(uploadPath, ulid() + '.' + ext);
        }

        writeFileSync(fileName, Buffer.from(response.data));

        if (mediaMessage.mediatype === 'image') {
          preview = response.data;
        }
      }

      if (mediaMessage.mediatype === 'video') {
        try {
          preview = await this.generateVideoThumbnailFromStream(fileName);
        } catch (error) {
          preview = readFileSync(join(ROOT_DIR, 'public', 'images', 'video-cover.png'));
        }
      }

      const isAccOrOgg = /aac|ogg/.test(mediaMessage?.mimetype || mimetype);
      if (mediaMessage.convert && isAccOrOgg) {
        if (['ogg', 'oga'].includes(ext)) {
          media = readFileSync(fileName);
        } else {
          media = await this.convertAudioToWH(fileName, {
            input: ext,
            to: 'ogg',
          });
        }
      }

      if (!media) {
        media = readFileSync(fileName);
      }

      const prepareMedia = await prepareWAMessageMedia(
        { [mediaMessage.mediatype]: media } as any,
        { upload: this.client.waUploadToServer },
      );

      const mediaType = mediaMessage.mediatype + 'Message';

      if (mediaMessage.mediatype === 'document' && !mediaMessage.fileName) {
        const regex = new RegExp(/.*\/(.+?)\./);
        const arrayMatch = regex.exec(mediaMessage.media as string);
        mediaMessage.fileName = arrayMatch[1];
      }

      if (mediaMessage?.fileName) {
        mimetype = mime.lookup(mediaMessage.fileName) as string;
        if (mimetype === 'application/mp4') {
          mimetype = 'video/mp4';
        }
      }

      prepareMedia[mediaType].caption = mediaMessage?.caption;
      prepareMedia[mediaType].mimetype = mediaMessage?.mimetype || mimetype;
      prepareMedia[mediaType].fileName = mediaMessage.fileName;

      if (isAccOrOgg) {
        prepareMedia.audioMessage.ptt = true;
      }

      if (mediaMessage.mediatype === 'video') {
        prepareMedia[mediaType].jpegThumbnail = preview;
        prepareMedia[mediaType].gifPlayback = false;
      }

      if (mediaMessage.mediatype === 'image') {
        const p = await sharp(preview || media)
          .resize(320, 240, { fit: 'contain' })
          .toFormat('jpeg', { quality: 80 })
          .toBuffer();

        prepareMedia.imageMessage.jpegThumbnail = p;
      }

      return generateWAMessageFromContent(
        '',
        { [mediaType]: { ...prepareMedia[mediaType] } },
        { userJid: this.instance.ownerJid },
      );
    } catch (error) {
      const axiosError = error as AxiosError;
      if (axiosError?.isAxiosError) {
        this.logger.error(axiosError?.message);
        const err = Buffer.from(axiosError?.response?.data as any).toString('utf-8');
        throw new BadRequestException(axiosError?.message, err);
      }

      this.logger.error(error);

      throw new InternalServerErrorException(error?.toString() || error);
    } finally {
      if (existsSync(fileName)) {
        unlinkSync(fileName);
      }
    }
  }

  public async mediaMessage(data: SendMediaDto) {
    if (data.mediaMessage?.fileName) {
      data.mediaMessage.extension = data.mediaMessage.fileName.split('.').pop();
    }

    const jid = this.createJid(data.number);

    if (isJidNewsletter(jid)) {
      // For newsletters, pass raw media directly so client.sendMessage uses the
      // newsletter-specific unencrypted upload path in prepareWAMessageMedia.
      // prepareMediaMessage uses encrypted upload which newsletters reject.
      // Caller (BE) is responsible for sending JPEG images — WhatsApp newsletter only renders JPEG.
      // We supply jpegThumbnail since Baileys' newsletter branch skips thumbnail generation.
      const { mediatype, media, caption, fileName } = data.mediaMessage;
      let mediaContent: any = typeof media === 'string' ? { url: media } : media;
      let mimetype: string | undefined;
      let jpegThumbnail: Buffer | undefined;
      let width: number | undefined;
      let height: number | undefined;
      let thumbnailDirectPath: string | undefined;
      let thumbnailSha256: Buffer | undefined;

      if (mediatype === 'image') {
        try {
          const isURL = typeof media === 'string' && /^https?:\/\//.test(media);
          const srcBuffer = isURL
            ? Buffer.from((await axios.get(media as string, { responseType: 'arraybuffer' })).data)
            : (media as Buffer);

          // Convert main image to JPEG buffer — ensures uploaded bytes, fileSha256,
          // fileLength and mimetype all agree. Passing { url } risks a PNG/JPEG mismatch.
          const jpegBuffer = await sharp(srcBuffer).toFormat('jpeg', { quality: 100 }).toBuffer();
          const meta = await sharp(jpegBuffer).metadata();
          width = meta.width;
          height = meta.height;
          mimetype = 'image/jpeg';
          mediaContent = jpegBuffer;

          jpegThumbnail = await sharp(jpegBuffer)
            .resize(320, 240, { fit: 'contain' })
            .toFormat('jpeg', { quality: 80 })
            .toBuffer();

          // Upload thumbnail to WA CDN — native newsletter requires thumbnailDirectPath
          const thumbPath = join(tmpdir(), `thumb-${ulid(Date.now())}.jpg`);
          writeFileSync(thumbPath, jpegThumbnail);
          const thumbSha256 = createHash('sha256').update(jpegThumbnail).digest();
          const thumbSha256B64 = encodeURIComponent(thumbSha256.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''));
          try {
            const thumbUpload = await (this.client as any).waUploadToServer(thumbPath, {
              mediaType: 'image',
              fileEncSha256B64: thumbSha256B64,
            });
            thumbnailDirectPath = thumbUpload?.directPath;
            thumbnailSha256 = thumbSha256;
          } catch { /* best-effort */ }
          try { unlinkSync(thumbPath); } catch { /* ignore */ }
        } catch { /* fall back to original if anything fails */ }
      }

      const content = { [mediatype]: mediaContent, caption, fileName, mimetype, jpegThumbnail, width, height, thumbnailDirectPath, thumbnailSha256 } as any;
      this.logger.info(`[newsletter-media] sending ${mediatype} to ${jid} url=${typeof mediaContent === 'object' && !Buffer.isBuffer(mediaContent) && (mediaContent as any).url ? (mediaContent as any).url : 'buffer'}`);
      const result = await this.sendMessageWithTyping(data.number, content, data?.options);
      this.logger.info(`[newsletter-media] sent keyId=${result?.keyId} messageType=${result?.messageType}`);
      return result;
    }

    const generate = await this.prepareMediaMessage(data.mediaMessage);

    return await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      data?.options,
    );
  }

  public async mediaFileMessage(data: MediaFileDto, fileName: string) {
    const ext = fileName.split('.').pop();
    const generate = await this.prepareMediaMessage({
      fileName: fileName,
      media: fileName,
      mediatype: data.mediatype,
      caption: data?.caption,
      extension: ext,
    });

    return await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        presence: isNotEmpty(data?.presence) ? data.presence : undefined,
        delay: data?.options?.delay,
        quotedMessage: data?.options?.quotedMessage,
        quotedMessageId: data?.options?.quotedMessageId,
      },
    );
  }

  public async audioWhatsapp(data: SendAudioDto) {
    const generate = await this.prepareMediaMessage({
      media: data.audioMessage.audio,
      mimetype: 'audio/aac',
      mediatype: 'audio',
      convert: data?.options?.convertAudio,
    });

    return this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        presence: 'recording',
        delay: data?.options?.delay,
        quotedMessage: data?.options?.quotedMessage,
        quotedMessageId: data?.options?.quotedMessageId,
      },
    );
  }

  public async audioWhatsAppFile(data: AudioMessageFileDto, fileName: string) {
    const ext = fileName.split('.').pop();
    const generate = await this.prepareMediaMessage({
      fileName: fileName,
      media: fileName,
      mediatype: 'audio',
      mimetype: 'audio/aac',
      convert: data?.convertAudio as boolean,
      extension: ext,
    });

    return this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        presence: 'recording',
        delay: data?.options?.delay,
        quotedMessage: data?.options?.quotedMessage,
        quotedMessageId: data?.options?.quotedMessageId,
      },
    );
  }

  public async locationMessage(data: SendLocationDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        locationMessage: {
          degreesLatitude: data.locationMessage.latitude,
          degreesLongitude: data.locationMessage.longitude,
          name: data.locationMessage?.name,
          address: data.locationMessage?.address,
        },
      },
      data?.options,
    );
  }

  public async contactMessage(data: SendContactDto) {
    const message: proto.IMessage = {};

    const vcard = (contact: ContactMessage) => {
      return (
        'BEGIN:VCARD\n' +
        'VERSION:3.0\n' +
        'FN:' +
        contact.fullName +
        '\n' +
        'item1.TEL;waid=' +
        contact.wuid +
        ':' +
        contact.phoneNumber +
        '\n' +
        'item1.X-ABLabel:Celular\n' +
        'END:VCARD'
      );
    };

    if (data.contactMessage.length === 1) {
      message.contactMessage = {
        displayName: data.contactMessage[0].fullName,
        vcard: vcard(data.contactMessage[0]),
      };
    } else {
      message.contactsArrayMessage = {
        displayName: `${data.contactMessage.length} contacts`,
        contacts: data.contactMessage.map((contact) => {
          return {
            displayName: contact.fullName,
            vcard: vcard(contact),
          };
        }),
      };
    }

    return await this.sendMessageWithTyping(data.number, { ...message }, data?.options);
  }

  public async fetchAllGroups() {
    try {
      const groups = await this.client.groupFetchAllParticipating();
      const groupList = Object.values(groups).filter((g) => g?.id);

      const contacts = await this.repository.contact.findMany({
        where: { instanceId: this.instance.id },
        select: { remoteJid: true, pushName: true },
      });
      const nameMap = new Map(contacts.map((c) => [c.remoteJid, c.pushName]));

      return groupList.map((g) => ({
        ...g,
        participants: g.participants.map((p) => {
          const jid = (p as any).phoneNumber ?? p.id;
          const inMemory = (this.client as any).contacts?.[jid];
          return {
            ...p,
            name: nameMap.get(jid) ?? inMemory?.name ?? inMemory?.notify ?? null,
          };
        }),
      }));
    } catch (error) {
      // groupFetchAllParticipating can fail when a deleted group remains in the Baileys
      // socket's in-memory state. Return empty list instead of 500 so the caller is not
      // blocked — the stale state clears itself on the next reconnect.
      this.logger.warn(
        'fetchAllGroups: groupFetchAllParticipating failed (stale deleted group in socket state?): ' +
          error.toString(),
      );
      return [];
    }
  }

  public async reactionMessage(data: SendReactionDto) {
    return await this.sendMessageWithTyping<AnyMessageContent>(
      data.reactionMessage.key.remoteJid,
      {
        react: {
          key: data.reactionMessage.key,
          text: data.reactionMessage.reaction,
        },
      },
    );
  }

  public async linkMessage(data: SendLinkDto) {
    return await this.sendMessageWithTyping(data.number, {
      extendedTextMessage: {
        text: (() => {
          let t = data.linkMessage.link;
          if (data.linkMessage?.text) {
            t += '\n\n';
            t += data.linkMessage.text;
          }
          return t;
        })(),
        canonicalUrl: data.linkMessage.link,
        matchedText: data.linkMessage?.link,
        previewType: proto.Message.ExtendedTextMessage.PreviewType.IMAGE,
        title: data.linkMessage?.title || data.linkMessage?.link,
        description: data.linkMessage?.description,
        jpegThumbnail: await (async () => {
          if (data.linkMessage?.thumbnailUrl) {
            try {
              const response = await axios.get(data.linkMessage.thumbnailUrl, {
                responseType: 'arraybuffer',
              });
              return new Uint8Array(response.data);
            } catch (error) {
              //
            }
          }
        })(),
      },
    });
  }

  public async pollMessage(data: SendPollDto) {
    const selectableCount = data.pollMessage.selectableCount ?? 0;

    const pollData = {
      name: data.pollMessage.name,
      selectableOptionsCount: selectableCount,
      options: data.pollMessage.values.map((v) => ({ optionName: v })),
      pollType: proto.Message.PollType.POLL,
      pollContentType: proto.Message.PollContentType.TEXT,
    };

    // selectableCount === 1 → single-select → V3; otherwise multi-select → V1
    const pollKey = selectableCount === 1 ? 'pollCreationMessageV3' : 'pollCreationMessage';
    const message: proto.IMessage = {
      [pollKey]: pollData,
      messageContextInfo: { messageSecret: randomBytes(32) },
    };

    return await this.sendMessageWithTyping(data.number, message, data?.options);
  }

  public async quizMessage(data: SendQuizDto) {
    const jid = this.createJid(data.number);
    if (!isJidNewsletter(jid)) {
      throw new BadRequestException('Quiz can only be sent to channels (@newsletter)');
    }

    const message: proto.IMessage = {
      pollCreationMessageV3: {
        name: data.quizMessage.name,
        selectableOptionsCount: 1,
        options: data.quizMessage.values.map((v) => ({ optionName: v })),
        pollType: proto.Message.PollType.QUIZ,
        pollContentType: proto.Message.PollContentType.TEXT,
        correctAnswer: { optionName: data.quizMessage.correctAnswer },
      },
      messageContextInfo: { messageSecret: randomBytes(32) },
    };

    return await this.sendMessageWithTyping(data.number, message, data?.options);
  }

  public async eventMessage(data: SendEventDto) {
    const startDate = new Date(data.eventMessage.startDate);
    if (isNaN(startDate.getTime())) {
      throw new BadRequestException('Invalid startDate — must be a valid ISO date string');
    }

    const endDate = data.eventMessage.endDate ? new Date(data.eventMessage.endDate) : undefined;
    if (endDate && isNaN(endDate.getTime())) {
      throw new BadRequestException('Invalid endDate — must be a valid ISO date string');
    }

    const startTime = Math.floor(startDate.getTime() / 1000);

    // When call type is provided, auto-generate the WhatsApp call link via the socket
    let joinLink: string | undefined;
    if (data.eventMessage.call) {
      const prefix =
        data.eventMessage.call === 'audio'
          ? 'https://call.whatsapp.com/voice/'
          : 'https://call.whatsapp.com/video/';
      const token = await this.client.createCallLink(data.eventMessage.call, { startTime });
      if (token) {
        joinLink = prefix + token;
      }
    }

    const location = data.eventMessage.location
      ? { name: data.eventMessage.location }
      : undefined;

    // Build proto directly — hasReminder/reminderOffsetSec exist in the proto but are
    // not exposed in Baileys' high-level EventMessageOptions
    const message: proto.IMessage = {
      eventMessage: {
        name: data.eventMessage.name,
        description: data.eventMessage.description,
        startTime,
        endTime: endDate ? Math.floor(endDate.getTime() / 1000) : undefined,
        location,
        joinLink,
        isCanceled: data.eventMessage.isCancelled ?? false,
        // isScheduleCall is only meaningful when a call link is generated
        isScheduleCall: !!data.eventMessage.call,
        // WA defaults: reminder ON at 1 hour, guests OFF
        hasReminder: data.eventMessage.hasReminder ?? true,
        reminderOffsetSec: data.eventMessage.reminderOffsetSec ?? 3600,
        extraGuestsAllowed: data.eventMessage.extraGuestsAllowed ?? false,
      },
      messageContextInfo: {
        messageSecret: randomBytes(32),
      },
    };

    return await this.sendMessageWithTyping(data.number, message, data?.options);
  }

  public async stickerMessage(data: SendStickerDto) {
    const generate = await this.prepareMediaMessage({
      media: data.stickerMessage.sticker,
      mediatype: 'sticker' as any,
      mimetype: 'image/webp',
    });

    return await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      data?.options,
    );
  }

  public async editMessage(data: EditMessage) {
    try {
      const where: any = {
        instanceId: this.instance.id,
      };
      if (isInt(data.id)) {
        const id = Number.parseInt(data.id);
        where.id = id;
      } else {
        where.keyId = data.id;
      }

      const message = await this.repository.message.findFirst({ where });
      const messageKey: proto.IMessageKey = {
        id: message.keyId,
        fromMe: message.keyFromMe,
        remoteJid: message.keyRemoteJid,
        participant: message?.keyParticipant,
      };

      return await this.sendMessageWithTyping<AnyMessageContent>(message.keyRemoteJid, {
        edit: messageKey,
        text: data.text,
      });
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Chat Controller
  public async whatsappNumber(data: WhatsAppNumberDto) {
    const onWhatsapp: OnWhatsAppDto[] = [];
    for await (const number of data.numbers) {
      const jid = this.createJid(number);
      if (isLidUser(jid)) {
        onWhatsapp.push(new OnWhatsAppDto(true, '', jid));
      }
      if (isJidGroup(jid)) {
        const group = await this.findGroup({ groupJid: jid }, 'inner');
        onWhatsapp.push(new OnWhatsAppDto(!!group?.id, group.id, '', group?.subject));
      } else if (jid.includes('@broadcast')) {
        onWhatsapp.push(new OnWhatsAppDto(true, jid));
      } else {
        try {
          const result = (await this.client.onWhatsApp(jid))[0];

          let lid: string | undefined;
          if (result?.exists) {
            const item = (await this.getLid(result.jid))[0];
            lid = item?.lid;
          }
          onWhatsapp.push(new OnWhatsAppDto(!!result.exists, result.jid, lid));
        } catch (error) {
          onWhatsapp.push(new OnWhatsAppDto(false, number));
        }
      }
    }

    return onWhatsapp;
  }

  public async getLid(...jids: string[]): Promise<{ id: string; lid: string }[]> {
    const q = new USyncQuery().withLIDProtocol().withContext('background');

    for (const jid of jids) {
      if (isLidUser(jid)) {
        continue;
      }

      q.withUser(new USyncUser().withId(jid));
    }

    if (q.users.length === 0) {
      return [];
    }

    const results = await this.client.executeUSyncQuery(q);
    if (results) {
      return results.list
        .filter((i) => !!i?.lid)
        .map(({ lid, id }) => ({ lid, id }) as { id: string; lid: string });
    }

    return [];
  }

  /**
   * @deprecated
   */
  public async markMessageAsRead(data: ReadMessageDto) {
    try {
      const keys: proto.IMessageKey[] = [];
      data.readMessages.forEach((read) => {
        if (isJidGroup(read.remoteJid) || isLidUser(read.remoteJid)) {
          keys.push({
            remoteJid: read.remoteJid,
            fromMe: read.fromMe,
            id: read.id,
          });
        }
      });
      await this.client.readMessages(keys);
      return { message: 'Read messages', read: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Read messages fail', error.toString());
    }
  }

  public async deleteChat(chatId: string) {
    try {
      const lastMessage = await this.repository.message.findFirst({
        where: { keyRemoteJid: this.createJid(chatId) },
        orderBy: { messageTimestamp: 'desc' },
      });
      if (!lastMessage) {
        throw new Error('Chat not found');
      }

      await this.client.chatModify(
        {
          delete: true,
          lastMessages: [
            {
              key: {
                id: lastMessage.keyId,
                fromMe: lastMessage.keyFromMe,
                remoteJid: lastMessage.keyRemoteJid,
              },
              messageTimestamp: lastMessage.messageTimestamp,
            },
          ],
        },
        lastMessage.keyRemoteJid,
      );

      return { deletedAt: new Date(), chatId: lastMessage.keyRemoteJid };
    } catch (error) {
      throw new BadRequestException('Error while deleting chat', error?.message);
    }
  }

  public async readMessages(data: ReadMessageIdDto) {
    const keys: proto.IMessageKey[] = [];
    try {
      const messages = await this.repository.message.findMany({
        where: { id: { in: data.messageId } },
        select: {
          keyFromMe: true,
          keyId: true,
          keyRemoteJid: true,
          keyParticipant: true,
        },
      });

      for (const message of messages) {
        keys.push({
          remoteJid: message.keyRemoteJid,
          fromMe: message.keyFromMe,
          id: message.keyId,
          participant: message?.keyParticipant,
        });
      }
      await this.client.readMessages(keys);
      return { message: 'Read messages', read: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Read messages fail', error.toString());
    }
  }

  public async archiveChat(data: ArchiveChatDto) {
    try {
      data.lastMessage.messageTimestamp =
        data.lastMessage?.messageTimestamp ?? Date.now();
      await this.client.chatModify(
        {
          archive: data.archive,
          lastMessages: [data.lastMessage],
        },
        data.lastMessage.key.remoteJid,
      );

      return {
        chatId: data.lastMessage.key.remoteJid,
        archived: true,
      };
    } catch (error) {
      throw new InternalServerErrorException({
        archived: false,
        message: [
          'An error occurred while archiving the chat. Open a calling.',
          error.toString(),
        ],
      });
    }
  }

  public async deleteMessage(del: DeleteMessage) {
    try {
      const id = Number.parseInt(del.id);
      const everyOne = del?.everyOne === 'true';
      const message = await this.repository.message.findUnique({
        where: { id },
      });

      if (!everyOne) {
        await this.client.chatModify(
          {
            clear: {
              messages: [
                {
                  id: message.keyId,
                  fromMe: message.keyFromMe,
                  timestamp: message.messageTimestamp,
                },
              ],
            },
          } as any,
          message.keyRemoteJid,
        );
      }

      await this.client.sendMessage(message.keyRemoteJid, {
        delete: {
          id: message.keyId,
          fromMe: message.keyFromMe,
          participant: message?.keyParticipant,
          remoteJid: message.keyRemoteJid,
        },
      });

      return { deletedAt: new Date(), message };
    } catch (error) {
      throw new InternalServerErrorException(
        'Error while deleting message for everyone',
        error?.toString(),
      );
    }
  }

  public async isMediaMessage(m: PrismType.Message) {
    let mediaMessage: any;
    let mediaType: string;

    try {
      const msg: proto.IWebMessageInfo = m?.content
        ? {
            key: {
              id: m.keyId,
              fromMe: m.keyFromMe,
              remoteJid: m.keyRemoteJid,
            },
            message: {
              [m.messageType]: m.content,
            },
          }
        : ((await this.getMessage(m, true)) as proto.IWebMessageInfo);

      if (msg?.message?.documentWithCaptionMessage) {
        msg.message.documentMessage =
          msg.message.documentWithCaptionMessage?.message?.documentMessage;
      }

      for (const subtype of MessageSubtype()) {
        if (msg?.message?.[subtype]) {
          msg.message = msg.message[subtype].message;
        }
      }

      for (const type of TypeMediaMessage()) {
        mediaMessage = msg?.message?.[type];
        if (mediaMessage) {
          mediaType = type;
          break;
        }
      }

      if (!mediaMessage) {
        return {};
      }

      return { mediaMessage, mediaType, msg };
    } catch {
      return {};
    }
  }

  public async getMediaMessage(m: PrismType.Message, inner = false) {
    try {
      const { mediaMessage, mediaType, msg } = await this.isMediaMessage(m);

      if (!mediaMessage) {
        if (inner) {
          return;
        }
        throw new Error('The message is not of the media type');
      }

      if (typeof mediaMessage['mediaKey'] === 'object') {
        msg.message = JSON.parse(JSON.stringify(msg.message));
      }

      const stream = await downloadMediaMessage(
        { key: msg?.key, message: msg?.message },
        'stream',
        {},
        {
          logger: P({ level: 'silent' }) as any,
          reuploadRequest: this.client.updateMediaMessage,
        },
      );

      const ext = mime.extension(mediaMessage?.['mimetype']);

      const fileName =
        mediaMessage?.['fileName'] || `${msg.key.id}.${ext}` || `${ulid()}.${ext}`;

      return {
        mediaType,
        fileName,
        caption: mediaMessage?.['caption'],
        size: {
          fileLength: mediaMessage?.['fileLength'],
          height: mediaMessage?.['height'],
          width: mediaMessage?.['width'],
        },
        mimetype: mediaMessage?.['mimetype'],
        stream,
      };
    } catch (error) {
      this.logger.error(error);
      if (this.configService.get<Database>('DATABASE').DB_OPTIONS.ACTIVITY_LOGS) {
        this.repository.activityLogs
          .create({
            data: {
              type: 'error',
              context: WAStartupService.name,
              description: 'Error on get media message',
              content: [error?.message, JSON.stringify(error?.stack)],
              instanceId: this.instance.id,
            },
          })
          .catch((error) => this.logger.error(error));
      }
      if (inner) {
        return;
      }
      throw new BadRequestException(error.message);
    }
  }

  async getMediaUrl(messageId: number, expiry: number) {
    if (!s3Service.BUCKET?.ENABLE) {
      throw new BadRequestException('S3 is not enabled');
    }

    const media = await this.repository.media.findFirst({
      where: { messageId },
    });

    const mediaUrl = await s3Service.getObjectUrl(media.fileName, expiry);

    return { mediaUrl };
  }

  public async fetchContacts(query: Query<PrismType.Contact>) {
    return await this.repository.contact.findMany({
      where: {
        instanceId: this.instance.id,
        remoteJid: query.where?.remoteJid,
      },
    });
  }

  public async fetchMessages(query: Query<PrismType.Message>) {
    const where = {
      instanceId: this.instance.id,
      id: query?.where?.id,
      keyId: query?.where?.keyId,
      keyFromMe: query?.where?.keyFromMe,
      keyRemoteJid: query.where?.keyRemoteJid,
      device: query?.where?.device,
      messageType: query?.where?.messageType,
    };

    if (query?.where?.['messageStatus']) {
      where['MessageUpdate'] = {
        some: {
          status: query.where['messageStatus'],
        },
      };
    }

    const count = await this.repository.message.count({
      where,
    });

    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    const messages = await this.repository.message.findMany({
      where,
      orderBy: {
        messageTimestamp: 'desc',
      },
      skip: query.offset * (query?.page === 1 ? 0 : query?.page - 1),
      take: query.offset,
      select: {
        id: true,
        keyId: true,
        keyFromMe: true,
        keyRemoteJid: true,
        keyParticipant: true,
        pushName: true,
        messageType: true,
        content: true,
        messageTimestamp: true,
        instanceId: true,
        device: true,
        MessageUpdate: {
          select: {
            status: true,
            dateTime: true,
          },
        },
      },
    });

    return {
      messages: {
        total: count,
        pages: Math.ceil(count / query.offset),
        currentPage: query.page,
        records: messages,
      },
    };
  }

  public async createChannel(name: string, description?: string) {
    const meta = await this.client.newsletterCreate(name, description);

    // newsletterCreate emits no Baileys events — save to Chat table manually so
    // fetchChannels picks it up immediately without waiting for a reconnect sync
    const existing = await this.repository.chat.findFirst({
      where: { instanceId: this.instance.id, remoteJid: meta.id },
    });
    if (existing) {
      await this.repository.chat.update({
        where: { id: existing.id },
        data: { content: meta as any },
      });
    } else {
      await this.repository.chat.create({
        data: { remoteJid: meta.id, instanceId: this.instance.id, content: meta as any },
      });
    }

    return meta;
  }

  private normalizeNewsletterMeta(meta: any) {
    if (!meta) return null;
    // Already flat (from parseNewsletterCreateResponse / createChannel)
    if (typeof meta.name === 'string') return meta;
    // Raw nested WA API response (from newsletterMetadata)
    if (meta.thread_metadata) {
      return {
        id: meta.id,
        name: meta.thread_metadata.name?.text ?? null,
        description: meta.thread_metadata.description?.text ?? null,
        invite: meta.thread_metadata.invite ?? null,
        subscribers: parseInt(meta.thread_metadata.subscribers_count || '0', 10),
        verification: meta.thread_metadata.verification ?? null,
        creation_time: parseInt(meta.thread_metadata.creation_time || '0', 10),
        picture: meta.thread_metadata.picture ?? null,
        state: meta.state ?? null,
        role: meta.viewer_metadata?.role ?? null,
        mute_state: meta.viewer_metadata?.mute ?? null,
      };
    }
    return null;
  }

  public async fetchChannels() {
    const chats = await this.repository.chat.findMany({
      where: { instanceId: this.instance.id, remoteJid: { contains: '@newsletter' } },
      distinct: ['remoteJid'],
      orderBy: { id: 'desc' },
    });

    const results = await Promise.all(
      chats.map(async (chat) => {
        const content = chat.content as any;
        const normalized = this.normalizeNewsletterMeta(content);

        if (normalized) {
          return { ...chat, metadata: normalized };
        }

        // Content is stale chat data — fetch real metadata, normalize, and persist
        try {
          const meta = await this.client.newsletterMetadata('jid', chat.remoteJid);
          if (meta) {
            await this.repository.chat
              .update({ where: { id: chat.id }, data: { content: meta as any } })
              .catch(() => {});
            return { ...chat, metadata: this.normalizeNewsletterMeta(meta) };
          }
        } catch { /* best-effort */ }

        return { ...chat, metadata: null };
      }),
    );

    return results.filter((r) => r.metadata !== null);
  }

  public async fetchChats(type?: string) {
    const where = { instanceId: this.instance.id };
    if (['chats', 'group'].includes(type)) {
      where['remoteJid'] = {
        contains: '@s.whatsapp.net',
      };
    }
    return await this.repository.chat.findMany({ where });
  }

  public async addChannelsByJid(channels: string[]) {
    const results = [];
    for (const channel of channels) {
      // Detect if it's a JID or an invite code
      const isJid = channel.includes('@newsletter');
      const type: 'jid' | 'invite' = isJid ? 'jid' : 'invite';
      const key = isJid ? channel : channel;
      try {
        const meta = await this.client.newsletterMetadata(type, key);
        if (meta) {
          const remoteJid = (meta as any).id;
          const existing = await this.repository.chat.findFirst({
            where: { instanceId: this.instance.id, remoteJid },
          });
          if (existing) {
            await this.repository.chat.update({
              where: { id: existing.id },
              data: { content: meta as any },
            });
          } else {
            await this.repository.chat.create({
              data: { remoteJid, instanceId: this.instance.id, content: meta as any },
            });
          }
          results.push({ jid: remoteJid, status: 'ok' });
        } else {
          results.push({ channel, status: 'not_found' });
        }
      } catch {
        results.push({ channel, status: 'error' });
      }
    }
    return results;
  }

  public async rejectCall(data: RejectCallDto) {
    try {
      await this.client.rejectCall(data.callId, data.callFrom);
      return {
        call: data,
        rejected: true,
        status: 'rejected',
      };
    } catch (error) {
      throw new InternalServerErrorException(
        'Failed to reject a call',
        error?.toString(),
      );
    }
  }

  public async assertSessions(chats: string[]) {
    if (!Array.isArray(chats) || chats.length === 0) {
      throw new BadRequestException('Empty or invalid array');
    }
    try {
      await this.client.assertSessions(
        chats.map((c) => this.createJid(c)),
        true,
      );
      return { message: 'Session asserted' };
    } catch (error) {
      throw new InternalServerErrorException('Error asserting session', error.toString());
    }
  }

  // Group
  public async createGroup(create: CreateGroupDto) {
    try {
      const participants = create.participants.map((p) => this.createJid(p));
      const { id } = await this.client.groupCreate(create.subject, participants);
      if (create?.description) {
        await this.client.groupUpdateDescription(id, create.description);
      }

      const group = await this.client.groupMetadata(id);

      return { groupMetadata: group };
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException('Error creating group', error.toString());
    }
  }

  public async updateGroupPicture(picture: GroupPictureDto) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture.image)) {
        pic = (await axios.get(picture.image, { responseType: 'arraybuffer' })).data;
      } else if (isBase64(picture.image)) {
        pic = Buffer.from(picture.image, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }
      await this.client.updateProfilePicture(picture.groupJid, pic);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error creating group', error.toString());
    }
  }

  public async findGroup(id: GroupJid, reply: 'inner' | 'out' = 'out') {
    try {
      return await this.client.groupMetadata(id.groupJid);
    } catch (error) {
      if (reply === 'inner') {
        return;
      }
      throw new BadRequestException('Error fetching group', error.toString());
    }
  }

  public async findAllGroups() {
    try {
      return await this.client.groupFetchAllParticipating();
    } catch (error) {
      throw new BadRequestException('Error searching all groups', error.toString());
    }
  }

  public async invitationCode(id: GroupJid) {
    try {
      const code = await this.client.groupInviteCode(id.groupJid);
      return { inviteUrl: `https://chat.whatsapp.com/${code}`, inviteCode: code };
    } catch (error) {
      throw new BadRequestException('No invite code', error.toString());
    }
  }

  public async revokeInvitationCode(id: GroupJid) {
    try {
      const inviteCode = await this.client.groupRevokeInvite(id.groupJid);
      return { revoked: true, inviteCode };
    } catch (error) {
      throw new BadRequestException('Revoke error', error.toString());
    }
  }

  public async findParticipants(id: GroupJid) {
    try {
      const participants = (await this.client.groupMetadata(id.groupJid)).participants;
      return { participants };
    } catch (error) {
      throw new BadRequestException('No participants', error.toString());
    }
  }

  public async updateGParticipant(update: GroupUpdateParticipantDto) {
    try {
      const participants = update.participants.map((p) => this.createJid(p));
      const updateParticipants = await this.client.groupParticipantsUpdate(
        update.groupJid,
        participants,
        update.action,
      );
      return { updateParticipants: updateParticipants };
    } catch (error) {
      throw new BadRequestException('Error updating participants', error.toString());
    }
  }

  public async leaveGroup(id: GroupJid) {
    try {
      await this.client.groupLeave(id.groupJid);
      return { groupJid: id.groupJid, leave: true };
    } catch (error) {
      throw new BadRequestException('Unable to leave the group', error.toString());
    }
  }
}
