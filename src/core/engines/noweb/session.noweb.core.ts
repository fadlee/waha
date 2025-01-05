import makeWASocket, {
  Browsers,
  Chat,
  Contact,
  DisconnectReason,
  downloadMediaMessage,
  extractMessageContent,
  getAggregateVotesInPollMessage,
  getContentType,
  getKeyAuthor,
  getUrlFromDirectPath,
  isJidGroup,
  isJidNewsletter,
  isJidStatusBroadcast,
  isJidUser,
  isRealMessage,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  NewsletterMetadata,
  normalizeMessageContent,
  PresenceData,
  proto,
  SocketConfig,
  WAMessageContent,
  WAMessageKey,
  WAMessageUpdate,
} from '@adiwajshing/baileys';
import { WACallEvent } from '@adiwajshing/baileys/lib/Types/Call';
import { BaileysEventMap } from '@adiwajshing/baileys/lib/Types/Events';
import { GroupMetadata } from '@adiwajshing/baileys/lib/Types/GroupMetadata';
import {
  Label as NOWEBLabel,
  LabelActionBody,
} from '@adiwajshing/baileys/lib/Types/Label';
import {
  ChatLabelAssociation,
  LabelAssociationType,
} from '@adiwajshing/baileys/lib/Types/LabelAssociation';
import { MessageUserReceiptUpdate } from '@adiwajshing/baileys/lib/Types/Message';
import { isLidUser } from '@adiwajshing/baileys/lib/WABinary/jid-utils';
import { Logger as BaileysLogger } from '@adiwajshing/baileys/node_modules/pino';
import { UnprocessableEntityException } from '@nestjs/common';
import { sendButtonMessage } from '@waha/core/engines/noweb/noweb.buttons';
import { NowebInMemoryStore } from '@waha/core/engines/noweb/store/NowebInMemoryStore';
import { IMediaEngineProcessor } from '@waha/core/media/IMediaEngineProcessor';
import { flipObject, splitAt } from '@waha/helpers';
import { PairingCodeResponse } from '@waha/structures/auth.dto';
import { CallData } from '@waha/structures/calls.dto';
import {
  Channel,
  ChannelRole,
  CreateChannelRequest,
  ListChannelsQuery,
} from '@waha/structures/channels.dto';
import {
  ChatSummary,
  GetChatMessageQuery,
  GetChatMessagesFilter,
  GetChatMessagesQuery,
  PinDuration,
} from '@waha/structures/chats.dto';
import { SendButtonsRequest } from '@waha/structures/chatting.buttons.dto';
import { ContactQuery, ContactRequest } from '@waha/structures/contacts.dto';
import { BinaryFile, RemoteFile } from '@waha/structures/files.dto';
import {
  Label,
  LabelChatAssociation,
  LabelDTO,
  LabelID,
} from '@waha/structures/labels.dto';
import { ReplyToMessage } from '@waha/structures/message.dto';
import { PaginationParams } from '@waha/structures/pagination.dto';
import {
  EnginePayload,
  PollVote,
  PollVotePayload,
  WAMessageAckBody,
} from '@waha/structures/webhooks.dto';
import { LoggerBuilder } from '@waha/utils/logging';
import { sleep, waitUntil } from '@waha/utils/promiseTimeout';
import { exclude } from '@waha/utils/reactive/ops/exclude';
import { SingleDelayedJobRunner } from '@waha/utils/SingleDelayedJobRunner';
import { SinglePeriodicJobRunner } from '@waha/utils/SinglePeriodicJobRunner';
import * as Buffer from 'buffer';
import { Agent } from 'https';
import * as lodash from 'lodash';
import * as NodeCache from 'node-cache';
import {
  filter,
  fromEvent,
  identity,
  merge,
  mergeAll,
  mergeMap,
  Observable,
  partition,
  share,
  Subject,
} from 'rxjs';
import { map } from 'rxjs/operators';

import {
  ChatRequest,
  CheckNumberStatusQuery,
  EditMessageRequest,
  MessageContactVcardRequest,
  MessageDestination,
  MessageFileRequest,
  MessageForwardRequest,
  MessageImageRequest,
  MessageLinkPreviewRequest,
  MessageLocationRequest,
  MessagePollRequest,
  MessageReactionRequest,
  MessageReplyRequest,
  MessageStarRequest,
  MessageTextRequest,
  MessageVoiceRequest,
  SendSeenRequest,
  WANumberExistResult,
} from '../../../structures/chatting.dto';
import {
  ACK_UNKNOWN,
  SECOND,
  WAHAEngine,
  WAHAEvents,
  WAHAPresenceStatus,
  WAHASessionStatus,
  WAMessageAck,
} from '../../../structures/enums.dto';
import {
  CreateGroupRequest,
  ParticipantsRequest,
} from '../../../structures/groups.dto';
import {
  WAHAChatPresences,
  WAHAPresenceData,
} from '../../../structures/presence.dto';
import {
  WAMessage,
  WAMessageReaction,
} from '../../../structures/responses.dto';
import { MeInfo } from '../../../structures/sessions.dto';
import {
  BROADCAST_ID,
  DeleteStatusRequest,
  TextStatus,
} from '../../../structures/status.dto';
import {
  ensureSuffix,
  getChannelInviteLink,
  isNewsletter,
  WhatsappSession,
} from '../../abc/session.abc';
import {
  AvailableInPlusVersion,
  NotImplementedByEngineError,
} from '../../exceptions';
import { toVcard } from '../../helpers';
import { createAgentProxy } from '../../helpers.proxy';
import { QR } from '../../QR';
import { NowebAuthFactoryCore } from './NowebAuthFactoryCore';
import { INowebStore } from './store/INowebStore';
import { NowebPersistentStore } from './store/NowebPersistentStore';
import { NowebStorageFactoryCore } from './store/NowebStorageFactoryCore';
import { ensureNumber, extractMediaContent } from './utils';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const QRCode = require('qrcode');

export const BaileysEvents = {
  CONNECTION_UPDATE: 'connection.update',
  CREDS_UPDATE: 'creds.update',
  MESSAGES_UPDATE: 'messages.update',
  MESSAGES_UPSERT: 'messages.upsert',
  MESSAGE_RECEIPT_UPDATE: 'message-receipt.update',
  GROUPS_UPSERT: 'groups.upsert',
  PRESENCE_UPDATE: 'presence.update',
};

const PresenceStatuses = {
  unavailable: WAHAPresenceStatus.OFFLINE,
  available: WAHAPresenceStatus.ONLINE,
  composing: WAHAPresenceStatus.TYPING,
  recording: WAHAPresenceStatus.RECORDING,
  paused: WAHAPresenceStatus.PAUSED,
};
const ToEnginePresenceStatus = flipObject(PresenceStatuses);

export class WhatsappSessionNoWebCore extends WhatsappSession {
  private START_ATTEMPT_DELAY_SECONDS = 2;
  private AUTO_RESTART_AFTER_SECONDS = 28 * 60;

  engine = WAHAEngine.NOWEB;
  authFactory = new NowebAuthFactoryCore();
  storageFactory = new NowebStorageFactoryCore();
  private startDelayedJob: SingleDelayedJobRunner;
  private shouldRestart: boolean;

  private autoRestartJob: SinglePeriodicJobRunner;
  private msgRetryCounterCache: NodeCache;
  private placeholderResendCache: NodeCache;
  protected engineLogger: BaileysLogger;

  private authNOWEBStore: any;

  sock: ReturnType<typeof makeWASocket>;
  store: INowebStore;
  private qr: QR;

  public constructor(config) {
    super(config);
    this.shouldRestart = true;

    this.qr = new QR();
    // external map to store retry counts of messages when decryption/encryption fails
    // keep this out of the socket itself, to prevent a message decryption/encryption loop across socket restarts
    this.msgRetryCounterCache = new NodeCache({
      stdTTL: 60 * 60, // 1 hour
      useClones: false,
    });
    this.placeholderResendCache = new NodeCache({
      stdTTL: 60 * 60, // 1 hour
      useClones: false,
    });

    this.engineLogger = this.loggerBuilder.child({
      name: 'NOWEBEngine',
    }) as unknown as BaileysLogger;

    // Restart job if session failed
    this.startDelayedJob = new SingleDelayedJobRunner(
      'start-engine',
      this.START_ATTEMPT_DELAY_SECONDS * SECOND,
      this.logger,
    );

    // Enable auto-restart
    const shiftSeconds = Math.floor(Math.random() * 30);
    const delay = this.AUTO_RESTART_AFTER_SECONDS + shiftSeconds;
    this.autoRestartJob = new SinglePeriodicJobRunner(
      'auto-restart',
      delay * SECOND,
      this.logger,
    );
    this.authNOWEBStore = null;
  }

  async start() {
    this.status = WAHASessionStatus.STARTING;
    await this.buildClient().catch((err) => {
      this.logger.error('Failed to start the client');
      this.logger.error(err, err.stack);
      this.status = WAHASessionStatus.FAILED;
      this.restartClient();
    });
  }

  async unpair() {
    this.unpairing = true;
    this.shouldRestart = false;
    await this.sock?.logout();
  }

  getSocketConfig(agent, state): Partial<SocketConfig> {
    const fullSyncEnabled = this.sessionConfig?.noweb?.store?.fullSync || false;
    const browser = fullSyncEnabled
      ? Browsers.ubuntu('Desktop')
      : Browsers.ubuntu('Chrome');
    let markOnlineOnConnect = this.sessionConfig?.noweb?.markOnline;
    if (markOnlineOnConnect == undefined) {
      markOnlineOnConnect = true;
    }
    return {
      agent: agent,
      fetchAgent: agent,
      auth: state,
      printQRInTerminal: false,
      browser: browser,
      logger: this.engineLogger,
      mobile: false,
      defaultQueryTimeoutMs: 120_000,
      keepAliveIntervalMs: 30_000,
      getMessage: (key) => this.getMessage(key),
      syncFullHistory: fullSyncEnabled,
      msgRetryCounterCache: this.msgRetryCounterCache,
      placeholderResendCache: this.placeholderResendCache,
      markOnlineOnConnect: markOnlineOnConnect,
    };
  }

  async makeSocket(): Promise<any> {
    if (!this.authNOWEBStore) {
      const store = await this.authFactory.buildAuth(
        this.sessionStore,
        this.name,
      );
      /** caching makes the store faster to send/recv messages */
      store.state.keys = makeCacheableSignalKeyStore(
        store.state.keys,
        this.engineLogger,
      );
      this.authNOWEBStore = store;
    }
    const { state, saveCreds } = this.authNOWEBStore;
    const agent = this.makeAgent();
    const socketConfig: SocketConfig = this.getSocketConfig(
      agent,
      state,
    ) as SocketConfig;
    const sock = makeWASocket(socketConfig);
    sock.ev.on('creds.update', saveCreds);
    return sock;
  }

  protected makeAgent(): Agent {
    if (!this.proxyConfig) {
      return undefined;
    }
    return createAgentProxy(this.proxyConfig);
  }

  private async ensureStore() {
    if (this.store) {
      return;
    }

    this.logger.debug(`Making a new store...`);
    const storeEnabled = this.sessionConfig?.noweb?.store?.enabled || false;
    if (!storeEnabled) {
      this.logger.debug('Using NowebInMemoryStore');
      this.store = new NowebInMemoryStore();
      return;
    }

    this.logger.debug('Using NowebPersistentStore');
    const storage = this.storageFactory.createStorage(
      this.sessionStore,
      this.name,
    );
    this.store = new NowebPersistentStore(
      this.loggerBuilder.child({ name: NowebPersistentStore.name }),
      storage,
    );
    await this.store.init();
  }

  connectStore() {
    this.logger.debug(`Connecting store...`);
    this.logger.debug(`Binding store to socket...`);
    this.store.bind(this.sock.ev, this.sock);
  }

  resubscribeToKnownPresences() {
    for (const jid in this.store.presences) {
      this.sock.presenceSubscribe(jid);
    }
  }

  async buildClient() {
    this.shouldRestart = true;
    // @ts-ignore
    this.sock?.ev?.removeAllListeners();

    await this.ensureStore();
    this.sock = await this.makeSocket();

    this.issueMessageUpdateOnEdits();
    this.issuePresenceUpdateOnMessageUpsert();
    if (this.isDebugEnabled()) {
      this.listenEngineEventsInDebugMode();
    }
    this.connectStore();
    this.listenConnectionEvents();
    this.subscribeEngineEvents2();
    this.listenContactsUpdatePictureProfile();
    this.enableAutoRestart();
  }

  private enableAutoRestart() {
    this.autoRestartJob.start(async () => {
      this.logger.info('Auto-restarting the client connection...');
      if (this.sock?.ws?.isConnecting) {
        this.logger.warn('Auto-restart skipped, the client is connecting...');
        return;
      }
      this.sock?.end(undefined);
    });
  }

  protected async getMessage(
    key: WAMessageKey,
  ): Promise<WAMessageContent | undefined> {
    if (!this.store) {
      return proto.Message.fromObject({});
    }
    const msg = await this.store.loadMessage(key.remoteJid, key.id);
    return msg?.message || undefined;
  }

  protected listenEngineEventsInDebugMode() {
    this.sock.ev.process((events) => {
      this.logger.debug({ events: events }, `NOWEB events`);
    });
  }

  private restartClient() {
    if (!this.shouldRestart) {
      this.logger.debug(
        'Should not restart the client, ignoring restart request',
      );
      return;
    }
    this.startDelayedJob.schedule(async () => {
      if (!this.shouldRestart) {
        this.logger.warn(
          'Should not restart the client, ignoring restart request',
        );
        return;
      }
      await this.end();
      await this.start();
    });
  }

  protected listenConnectionEvents() {
    this.logger.debug(`Start listening ${BaileysEvents.CONNECTION_UPDATE}...`);
    this.sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr, isNewLogin } = update;
      if (isNewLogin) {
        this.restartClient();
      } else if (connection === 'open') {
        this.qr.save('');
        this.status = WAHASessionStatus.WORKING;
        this.resubscribeToKnownPresences();
        return;
      } else if (connection === 'close') {
        const shouldReconnect =
          // @ts-ignore: Property output does not exist on type 'Error'
          lastDisconnect.error?.output?.statusCode !==
          DisconnectReason.loggedOut;
        this.qr.save('');
        // reconnect if not logged out
        if (shouldReconnect) {
          if (lastDisconnect.error) {
            this.logger.info(
              `Connection closed due to '${lastDisconnect.error}', reconnecting...`,
            );
          }
          this.restartClient();
        } else {
          this.logger.error(
            `Connection closed due to '${lastDisconnect.error}', do not reconnect the session.`,
          );
          await this.failed();
        }
      }

      // Save QR
      if (qr) {
        const url = await QRCode.toDataURL(qr);
        this.qr.save(url, qr);
        this.printQR(this.qr);
        this.status = WAHASessionStatus.SCAN_QR_CODE;
      }
    });
  }

  async stop() {
    this.shouldRestart = false;
    this.startDelayedJob.cancel();
    this.autoRestartJob.stop();

    if (this.authNOWEBStore && this.status == WAHASessionStatus.WORKING) {
      this.logger.info('Saving creds before stopping...');
      await this.authNOWEBStore.saveCreds().catch((e) => {
        this.logger.error('Failed to save creds');
        this.logger.error(e, e.stack);
      });
      this.logger.info('Creds saved');
      this.authNOWEBStore = null;
    }
    this.status = WAHASessionStatus.STOPPED;
    this.stopEvents();

    await this.end();
    await this.store?.close();
  }

  protected async failed() {
    this.shouldRestart = false;
    this.startDelayedJob.cancel();
    this.autoRestartJob.stop();

    // We'll restart the client if it's in the process of unpairing
    this.status = WAHASessionStatus.FAILED;

    if (this.unpairing) {
      // Wait for unpairing to complete before ending the socket
      await sleep(1_000);
    }

    await this.end();
    await this.store?.close();
  }

  private issueMessageUpdateOnEdits() {
    // Remove it after it's been merged
    // https://github.com/WhiskeySockets/Baileys/pull/855/
    this.sock.ev.on('messages.upsert', ({ messages }) => {
      for (const message of messages) {
        const content = normalizeMessageContent(message.message);
        const protocolMsg = content?.protocolMessage;
        if (
          protocolMsg !== null &&
          protocolMsg !== undefined &&
          protocolMsg.editedMessage
        ) {
          this.sock?.ev.emit('messages.update', [
            {
              key: {
                ...message.key,
                id: protocolMsg.key.id,
              },
              update: { message: protocolMsg.editedMessage },
            },
          ]);
        }
      }
    });
  }

  private issuePresenceUpdateOnMessageUpsert() {
    //
    // Fix for "typing" after sending a message
    // https://github.com/devlikeapro/waha/issues/379
    //
    this.sock.ev.on('messages.upsert', ({ messages }) => {
      const meId = this.sock?.authState?.creds?.me?.id;
      for (const message of messages) {
        if (!isRealMessage(message, meId)) {
          continue;
        }
        if (message.key.fromMe) {
          continue;
        }
        const jid = message.key.remoteJid;
        const participant = message.key.participant || jid;
        const jidPresences = this.store?.presences?.[jid];
        const participantPresence = jidPresences?.[participant];
        if (participantPresence?.lastKnownPresence === 'composing') {
          this.logger.debug(
            `Fixing presence for '${participant}' in '${jid} since it's typing`,
          );
          const presence: PresenceData = { lastKnownPresence: 'available' };
          this.sock?.ev?.emit('presence.update', {
            id: jid,
            presences: { [participant]: presence },
          });
        }
      }
    });
  }

  private async end() {
    this.autoRestartJob.stop();
    // @ts-ignore
    this.sock?.ev?.removeAllListeners();
    this.sock?.ws?.removeAllListeners();
    // wait until connection is not connecting to avoid error:
    // "WebSocket was closed before the connection was established"
    await waitUntil(async () => !this.sock?.ws?.isConnecting, 1_000, 10_000);
    this.sock?.end(undefined);
  }

  getSessionMeInfo(): MeInfo | null {
    const me = this.sock?.authState?.creds?.me;
    if (!me) {
      return null;
    }
    const meId = jidNormalizedUser(me.id);
    return {
      id: toCusFormat(meId),
      pushName: me.name,
    };
  }

  /**
   * START - Methods for API
   */

  /**
   * Auth methods
   */
  public getQR(): QR {
    return this.qr;
  }

  public async requestCode(
    phoneNumber: string,
    method: string,
    params?: any,
  ): Promise<PairingCodeResponse> {
    if (method) {
      const err = `NOWEB engine doesn't support any 'method', remove it and try again`;
      throw new UnprocessableEntityException(err);
    }

    if (this.status == WAHASessionStatus.STARTING) {
      this.logger.debug('Waiting for connection update...');
      await this.sock.waitForConnectionUpdate((update) => !!update.qr);
    }

    if (this.status != WAHASessionStatus.SCAN_QR_CODE) {
      const err = `Can request code only in SCAN_QR_CODE status. The current status is ${this.status}`;
      throw new UnprocessableEntityException(err);
    }

    this.logger.info(`Requesting pairing code for '${phoneNumber}'...`);
    const code: string = await this.sock.requestPairingCode(phoneNumber);
    // show it as ABCD-ABCD
    const parts = splitAt(code, 4);
    const codeRepr = parts.join('-');
    this.logger.info(`Your code: ${codeRepr}`);
    return { code: codeRepr };
  }

  async getScreenshot(): Promise<Buffer> {
    if (this.status === WAHASessionStatus.STARTING) {
      throw new UnprocessableEntityException(
        `The session is starting, please try again after few seconds`,
      );
    } else if (this.status === WAHASessionStatus.SCAN_QR_CODE) {
      return Promise.resolve(this.qr.get());
    } else if (this.status === WAHASessionStatus.WORKING) {
      throw new UnprocessableEntityException(
        `Can not get screenshot for non chrome based engine.`,
      );
    } else {
      throw new UnprocessableEntityException(`Unknown status - ${this.status}`);
    }
  }

  /**
   * Other methods
   */
  async checkNumberStatus(
    request: CheckNumberStatusQuery,
  ): Promise<WANumberExistResult> {
    const phone = request.phone.split('@')[0];
    const [result] = await this.sock.onWhatsApp(phone);
    if (!result || !result.exists) {
      return { numberExists: false };
    }
    return {
      numberExists: true,
      chatId: toCusFormat(result.jid),
    };
  }

  async sendText(request: MessageTextRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const message = {
      text: request.text,
      mentions: request.mentions?.map(toJID),
      linkPreview: this.getLinkPreview(request),
    };
    const options = await this.getMessageOptions(request);
    return this.sock.sendMessage(chatId, message, options);
  }

  public deleteMessage(chatId: string, messageId: string) {
    const jid = toJID(this.ensureSuffix(chatId));
    const key = parseMessageIdSerialized(messageId);
    return this.sock.sendMessage(jid, { delete: key });
  }

  public editMessage(
    chatId: string,
    messageId: string,
    request: EditMessageRequest,
  ) {
    const jid = toJID(this.ensureSuffix(chatId));
    const key = parseMessageIdSerialized(messageId);
    const message = {
      text: request.text,
      mentions: request.mentions?.map(toJID),
      edit: key,
    };
    return this.sock.sendMessage(jid, message);
  }

  async sendContactVCard(request: MessageContactVcardRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const contacts = request.contacts.map((el) => ({ vcard: toVcard(el) }));
    await this.sock.sendMessage(chatId, {
      contacts: {
        contacts: contacts,
      },
    });
  }

  async sendPoll(request: MessagePollRequest) {
    const requestPoll = request.poll;
    const poll = {
      name: requestPoll.name,
      values: requestPoll.options,
      selectableCount: requestPoll.multipleAnswers
        ? requestPoll.options.length
        : 1,
    };
    const message = { poll: poll };
    const remoteJid = toJID(request.chatId);
    const options = await this.getMessageOptions(request);
    const result = await this.sock.sendMessage(remoteJid, message, options);
    return this.toWAMessage(result);
  }

  async reply(request: MessageReplyRequest) {
    const options = await this.getMessageOptions(request);
    const message = {
      text: request.text,
      mentions: request.mentions?.map(toJID),
    };
    return await this.sock.sendMessage(request.chatId, message, options);
  }

  sendImage(request: MessageImageRequest) {
    throw new AvailableInPlusVersion();
  }

  sendFile(request: MessageFileRequest) {
    throw new AvailableInPlusVersion();
  }

  sendVoice(request: MessageVoiceRequest) {
    throw new AvailableInPlusVersion();
  }

  protected async uploadMedia(
    file: RemoteFile | BinaryFile,
    type,
  ): Promise<any> {
    if (file && ('url' in file || 'data' in file)) {
      throw new AvailableInPlusVersion('Sending media (image, video, pdf)');
    }
    return;
  }

  async sendButtons(request: SendButtonsRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const headerImage = await this.uploadMedia(request.headerImage, 'image');
    return await sendButtonMessage(
      this.sock,
      chatId,
      request.buttons,
      request.header,
      headerImage,
      request.body,
      request.footer,
    );
  }

  sendLocation(request: MessageLocationRequest) {
    return this.sock.sendMessage(request.chatId, {
      location: {
        degreesLatitude: request.latitude,
        degreesLongitude: request.longitude,
      },
    });
  }

  async forwardMessage(request: MessageForwardRequest): Promise<WAMessage> {
    const key = parseMessageIdSerialized(request.messageId);
    const forwardMessage = await this.store.loadMessage(key.remoteJid, key.id);
    if (!forwardMessage) {
      throw new UnprocessableEntityException(
        `Message with id '${request.messageId}' not found`,
      );
    }
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const message = {
      forward: forwardMessage,
      force: true,
    };
    const result = await this.sock.sendMessage(chatId, message, {});
    return this.toWAMessage(result);
  }

  sendLinkPreview(request: MessageLinkPreviewRequest) {
    const text = `${request.title}\n${request.url}`;
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.sock.sendMessage(chatId, { text: text });
  }

  async sendSeen(request: SendSeenRequest) {
    const key = parseMessageIdSerialized(request.messageId);
    const participant = request.participant
      ? toJID(this.ensureSuffix(request.participant))
      : undefined;
    const data = {
      remoteJid: key.remoteJid,
      id: key.id,
      participant: participant,
    };
    return this.sock.readMessages([data]);
  }

  async startTyping(request: ChatRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.sock.sendPresenceUpdate('composing', chatId);
  }

  async stopTyping(request: ChatRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.sock.sendPresenceUpdate('paused', chatId);
  }

  public async getChatMessages(
    chatId: string,
    query: GetChatMessagesQuery,
    filter: GetChatMessagesFilter,
  ) {
    const downloadMedia = query.downloadMedia;
    const pagination = query as PaginationParams;
    const messages = await this.store.getMessagesByJid(
      toJID(chatId),
      filter,
      pagination,
    );

    const promises = [];
    for (const msg of messages) {
      promises.push(this.processIncomingMessage(msg, downloadMedia));
    }
    let result = await Promise.all(promises);
    result = result.filter(Boolean);
    return result;
  }

  public async getChatMessage(
    chatId: string,
    messageId: string,
    query: GetChatMessageQuery,
  ): Promise<null | WAMessage> {
    const key = parseMessageIdSerialized(messageId, true);
    const message = await this.store.getMessageById(toJID(chatId), key.id);
    if (!message) return null;
    return await this.processIncomingMessage(message, query.downloadMedia);
  }

  public async pinMessage(
    chatId: string,
    messageId: string,
    duration: PinDuration,
  ): Promise<boolean> {
    const jid = toJID(chatId);
    const key = parseMessageIdSerialized(messageId);
    await this.sock.sendMessage(jid, {
      pin: key,
      type: proto.PinInChat.Type.PIN_FOR_ALL,
      time: duration,
    });
    return true;
  }

  public async unpinMessage(
    chatId: string,
    messageId: string,
  ): Promise<boolean> {
    const jid = toJID(chatId);
    const key = parseMessageIdSerialized(messageId);
    await this.sock.sendMessage(jid, {
      pin: key,
      type: proto.PinInChat.Type.UNPIN_FOR_ALL,
    });
    return true;
  }

  async setReaction(request: MessageReactionRequest) {
    const key = parseMessageIdSerialized(request.messageId);
    const reactionMessage = {
      react: {
        text: request.reaction,
        key: key,
      },
    };
    return this.sock.sendMessage(key.remoteJid, reactionMessage);
  }

  async setStar(request: MessageStarRequest) {
    const key = parseMessageIdSerialized(request.messageId);
    await this.sock.chatModify(
      {
        star: {
          messages: [{ id: key.id, fromMe: key.fromMe }],
          star: request.star,
        },
      },
      toJID(request.chatId),
    );
  }

  /**
   * Chats methods
   */

  async getChats(pagination: PaginationParams) {
    const chats = await this.store.getChats(pagination, true);
    // Remove unreadCount, it's not ready yet
    chats.forEach((chat) => delete chat.unreadCount);
    return chats;
  }

  public async getChatsOverview(
    pagination: PaginationParams,
  ): Promise<ChatSummary[]> {
    const chats = await this.store.getChats(pagination, false);
    // Remove unreadCount, it's not ready yet
    chats.forEach((chat) => delete chat.unreadCount);

    const promises = [];
    for (const chat of chats) {
      promises.push(this.fetchChatSummary(chat));
    }
    const result = await Promise.all(promises);
    return result;
  }

  protected async fetchChatSummary(chat: Chat): Promise<ChatSummary> {
    const id = toCusFormat(chat.id);
    const name = chat.name;
    const picture = await this.getContactProfilePicture(chat.id, false);
    const messages = await this.getChatMessages(
      chat.id,
      { limit: 1, offset: 0, downloadMedia: false },
      {},
    );
    const message = messages.length > 0 ? messages[0] : null;
    return {
      id: id,
      name: name || null,
      picture: picture,
      lastMessage: message,
      _chat: chat,
    };
  }

  protected async chatsPutArchive(
    chatId: string,
    archive: boolean,
  ): Promise<any> {
    const jid = toJID(chatId);
    const messages = await this.store.getMessagesByJid(jid, {}, { limit: 1 });
    return await this.sock.chatModify(
      { archive: archive, lastMessages: messages },
      jid,
    );
  }

  public chatsArchiveChat(chatId: string): Promise<any> {
    return this.chatsPutArchive(chatId, true);
  }

  public chatsUnarchiveChat(chatId: string): Promise<any> {
    return this.chatsPutArchive(chatId, false);
  }

  public async chatsUnreadChat(chatId: string): Promise<any> {
    const jid = toJID(chatId);
    const messages = await this.store.getMessagesByJid(jid, {}, { limit: 1 });
    return await this.sock.chatModify(
      { markRead: false, lastMessages: messages },
      jid,
    );
  }

  /**
   * Labels methods
   */

  public async getLabels(): Promise<Label[]> {
    const labels = await this.store.getLabels();
    return labels.map(this.toLabel);
  }

  public async createLabel(label: LabelDTO): Promise<Label> {
    const labels = await this.store.getLabels();
    const highestLabelId = lodash.max(
      labels.map((label) => parseInt(label.id)),
    );
    const labelId = highestLabelId ? highestLabelId + 1 : 1;
    const labelAction: LabelActionBody = {
      id: labelId.toString(),
      name: label.name,
      color: label.color,
      deleted: false,
      predefinedId: undefined,
    };
    await this.sock.addLabel(undefined, labelAction);

    return {
      id: labelId.toString(),
      name: label.name,
      color: label.color,
      colorHex: Label.toHex(label.color),
    };
  }

  public async updateLabel(label: Label): Promise<Label> {
    const labelAction: LabelActionBody = {
      id: label.id,
      name: label.name,
      color: label.color,
      deleted: false,
      predefinedId: undefined,
    };
    await this.sock.addLabel(undefined, labelAction);
    return label;
  }

  public async deleteLabel(label: Label): Promise<void> {
    const labelAction: LabelActionBody = {
      id: label.id,
      name: label.name,
      color: label.color,
      deleted: true,
      predefinedId: undefined,
    };
    await this.sock.addLabel(undefined, labelAction);
  }

  public async getChatsByLabelId(labelId: string) {
    const chats = await this.store.getChatsByLabelId(labelId);
    // Remove unreadCount, it's not ready yet
    chats.forEach((chat) => delete chat.unreadCount);
    return chats;
  }

  public async getChatLabels(chatId: string): Promise<Label[]> {
    const jid = toJID(chatId);
    const labels = await this.store.getChatLabels(jid);
    return labels.map(this.toLabel);
  }

  public async putLabelsToChat(chatId: string, labels: LabelID[]) {
    const jid = toJID(chatId);
    const labelsIds = labels.map((label) => label.id);
    const currentLabels = await this.store.getChatLabels(jid);
    const currentLabelsIds = currentLabels.map((label) => label.id);
    const addLabelsIds = lodash.difference(labelsIds, currentLabelsIds);
    const removeLabelsIds = lodash.difference(currentLabelsIds, labelsIds);
    for (const labelId of addLabelsIds) {
      await this.sock.addChatLabel(jid, labelId);
    }
    for (const labelId of removeLabelsIds) {
      await this.sock.removeChatLabel(jid, labelId);
    }
  }

  protected toLabel(label: NOWEBLabel): Label {
    const color = label.color;
    return {
      id: label.id,
      name: label.name,
      color: color,
      colorHex: Label.toHex(color),
    };
  }

  private async toLabelChatAssociation(
    association: ChatLabelAssociation,
  ): Promise<LabelChatAssociation> {
    const labelData = await this.store.getLabelById(association.labelId);
    const label = labelData ? this.toLabel(labelData) : null;
    return {
      labelId: association.labelId,
      chatId: toCusFormat(association.chatId),
      label: label,
    };
  }

  /**
   * Contacts methods
   */
  async getContact(query: ContactQuery) {
    const jid = toJID(query.contactId);
    const contact = await this.store.getContactById(jid);
    if (!contact) {
      return null;
    }
    return this.toWAContact(contact);
  }

  async getContacts(pagination: PaginationParams) {
    const contacts = await this.store.getContacts(pagination);
    return contacts.map(this.toWAContact);
  }

  public async getContactAbout(query: ContactQuery) {
    throw new NotImplementedByEngineError();
  }

  public async fetchContactProfilePicture(id: string) {
    const contact = this.ensureSuffix(id);
    if (isJidNewsletter(id)) {
      return null;
    }
    if (isJidStatusBroadcast(id)) {
      return null;
    }
    const url = await this.sock.profilePictureUrl(contact, 'image');
    return url;
  }

  public async blockContact(request: ContactRequest) {
    throw new NotImplementedByEngineError();
  }

  public async unblockContact(request: ContactRequest) {
    throw new NotImplementedByEngineError();
  }

  /**
   * Group methods
   */
  public createGroup(request: CreateGroupRequest) {
    const participants = request.participants.map(getId);
    return this.sock.groupCreate(request.name, participants);
  }

  public joinGroup(code: string) {
    return this.sock.groupAcceptInvite(code);
  }

  public joinInfoGroup(code: string) {
    return this.sock.groupGetInviteInfo(code);
  }

  public async getGroups(pagination: PaginationParams) {
    const groups = await this.store.getGroups(pagination);
    // return {id: group} mapping for backward compatability
    return lodash.keyBy(groups, 'id');
  }

  public async refreshGroups(): Promise<boolean> {
    this.store.resetGroupsCache();
    await this.store.getGroups({});
    return true;
  }

  public async getGroup(id) {
    const groups = await this.getGroups({});
    return groups[id];
  }

  public async deleteGroup(id) {
    throw new NotImplementedByEngineError();
  }

  public async leaveGroup(id) {
    return this.sock.groupLeave(id);
  }

  public async setDescription(id, description) {
    return this.sock.groupUpdateDescription(id, description);
  }

  public async setSubject(id, subject) {
    return this.sock.groupUpdateSubject(id, subject);
  }

  public async getInviteCode(id): Promise<string> {
    return this.sock.groupInviteCode(id);
  }

  public async revokeInviteCode(id): Promise<string> {
    await this.sock.groupRevokeInvite(id);
    return this.sock.groupInviteCode(id);
  }

  public async getParticipants(id) {
    const groups = await this.sock.groupFetchAllParticipating();
    return groups[id].participants;
  }

  public async addParticipants(id, request: ParticipantsRequest) {
    const participants = request.participants.map(getId);
    return this.sock.groupParticipantsUpdate(id, participants, 'add');
  }

  public async removeParticipants(id, request: ParticipantsRequest) {
    const participants = request.participants.map(getId);
    return this.sock.groupParticipantsUpdate(id, participants, 'remove');
  }

  public async promoteParticipantsToAdmin(id, request: ParticipantsRequest) {
    const participants = request.participants.map(getId);
    return this.sock.groupParticipantsUpdate(id, participants, 'promote');
  }

  public async demoteParticipantsToUser(id, request: ParticipantsRequest) {
    const participants = request.participants.map(getId);
    return this.sock.groupParticipantsUpdate(id, participants, 'demote');
  }

  public async setPresence(presence: WAHAPresenceStatus, chatId?: string) {
    const enginePresence = ToEnginePresenceStatus[presence];
    if (!enginePresence) {
      throw new NotImplementedByEngineError(
        `NOWEB engine doesn't support '${presence}' presence.`,
      );
    }
    if (chatId) {
      chatId = toJID(this.ensureSuffix(chatId));
    }
    await this.sock.sendPresenceUpdate(enginePresence, chatId);
  }

  public async getPresences(): Promise<WAHAChatPresences[]> {
    const result: WAHAChatPresences[] = [];
    for (const remoteJid in this.store.presences) {
      const storedPresences = this.store.presences[remoteJid];
      result.push(this.toWahaPresences(remoteJid, storedPresences));
    }
    return result;
  }

  public async getPresence(chatId: string): Promise<WAHAChatPresences> {
    const remoteJid = toJID(chatId);
    if (!(remoteJid in this.store.presences)) {
      this.store.presences[remoteJid] = [];
      await this.sock.presenceSubscribe(remoteJid);
    }
    const result = this.store.presences[remoteJid];
    return this.toWahaPresences(remoteJid, result);
  }

  /**
   * Status methods
   */
  public async sendTextStatus(status: TextStatus) {
    const message = { text: status.text };
    const jids = await this.prepareJidsForStatus(status.contacts);
    const options = {
      backgroundColor: status.backgroundColor,
      font: status.font,
      statusJidList: jids,
    };

    return await this.sock.sendMessage(BROADCAST_ID, message, options);
  }

  protected async prepareJidsForStatus(contacts: string[]) {
    let jids: string[];
    if (contacts?.length > 0) {
      jids = contacts.map(toJID);
    } else {
      jids = await this.fetchMyContactsJids();
    }
    this.upsertMeInJIDs(jids);
    return jids;
  }

  protected async fetchMyContactsJids() {
    const contacts = await this.store.getContacts({});
    const jids = contacts.map((contact) => contact.id);
    return jids.filter((jid) => jid.endsWith('@s.whatsapp.net'));
  }

  public async deleteStatus(request: DeleteStatusRequest) {
    const messageId = request.id;
    const key = parseMessageIdSerialized(messageId, true);
    key.fromMe = true;
    key.remoteJid = BROADCAST_ID;
    const jids = await this.prepareJidsForStatus(request.contacts);
    const options = {
      statusJidList: jids,
    };
    return await this.sock.sendMessage(BROADCAST_ID, { delete: key }, options);
  }

  protected upsertMeInJIDs(jids: string[]) {
    if (!this.sock?.authState?.creds?.me) {
      return;
    }
    const myJID = jidNormalizedUser(this.sock.authState.creds.me.id);
    if (!jids.includes(myJID)) {
      jids.push(myJID);
    }
  }

  /**
   * Channels methods
   */
  protected toChannel(newsletter: NewsletterMetadata): Channel {
    const role =
      // @ts-ignore
      newsletter.viewer_metadata?.role ||
      (newsletter.viewer_metadata?.view_role as ChannelRole) ||
      ChannelRole.GUEST;
    const preview = newsletter.preview
      ? getUrlFromDirectPath(newsletter.preview)
      : null;
    const picture = newsletter.picture
      ? getUrlFromDirectPath(newsletter.picture)
      : null;
    return {
      id: newsletter.id,
      name: newsletter.name,
      description: newsletter.description,
      invite: getChannelInviteLink(newsletter.invite),
      preview: preview,
      picture: picture,
      verified: newsletter.verification === 'VERIFIED',
      role: role,
    };
  }

  public async channelsList(query: ListChannelsQuery): Promise<Channel[]> {
    const newsletters = await this.sock.newsletterSubscribed();
    let channels = newsletters.map(this.toChannel);
    if (query.role) {
      // @ts-ignore
      channels = channels.filter((channel) => channel.role === query.role);
    }
    return channels;
  }

  public async channelsCreateChannel(request: CreateChannelRequest) {
    const newsletter = await this.sock.newsletterCreate(
      request.name,
      request.description,
    );
    return this.toChannel(newsletter);
  }

  public async channelsGetChannel(id: string) {
    const newsletter = await this.sock.newsletterMetadata('jid', id);
    return this.toChannel(newsletter);
  }

  public async channelsGetChannelByInviteCode(inviteCode: string) {
    const newsletter = await this.sock.newsletterMetadata('invite', inviteCode);
    return this.toChannel(newsletter);
  }

  public async channelsDeleteChannel(id: string) {
    return await this.sock.newsletterDelete(id);
  }

  public async channelsFollowChannel(id: string): Promise<void> {
    return await this.sock.newsletterAction(id, 'follow');
  }

  public async channelsUnfollowChannel(id: string): Promise<void> {
    return await this.sock.newsletterAction(id, 'unfollow');
  }

  public async channelsMuteChannel(id: string): Promise<void> {
    return await this.sock.newsletterAction(id, 'mute');
  }

  public async channelsUnmuteChannel(id: string): Promise<void> {
    return await this.sock.newsletterAction(id, 'unmute');
  }

  subscribeEngineEvents2() {
    //
    // All
    //
    const all$ = new Observable<EnginePayload>((subscriber) => {
      return this.sock.ev.process((events) => {
        // iterate over keys
        for (const event in events) {
          const data = events[event];
          subscriber.next({ event: event, data: data });
        }
      });
    });
    this.events2.get(WAHAEvents.ENGINE_EVENT).switch(all$);

    //
    // Messages
    //
    const messagesUpsert$ = fromEvent(this.sock.ev, 'messages.upsert').pipe(
      map((event: BaileysEventMap['messages.upsert']) => event.messages),
      mergeAll(),
    );
    let [messagesFromMe$, messagesFromOthers$] = partition(
      messagesUpsert$,
      isMine,
    );
    messagesFromMe$ = messagesFromMe$.pipe(
      mergeMap((msg) => this.processIncomingMessage(msg, true)),
      share(), // share it so we don't process twice in message.any
    );
    messagesFromOthers$ = messagesFromOthers$.pipe(
      mergeMap((msg) => this.processIncomingMessage(msg, true)),
      share(), // share it so we don't process twice in message.any
    );
    const messagesFromAll$ = merge(messagesFromMe$, messagesFromOthers$);
    this.events2.get(WAHAEvents.MESSAGE).switch(messagesFromOthers$);
    this.events2.get(WAHAEvents.MESSAGE_ANY).switch(messagesFromAll$);

    //
    // Message Reactions
    //
    const messageReactions$ = messagesUpsert$.pipe(
      map(this.processMessageReaction.bind(this)),
      filter(Boolean),
    );
    this.events2.get(WAHAEvents.MESSAGE_REACTION).switch(messageReactions$);

    //
    // Message Ack
    //
    const messageUpdates$: Observable<WAMessageUpdate> = fromEvent(
      this.sock.ev,
      'messages.update',
    ).pipe(
      // @ts-ignore
      mergeAll(),
    );
    const messageAckDirect$ = messageUpdates$.pipe(
      filter(isMine), // ack comes only for MY messages
      filter(isAckUpdateMessageEvent),
      map(this.convertMessageUpdateToMessageAck.bind(this)),
    );
    const messageReceiptUpdate$: Observable<MessageUserReceiptUpdate> =
      fromEvent(this.sock.ev, 'message-receipt.update').pipe(
        // @ts-ignore
        mergeAll(),
      );

    const messageAckGroups$ = messageReceiptUpdate$.pipe(
      filter(isMine), // ack comes only for MY messages
      map(this.convertMessageReceiptUpdateToMessageAck.bind(this)),
    );
    const messageAck$ = merge(messageAckDirect$, messageAckGroups$);
    this.events2.get(WAHAEvents.MESSAGE_ACK).switch(messageAck$);

    //
    // Other
    //
    this.events2
      .get(WAHAEvents.STATE_CHANGE)
      .switch(fromEvent(this.sock.ev, 'connection.update'));
    this.events2.get(WAHAEvents.GROUP_JOIN).switch(
      // @ts-ignore
      fromEvent<GroupMetadata[]>(this.sock.ev, 'groups.upsert').pipe(
        mergeAll(),
      ),
    );
    this.events2
      .get(WAHAEvents.PRESENCE_UPDATE)
      .switch(
        fromEvent(this.sock.ev, 'presence.update').pipe(
          map((data: any) => this.toWahaPresences(data.id, data.presences)),
        ),
      );

    //
    // Poll votes
    //
    this.events2
      .get(WAHAEvents.POLL_VOTE)
      .switch(
        messageUpdates$.pipe(
          mergeMap(this.handleMessagesUpdatePollVote.bind(this)),
          filter(Boolean),
        ),
      );
    this.events2
      .get(WAHAEvents.POLL_VOTE_FAILED)
      .switch(
        messagesUpsert$.pipe(
          mergeMap(this.handleMessageUpsertPollVoteFailed.bind(this)),
          filter(Boolean),
        ),
      );

    //
    // Calls
    //
    // @ts-ignore
    const calls$: Observable<WACallEvent[]> = fromEvent(this.sock.ev, 'call');
    const call$ = calls$.pipe(mergeMap(identity));
    this.events2.get(WAHAEvents.CALL_RECEIVED).switch(
      call$.pipe(
        filter((call: WACallEvent) => call.status === 'offer'),
        map(this.toCallData.bind(this)),
      ),
    );
    this.events2.get(WAHAEvents.CALL_ACCEPTED).switch(
      call$.pipe(
        filter((call: WACallEvent) => call.status === 'accept'),
        map(this.toCallData.bind(this)),
      ),
    );
    this.events2.get(WAHAEvents.CALL_REJECTED).switch(
      calls$.pipe(
        // Filter out if there's any "accept" events.
        // Meaning it's been accepted on one device, but rejected on another
        exclude((calls) => calls.some((call) => call.status === 'accept')),
        mergeAll(),
        filter((call: WACallEvent) => call.status === 'reject'),
        // We get two "reject" events, one with null isGroup property, ignore it
        exclude((call: WACallEvent) => call.isGroup == null),
        map(this.toCallData.bind(this)),
      ),
    );

    //
    // Labels
    //
    // @ts-ignore
    const labelsEdit$: Observable<NOWEBLabel> = fromEvent(
      this.sock.ev,
      'labels.edit',
    );
    this.events2.get(WAHAEvents.LABEL_UPSERT).switch(
      labelsEdit$.pipe(
        exclude((data: NOWEBLabel) => data.deleted),
        map(this.toLabel.bind(this)),
      ),
    );
    this.events2.get(WAHAEvents.LABEL_DELETED).switch(
      labelsEdit$.pipe(
        filter((data: NOWEBLabel) => data.deleted),
        map(this.toLabel.bind(this)),
      ),
    );
    const labelsAssociation$ = fromEvent(this.sock.ev, 'labels.association');
    const labelsAssociationAdd$: Observable<ChatLabelAssociation> =
      labelsAssociation$.pipe(
        filter(({ type }: any) => type === 'add'),
        map((data) => data.association),
        filter(
          (association: any) => association.type === LabelAssociationType.Chat,
        ),
      );

    const labelsAssociationRemove$: Observable<ChatLabelAssociation> =
      labelsAssociation$.pipe(
        filter(({ type }: any) => type === 'remove'),
        map((data) => data.association),
        filter(
          (association: any) => association.type === LabelAssociationType.Chat,
        ),
      );
    this.events2
      .get(WAHAEvents.LABEL_CHAT_ADDED)
      .switch(
        labelsAssociationAdd$.pipe(
          mergeMap(this.toLabelChatAssociation.bind(this)),
        ),
      );
    this.events2
      .get(WAHAEvents.LABEL_CHAT_DELETED)
      .switch(
        labelsAssociationRemove$.pipe(
          mergeMap(this.toLabelChatAssociation.bind(this)),
        ),
      );
  }

  protected listenContactsUpdatePictureProfile() {
    this.sock.ev.on('contacts.update', async (updates) => {
      for (const update of updates) {
        if (update.imgUrl !== 'changed') {
          continue;
        }

        this.logger.debug({ jid: update.id }, 'Profile picture updated');
        const url = await this.refreshProfilePicture(update.id);
        if (isJidUser(update.id)) {
          // update 123@c.us and 123 profiles as well
          const cus = toCusFormat(update.id);
          this.profilePictures.set(cus, url);
          const phone = update.id.split('@')[0];
          this.profilePictures.set(phone, url);
        }
      }
    });
  }

  /**
   * END - Methods for API
   */

  private processMessageReaction(message): WAMessageReaction | null {
    if (!message) return null;
    if (!message.message) return null;
    if (!message.message.reactionMessage) return null;

    const id = buildMessageId(message.key);
    const fromToParticipant = getFromToParticipant(message);
    const reactionMessage = message.message.reactionMessage;
    const messageId = buildMessageId(reactionMessage.key);
    const reaction: WAMessageReaction = {
      id: id,
      timestamp: ensureNumber(message.messageTimestamp),
      from: toCusFormat(fromToParticipant.from),
      fromMe: message.key.fromMe,
      to: toCusFormat(fromToParticipant.to),
      participant: toCusFormat(fromToParticipant.participant),
      reaction: {
        text: reactionMessage.text,
        messageId: messageId,
      },
    };
    return reaction;
  }

  private async processIncomingMessage(message, downloadMedia = true) {
    // if there is no text or media message
    if (!message) return;
    if (!message.message) return;
    // Ignore reactions, we have dedicated handler for that
    if (message.message.reactionMessage) return;
    // Ignore poll votes, we have dedicated handler for that
    if (message.message.pollUpdateMessage) return;

    if (downloadMedia) {
      try {
        message = await this.downloadMedia(message);
      } catch (e) {
        this.logger.error('Failed when tried to download media for a message');
        this.logger.error(e, e.stack);
      }
    }

    try {
      return await this.toWAMessage(message);
    } catch (error) {
      this.logger.error('Failed to process incoming message');
      this.logger.error(error);
      console.trace(error);
      return null;
    }
  }

  protected toWAMessage(message): Promise<WAMessage> {
    const fromToParticipant = getFromToParticipant(message);
    const id = buildMessageId(message.key);
    const body = this.extractBody(message.message);
    const replyTo = this.extractReplyTo(message.message);
    const ack = message.ack || message.status - 1;
    return Promise.resolve({
      id: id,
      timestamp: ensureNumber(message.messageTimestamp),
      from: toCusFormat(fromToParticipant.from),
      fromMe: message.key.fromMe,
      body: body,
      to: toCusFormat(fromToParticipant.to),
      participant: toCusFormat(fromToParticipant.participant),
      // Media
      hasMedia: Boolean(message.media),
      media: message.media,
      mediaUrl: message.media?.url,
      // @ts-ignore
      ack: ack,
      // @ts-ignore
      ackName: WAMessageAck[ack] || ACK_UNKNOWN,
      location: message.location,
      vCards: message.vCards,
      replyTo: replyTo,
      _data: message,
    });
  }

  protected extractBody(message): string | null {
    if (!message) {
      return null;
    }
    let body = message.conversation;
    if (!body) {
      // Some of the messages have no conversation, but instead have text in extendedTextMessage
      // https://github.com/devlikeapro/waha/issues/90
      body = message.extendedTextMessage?.text;
    }
    if (!body) {
      // Populate from caption
      const mediaContent = extractMediaContent(message);
      // @ts-ignore - AudioMessage doesn't have caption field
      body = mediaContent?.caption;
    }
    // Response for buttons
    if (!body) {
      body = message.templateButtonReplyMessage?.selectedDisplayText;
    }
    return body;
  }

  protected extractReplyTo(message): ReplyToMessage | null {
    const msgType = getContentType(message);
    const contextInfo = message[msgType]?.contextInfo;
    if (!contextInfo) {
      return null;
    }
    const quotedMessage = contextInfo.quotedMessage;
    if (!quotedMessage) {
      return null;
    }
    const body = this.extractBody(quotedMessage);
    return {
      id: contextInfo.stanzaId,
      participant: toCusFormat(contextInfo.participant),
      body: body,
    };
  }

  protected toWAContact(contact: Contact) {
    contact.id = toCusFormat(contact.id);
    // @ts-ignore
    contact.pushname = contact.notify;
    // @ts-ignore
    delete contact.notify;
    return contact;
  }

  protected convertMessageUpdateToMessageAck(event): WAMessageAckBody {
    const message = event;
    const fromToParticipant = getFromToParticipant(message);
    const id = buildMessageId(message.key);
    const ack = message.update.status - 1;
    const body: WAMessageAckBody = {
      id: id,
      from: toCusFormat(fromToParticipant.from),
      to: toCusFormat(fromToParticipant.to),
      participant: toCusFormat(fromToParticipant.participant),
      fromMe: message.key.fromMe,
      ack: ack,
      ackName: WAMessageAck[ack] || ACK_UNKNOWN,
    };
    return body;
  }

  protected convertMessageReceiptUpdateToMessageAck(event): WAMessageAckBody {
    const fromToParticipant = getFromToParticipant(event);
    const id = buildMessageId(event.key);

    const receipt = event.receipt;
    let ack;
    if (receipt.receiptTimestamp) {
      ack = WAMessageAck.SERVER;
    } else if (receipt.playedTimestamp) {
      ack = WAMessageAck.PLAYED;
    } else if (receipt.readTimestamp) {
      ack = WAMessageAck.READ;
    }
    const body: WAMessageAckBody = {
      id: id,
      from: toCusFormat(fromToParticipant.from),
      to: toCusFormat(fromToParticipant.to),
      participant: toCusFormat(fromToParticipant.participant),
      fromMe: event.key.fromMe,
      ack: ack,
      ackName: WAMessageAck[ack] || ACK_UNKNOWN,
    };
    return body;
  }

  protected async handleMessagesUpdatePollVote(event) {
    const { key, update } = event;
    const pollUpdates = update?.pollUpdates;
    if (!pollUpdates) {
      return;
    }

    const pollCreationMessageKey = key;
    const pollCreationMessage = await this.getMessage(key);
    // Handle updates one by one, so we can get Vote Message for the specific vote
    for (const pollUpdate of pollUpdates) {
      const votes = getAggregateVotesInPollMessage({
        message: pollCreationMessage,
        pollUpdates: [pollUpdate],
      });

      // Get selected options for the author
      const selectedOptions = [];
      for (const voteAggregation of votes) {
        for (const voter of voteAggregation.voters) {
          if (voter === getKeyAuthor(pollUpdate.pollUpdateMessageKey)) {
            selectedOptions.push(voteAggregation.name);
          }
        }
      }

      // Build payload and call the handler
      const voteDestination = getDestination(pollUpdate.pollUpdateMessageKey);
      const pollVote: PollVote = {
        ...voteDestination,
        selectedOptions: selectedOptions,
        timestamp: ensureNumber(pollUpdate.senderTimestampMs),
      };
      const payload: PollVotePayload = {
        vote: pollVote,
        poll: getDestination(pollCreationMessageKey),
      };
      return payload;
    }
  }

  protected async handleMessageUpsertPollVoteFailed(message) {
    const pollUpdateMessage = message.message?.pollUpdateMessage;
    if (!pollUpdateMessage) {
      return;
    }
    const pollCreationMessageKey = pollUpdateMessage.pollCreationMessageKey;
    const pollCreationMessage = await this.getMessage(pollCreationMessageKey);
    if (pollCreationMessage) {
      // We found message, so later the engine will issue a message.update message
      return;
    }

    // We didn't find the creation message, so send failed one
    const pollUpdateMessageKey = message.key;
    const voteDestination = getDestination(pollUpdateMessageKey);
    const pollVote: PollVote = {
      ...voteDestination,
      selectedOptions: [],
      // change to below line when the PR merged, so we have the same timestamps
      // https://github.com/WhiskeySockets/Baileys/pull/348
      // Or without toNumber() - it depends on the PR above
      // timestamp: pollUpdateMessage.senderTimestampMs.toNumber()
      timestamp: ensureNumber(message.messageTimestamp),
    };
    const payload: PollVotePayload = {
      vote: pollVote,
      poll: getDestination(pollCreationMessageKey),
    };
    return payload;
  }

  private toCallData(call: WACallEvent): CallData {
    // call.date can be either string 2024-07-18T09:45:55.000Z or Date
    const date = new Date(call.date);
    // convert to timestamp in seconds
    const timestamp: number = date.getTime() / 1000;
    return {
      id: call.id,
      from: toCusFormat(call.from),
      timestamp: timestamp,
      isVideo: call.isVideo,
      isGroup: call.isGroup,
    };
  }

  private toWahaPresences(
    remoteJid: string,
    storedPresences: { [participant: string]: PresenceData },
  ): WAHAChatPresences {
    const presences: WAHAPresenceData[] = [];
    for (const participant in storedPresences) {
      const data: PresenceData = storedPresences[participant];
      const lastKnownPresence = lodash.get(
        PresenceStatuses,
        data.lastKnownPresence,
        data.lastKnownPresence,
      );
      const presence: WAHAPresenceData = {
        participant: toCusFormat(participant),
        // @ts-ignore
        lastKnownPresence: lastKnownPresence,
        lastSeen: data.lastSeen || null,
      };
      presences.push(presence);
    }
    const chatId = toCusFormat(remoteJid);
    return { id: chatId, presences: presences };
  }

  protected downloadMedia(message) {
    const processor = new NOWEBEngineMediaProcessor(this, this.loggerBuilder);
    return this.mediaManager.processMedia(processor, message, this.name);
  }

  protected async getMessageOptions(request: any): Promise<any> {
    let quoted;
    if (request.reply_to) {
      const key = parseMessageIdSerialized(request.reply_to, true);
      quoted = await this.store.loadMessage(toJID(request.chatId), key.id);
    }

    return {
      quoted: quoted,
    };
  }

  protected getLinkPreview(request): any {
    // NOWEB works this way
    // If it's undefined - it'll generate it
    // If it's false - it will not generate it
    let linkPreview: boolean | undefined;
    switch (request.linkPreview) {
      case false:
        linkPreview = false;
        break;
      case true:
      default:
        linkPreview = undefined;
    }
    return linkPreview;
  }
}

function hasPath(url: string) {
  if (!url) {
    return false;
  }
  try {
    const urlObj = new URL(url);
    return urlObj.pathname !== '/';
  } catch (error) {
    return false;
  }
}

export class NOWEBEngineMediaProcessor implements IMediaEngineProcessor<any> {
  private readonly logger: BaileysLogger;

  constructor(
    public session: WhatsappSessionNoWebCore,
    loggerBuilder: LoggerBuilder,
  ) {
    this.logger = loggerBuilder.child({
      name: NOWEBEngineMediaProcessor.name,
    }) as unknown as BaileysLogger;
  }

  hasMedia(message: any): boolean {
    return Boolean(extractMediaContent(message.message));
  }

  getMessageId(message: any): string {
    return message.key.id;
  }

  getMimetype(message: any): string {
    const content = extractMediaContent(message.message);
    return content.mimetype;
  }

  async getMediaBuffer(message: any): Promise<Buffer | null> {
    const content = extractMediaContent(message.message);
    // Fix Stickers
    // https://github.com/devlikeapro/waha/issues/504
    const url = content.url;
    if (!hasPath(url)) {
      // Set it to null so the engine handles it right
      content.url = null;
    }

    return (await downloadMediaMessage(
      message,
      'buffer',
      {},
      {
        logger: this.logger,
        reuploadRequest: this.session.sock.updateMediaMessage,
      },
    ).finally(() => {
      // Fix Stickers - set url back, just to have it in the response
      // https://github.com/devlikeapro/waha/issues/504
      content.url = url;
    })) as Buffer;
  }

  getFilename(message: any): string | null {
    const content = extractMessageContent(message.message);
    return content?.documentMessage?.fileName || null;
  }
}

/**
 * Convert from 11111111111@s.whatsapp.net to 11111111111@c.us
 */
function toCusFormat(remoteJid) {
  if (!remoteJid) {
    return remoteJid;
  }
  if (isJidGroup(remoteJid)) {
    return remoteJid;
  }
  if (isJidStatusBroadcast(remoteJid)) {
    return remoteJid;
  }
  if (isLidUser(remoteJid)) {
    return remoteJid;
  }
  if (isNewsletter(remoteJid)) {
    return remoteJid;
  }
  if (!remoteJid) {
    return;
  }
  if (remoteJid == 'me') {
    return remoteJid;
  }
  const number = remoteJid.split('@')[0];
  return ensureSuffix(number);
}

export const ALL_JID = 'all@s.whatsapp.net';

/**
 * Convert from 11111111111@c.us to 11111111111@s.whatsapp.net
 * @param chatId
 */
export function toJID(chatId) {
  if (isJidGroup(chatId)) {
    return chatId;
  }
  if (isJidStatusBroadcast(chatId)) {
    return chatId;
  }
  if (isNewsletter(chatId)) {
    return chatId;
  }
  const number = chatId.split('@')[0];
  return number + '@s.whatsapp.net';
}

/**
 * Build WAHA message id from engine one
 * {id: "AAA", remoteJid: "11111111111@s.whatsapp.net", "fromMe": false}
 * false_11111111111@c.us_AA
 */
function buildMessageId({ id, remoteJid, fromMe, participant }: WAMessageKey) {
  const chatId = toCusFormat(remoteJid);
  const parts = [fromMe || false, chatId, id];
  if (participant) {
    parts.push(toCusFormat(participant));
  }
  return parts.join('_');
}

/**
 * Parse message id from WAHA to engine
 * false_11111111111@c.us_AAA
 * {id: "AAA", remoteJid: "11111111111@s.whatsapp.net", "fromMe": false}
 */
function parseMessageIdSerialized(
  messageId: string,
  soft: boolean = false,
): WAMessageKey {
  if (!messageId.includes('_') && soft) {
    return { id: messageId };
  }

  const parts = messageId.split('_');
  if (parts.length != 3 && parts.length != 4) {
    throw new Error(
      'Message id be in format false_11111111111@c.us_AAAAAAAAAAAAAAAAAAAA[_participant]',
    );
  }
  const fromMe = parts[0] == 'true';
  const chatId = parts[1];
  const remoteJid = toJID(chatId);
  const id = parts[2];
  const participant = parts[3] ? toJID(parts[3]) : undefined;
  return {
    fromMe: fromMe,
    id: id,
    remoteJid: remoteJid,
    participant: participant,
  };
}

function getId(object) {
  return object.id;
}

function isMine(message) {
  return message?.key?.fromMe;
}

function isNotMine(message) {
  return !message?.key?.fromMe;
}

function isAckUpdateMessageEvent(event) {
  return event?.update.status != null;
}

function getFromToParticipant(message) {
  const isGroupMessage = Boolean(message.key.participant);
  let participant: string;
  let to: string;
  if (isGroupMessage) {
    participant = message.key.participant;
    to = message.key.remoteJid;
  }
  const from = message.key.remoteJid;
  return {
    from: from,
    to: to,
    participant: participant,
  };
}

function getTo(key, meId = undefined) {
  // For group - always to group JID
  const isGroupMessage = Boolean(key.participant);
  if (isGroupMessage) {
    return key.remoteJid;
  }
  if (key.fromMe) {
    return key.remoteJid;
  }
  return meId || 'me';
}

function getFrom(key, meId) {
  // For group - always from participant
  const isGroupMessage = Boolean(key.participant);
  if (isGroupMessage) {
    return key.participant;
  }
  if (key.fromMe) {
    return meId || 'me';
  }
  return key.remoteJid;
}

function getDestination(key, meId = undefined): MessageDestination {
  return {
    id: buildMessageId(key),
    to: toCusFormat(getTo(key, meId)),
    from: toCusFormat(getFrom(key, meId)),
    fromMe: key.fromMe,
  };
}
