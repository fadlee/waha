import {
  Channel,
  CreateChannelRequest,
  ListChannelsQuery,
} from '@waha/structures/channels.dto';
import {
  ChatSummary,
  GetChatMessageQuery,
  GetChatMessagesFilter,
  GetChatMessagesQuery,
} from '@waha/structures/chats.dto';
import { SendButtonsRequest } from '@waha/structures/chatting.buttons.dto';
import { Label, LabelDTO, LabelID } from '@waha/structures/labels.dto';
import { PaginationParams } from '@waha/structures/pagination.dto';
import { WAMessage } from '@waha/structures/responses.dto';
import { DefaultMap } from '@waha/utils/DefaultMap';
import { generatePrefixedId } from '@waha/utils/ids';
import { LoggerBuilder } from '@waha/utils/logging';
import { complete } from '@waha/utils/reactive/complete';
import { SwitchObservable } from '@waha/utils/reactive/SwitchObservable';
import * as fs from 'fs';
import * as lodash from 'lodash';
import * as NodeCache from 'node-cache';
import { Logger } from 'pino';
import {
  BehaviorSubject,
  catchError,
  delay,
  filter,
  of,
  retry,
  share,
  Subject,
  switchMap,
} from 'rxjs';
import { distinctUntilChanged, map } from 'rxjs/operators';
import { MessageId } from 'whatsapp-web.js';

import {
  ChatRequest,
  CheckNumberStatusQuery,
  EditMessageRequest,
  MessageContactVcardRequest,
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
  MessageVideoRequest,
  MessageVoiceRequest,
} from '../../structures/chatting.dto';
import { ContactQuery, ContactRequest } from '../../structures/contacts.dto';
import {
  WAHAEngine,
  WAHAEvents,
  WAHAPresenceStatus,
  WAHASessionStatus,
} from '../../structures/enums.dto';
import {
  CreateGroupRequest,
  GroupsPaginationParams,
  ParticipantsRequest,
  SettingsSecurityChangeInfo,
} from '../../structures/groups.dto';
import { WAHAChatPresences } from '../../structures/presence.dto';
import {
  MeInfo,
  ProxyConfig,
  SessionConfig,
} from '../../structures/sessions.dto';
import {
  DeleteStatusRequest,
  ImageStatus,
  TextStatus,
  VideoStatus,
  VoiceStatus,
} from '../../structures/status.dto';
import { WASessionStatusBody } from '../../structures/webhooks.dto';
import { NotImplementedByEngineError } from '../exceptions';
import { IMediaManager } from '../media/IMediaManager';
import { QR } from '../QR';
import { DataStore } from './DataStore';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const qrcode = require('qrcode-terminal');

const CHROME_PATH = '/usr/bin/google-chrome-stable';
const CHROMIUM_PATH = '/usr/bin/chromium';

export function getBrowserExecutablePath() {
  if (fs.existsSync(CHROME_PATH)) {
    return CHROME_PATH;
  }
  return CHROMIUM_PATH;
}

export function ensureSuffix(phone) {
  const suffix = '@c.us';
  if (phone.includes('@')) {
    return phone;
  }
  return phone + suffix;
}

export interface SessionParams {
  name: string;
  printQR: boolean;
  mediaManager: IMediaManager;
  loggerBuilder: LoggerBuilder;
  sessionStore: DataStore;
  proxyConfig?: ProxyConfig;
  sessionConfig?: SessionConfig;
  engineConfig?: any;
}

export abstract class WhatsappSession {
  public engine: WAHAEngine;

  public name: string;
  protected mediaManager: IMediaManager;
  public loggerBuilder: LoggerBuilder;
  protected logger: Logger;
  protected sessionStore: DataStore;
  protected proxyConfig?: ProxyConfig;
  public sessionConfig?: SessionConfig;
  protected engineConfig?: any;
  protected unpairing: boolean = false;

  private _status: WAHASessionStatus;
  private shouldPrintQR: boolean;
  protected events2: DefaultMap<WAHAEvents, SwitchObservable<any>>;
  private status$: Subject<WAHASessionStatus>;
  protected profilePictures: NodeCache = new NodeCache({
    stdTTL: 24 * 60 * 60, // 1 day
  });

  public constructor({
    name,
    printQR,
    loggerBuilder,
    sessionStore,
    proxyConfig,
    mediaManager,
    sessionConfig,
    engineConfig,
  }: SessionParams) {
    this.status$ = new BehaviorSubject(null);

    this.name = name;
    this.proxyConfig = proxyConfig;
    this.loggerBuilder = loggerBuilder;
    this.logger = loggerBuilder.child({ name: 'WhatsappSession' });
    this.events2 = new DefaultMap<WAHAEvents, SwitchObservable<any>>(
      (key) =>
        new SwitchObservable((obs$) => {
          return obs$.pipe(
            catchError((err) => {
              this.logger.error(
                `Caught error, dropping value from, event: '${key}'`,
              );
              this.logger.error(err, err.stack);
              throw err;
            }),
            filter(Boolean),
            map((data) => {
              data._eventId = generatePrefixedId('evt');
              return data;
            }),
            retry(),
            share(),
          );
        }),
    );

    this.events2.get(WAHAEvents.SESSION_STATUS).switch(
      this.status$
        // initial value is null
        .pipe(filter(Boolean))
        // Wait for WORKING status to get all the info
        // https://github.com/devlikeapro/waha/issues/409
        .pipe(
          switchMap((status: WAHASessionStatus) => {
            const me = this.getSessionMeInfo();
            const hasMe = !!me?.pushName && !!me?.id;
            // Delay WORKING by 1 second if condition is met
            // Usually we get WORKING with all the info after
            if (status === WAHASessionStatus.WORKING && !hasMe) {
              return of(status).pipe(delay(2000));
            }
            return of(status);
          }),
          // Remove consecutive duplicate WORKING statuses
          distinctUntilChanged(
            (prev, curr) => prev === curr && curr === WAHASessionStatus.WORKING,
          ),
        )
        // Populate the session info
        .pipe(
          map<WAHASessionStatus, WASessionStatusBody>((status) => {
            return { name: this.name, status: status };
          }),
        ),
    );

    this.sessionStore = sessionStore;
    this.mediaManager = mediaManager;
    this.sessionConfig = sessionConfig;
    this.engineConfig = engineConfig;
    this.shouldPrintQR = printQR;
  }

  public getEventObservable(event: WAHAEvents) {
    return this.events2.get(event);
  }

  protected set status(value: WAHASessionStatus) {
    if (this.unpairing && value !== WAHASessionStatus.STOPPED) {
      // In case of unpairing
      // wait for STOPPED event, ignore the rest
      return;
    }
    this._status = value;
    this.status$.next(value);
  }

  public get status() {
    return this._status;
  }

  getBrowserExecutablePath() {
    return getBrowserExecutablePath();
  }

  getBrowserArgsForPuppeteer() {
    // Run optimized version of Chrome
    // References:
    // https://github.com/pedroslopez/whatsapp-web.js/issues/1420
    // https://github.com/wppconnect-team/wppconnect/issues/1326
    // https://www.bannerbear.com/blog/ways-to-speed-up-puppeteer-screenshots/
    return [
      '--aggressive-cache-discard',
      '--disable-accelerated-2d-canvas',
      '--disable-application-cache',
      '--disable-background-networking',
      // DO NOT disable software rasterizer, it will break the video
      // https://github.com/devlikeapro/waha/issues/629
      // '--disable-software-rasterizer',
      '--disable-cache',
      '--disable-client-side-phishing-detection',
      '--disable-component-update',
      '--disable-default-apps',
      '--disable-dev-shm-usage',
      '--disable-extensions',
      '--disable-features=site-per-process',
      '--disable-gpu',
      '--disable-offer-store-unmasked-wallet-cards',
      '--disable-offline-load-stale-cache',
      '--disable-popup-blocking',
      '--disable-setuid-sandbox',
      '--disable-site-isolation-trials', // https://superuser.com/questions/654565/how-to-run-google-chrome-in-a-single-process
      '--disable-speech-api',
      '--disable-sync',
      '--disable-translate',
      '--disable-web-security',
      '--disk-cache-size=0',
      '--hide-scrollbars',
      '--ignore-certificate-errors',
      '--ignore-ssl-errors',
      '--in-process-gpu', // https://superuser.com/questions/654565/how-to-run-google-chrome-in-a-single-process
      '--metrics-recording-only',
      '--mute-audio',
      '--no-default-browser-check',
      '--no-first-run',
      '--no-pings',
      '--no-sandbox',
      '--no-zygote',
      '--password-store=basic',
      '--renderer-process-limit=2', // https://superuser.com/questions/654565/how-to-run-google-chrome-in-a-single-process
      '--safebrowsing-disable-auto-update',
      // '--single-process',
      '--use-mock-keychain',
      '--window-size=1280,720',
    ];
  }

  protected isDebugEnabled() {
    return this.logger.isLevelEnabled('debug');
  }

  /** Start the session */
  abstract start();

  /** Stop the session */
  abstract stop(): Promise<void>;

  protected stopEvents() {
    complete(this.events2);
  }

  /* Unpair the account */
  async unpair(): Promise<void> {
    return;
  }

  /**
   * START - Methods for API
   */

  /**
   * Auth methods
   */

  public getQR(): QR {
    throw new NotImplementedByEngineError();
  }

  public requestCode(phoneNumber: string, method: string, params?: any) {
    throw new NotImplementedByEngineError();
  }

  public authorizeCode(code: string) {
    throw new NotImplementedByEngineError();
  }

  abstract getScreenshot(): Promise<Buffer>;

  public getSessionMeInfo(): MeInfo | null {
    return null;
  }

  public getCaptcha(): Promise<QR> {
    throw new NotImplementedByEngineError();
  }

  public saveCaptcha(code: string) {
    throw new NotImplementedByEngineError();
  }

  /**
   * Other methods
   */
  abstract checkNumberStatus(request: CheckNumberStatusQuery);

  abstract sendText(request: MessageTextRequest);

  sendContactVCard(request: MessageContactVcardRequest) {
    throw new NotImplementedByEngineError();
  }

  sendPoll(request: MessagePollRequest) {
    throw new NotImplementedByEngineError();
  }

  abstract sendLocation(request: MessageLocationRequest);

  sendLinkPreview(request: MessageLinkPreviewRequest) {
    throw new NotImplementedByEngineError();
  }

  abstract forwardMessage(request: MessageForwardRequest): Promise<WAMessage>;

  abstract sendImage(request: MessageImageRequest);

  abstract sendFile(request: MessageFileRequest);

  abstract sendVoice(request: MessageVoiceRequest);

  sendVideo(request: MessageVideoRequest) {
    throw new NotImplementedByEngineError();
  }

  sendButtons(request: SendButtonsRequest) {
    throw new NotImplementedByEngineError();
  }

  abstract reply(request: MessageReplyRequest);

  abstract sendSeen(chat: ChatRequest);

  abstract startTyping(chat: ChatRequest);

  abstract stopTyping(chat: ChatRequest);

  abstract setReaction(request: MessageReactionRequest);

  setStar(request: MessageStarRequest): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  /**
   * Chats methods
   */
  public getChats(pagination: PaginationParams) {
    throw new NotImplementedByEngineError();
  }

  public getChatsOverview(
    pagination: PaginationParams,
  ): Promise<ChatSummary[]> {
    throw new NotImplementedByEngineError();
  }

  public deleteChat(chatId) {
    throw new NotImplementedByEngineError();
  }

  public getChatMessages(
    chatId: string,
    query: GetChatMessagesQuery,
    filter: GetChatMessagesFilter,
  ) {
    throw new NotImplementedByEngineError();
  }

  public getChatMessage(
    chatId: string,
    messageId: string,
    query: GetChatMessageQuery,
  ): Promise<null | WAMessage> {
    throw new NotImplementedByEngineError();
  }

  public pinMessage(
    chatId: string,
    messageId: string,
    duration: number,
  ): Promise<boolean> {
    throw new NotImplementedByEngineError();
  }

  public unpinMessage(chatId: string, messageId: string): Promise<boolean> {
    throw new NotImplementedByEngineError();
  }

  public deleteMessage(chatId: string, messageId: string) {
    throw new NotImplementedByEngineError();
  }

  public editMessage(
    chatId: string,
    messageId: string,
    request: EditMessageRequest,
  ) {
    throw new NotImplementedByEngineError();
  }

  public clearMessages(chatId) {
    throw new NotImplementedByEngineError();
  }

  public chatsArchiveChat(chatId: string): Promise<any> {
    throw new NotImplementedByEngineError();
  }

  public chatsUnarchiveChat(chatId: string): Promise<any> {
    throw new NotImplementedByEngineError();
  }

  public chatsUnreadChat(chatId: string): Promise<any> {
    throw new NotImplementedByEngineError();
  }

  /**
   * Labels methods
   */
  public async getLabel(labelId: string): Promise<Label | undefined> {
    const labels = await this.getLabels();
    return lodash.find(labels, { id: labelId });
  }

  public getLabels(): Promise<Label[]> {
    throw new NotImplementedByEngineError();
  }

  public async createLabel(label: LabelDTO): Promise<Label> {
    throw new NotImplementedByEngineError();
  }

  public async updateLabel(label: Label): Promise<Label> {
    throw new NotImplementedByEngineError();
  }

  public async deleteLabel(label: Label): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  public getChatsByLabelId(labelId: string) {
    throw new NotImplementedByEngineError();
  }

  public getChatLabels(chatId: string): Promise<Label[]> {
    throw new NotImplementedByEngineError();
  }

  public putLabelsToChat(chatId: string, labels: LabelID[]) {
    throw new NotImplementedByEngineError();
  }

  /**
   * Contacts methods
   */
  public getContact(query: ContactQuery) {
    throw new NotImplementedByEngineError();
  }

  public getContacts(pagination: PaginationParams) {
    throw new NotImplementedByEngineError();
  }

  public getContactAbout(query: ContactQuery) {
    throw new NotImplementedByEngineError();
  }

  /**
   * Fetch the latest profile picture of the contact (group, newsletter, etc.)
   * @param id
   */
  abstract fetchContactProfilePicture(id: string): Promise<string | null>;

  public async getContactProfilePicture(
    id: string,
    refresh: boolean,
  ): Promise<string | null> {
    const has: boolean = this.profilePictures.has(id);
    if (!has || refresh) {
      await this.refreshProfilePicture(id);
    }
    return this.profilePictures.get(id);
  }

  protected async refreshProfilePicture(id: string) {
    this.profilePictures.del(id);
    const url = await this.fetchContactProfilePicture(id).catch((err) => {
      this.logger.error('Error fetching profile picture');
      this.logger.error(err, err.stack);
      return null;
    });
    this.profilePictures.set(id, url);
    return url;
  }

  public blockContact(request: ContactRequest) {
    throw new NotImplementedByEngineError();
  }

  public unblockContact(request: ContactRequest) {
    throw new NotImplementedByEngineError();
  }

  /**
   * Group methods
   */
  public createGroup(request: CreateGroupRequest) {
    throw new NotImplementedByEngineError();
  }

  public joinGroup(code: string): Promise<string> {
    throw new NotImplementedByEngineError();
  }

  public joinInfoGroup(code: string): Promise<any> {
    throw new NotImplementedByEngineError();
  }

  public getGroups(pagination: PaginationParams) {
    throw new NotImplementedByEngineError();
  }

  public refreshGroups(): Promise<boolean> {
    throw new NotImplementedByEngineError();
  }

  public getGroup(id) {
    throw new NotImplementedByEngineError();
  }

  public getInfoAdminsOnly(id): Promise<SettingsSecurityChangeInfo> {
    throw new NotImplementedByEngineError();
  }

  public setInfoAdminsOnly(id, value) {
    throw new NotImplementedByEngineError();
  }

  public getMessagesAdminsOnly(id): Promise<SettingsSecurityChangeInfo> {
    throw new NotImplementedByEngineError();
  }

  public setMessagesAdminsOnly(id, value) {
    throw new NotImplementedByEngineError();
  }

  public deleteGroup(id) {
    throw new NotImplementedByEngineError();
  }

  public leaveGroup(id) {
    throw new NotImplementedByEngineError();
  }

  public setDescription(id, description) {
    throw new NotImplementedByEngineError();
  }

  public setSubject(id, description) {
    throw new NotImplementedByEngineError();
  }

  public getInviteCode(id): Promise<string> {
    throw new NotImplementedByEngineError();
  }

  public revokeInviteCode(id): Promise<string> {
    throw new NotImplementedByEngineError();
  }

  public getParticipants(id) {
    throw new NotImplementedByEngineError();
  }

  public addParticipants(id, request: ParticipantsRequest) {
    throw new NotImplementedByEngineError();
  }

  public removeParticipants(id, request: ParticipantsRequest) {
    throw new NotImplementedByEngineError();
  }

  public promoteParticipantsToAdmin(id, request: ParticipantsRequest) {
    throw new NotImplementedByEngineError();
  }

  public demoteParticipantsToUser(id, request: ParticipantsRequest) {
    throw new NotImplementedByEngineError();
  }

  public setPresence(presence: WAHAPresenceStatus, chatId?: string) {
    throw new NotImplementedByEngineError();
  }

  public getPresences(): Promise<WAHAChatPresences[]> {
    throw new NotImplementedByEngineError();
  }

  public getPresence(id: string): Promise<WAHAChatPresences> {
    throw new NotImplementedByEngineError();
  }

  public subscribePresence(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  /**
   * Channels methods
   */
  public channelsList(query: ListChannelsQuery): Promise<Channel[]> {
    throw new NotImplementedByEngineError();
  }

  public channelsCreateChannel(
    request: CreateChannelRequest,
  ): Promise<Channel> {
    throw new NotImplementedByEngineError();
  }

  public channelsGetChannel(id: string): Promise<Channel> {
    throw new NotImplementedByEngineError();
  }

  public channelsGetChannelByInviteCode(inviteCode: string): Promise<Channel> {
    throw new NotImplementedByEngineError();
  }

  public channelsDeleteChannel(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  public channelsFollowChannel(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  public channelsUnfollowChannel(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  public channelsMuteChannel(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  public channelsUnmuteChannel(id: string): Promise<void> {
    throw new NotImplementedByEngineError();
  }

  /**
   * Status methods
   */
  public sendTextStatus(status: TextStatus) {
    throw new NotImplementedByEngineError();
  }

  public sendImageStatus(status: ImageStatus) {
    throw new NotImplementedByEngineError();
  }

  public sendVoiceStatus(status: VoiceStatus) {
    throw new NotImplementedByEngineError();
  }

  public sendVideoStatus(status: VideoStatus) {
    throw new NotImplementedByEngineError();
  }

  public deleteStatus(request: DeleteStatusRequest) {
    throw new NotImplementedByEngineError();
  }

  public async getEngineInfo(): Promise<any> {
    return {};
  }

  /**
   * END - Methods for API
   */

  /**
   * Add WhatsApp suffix (@c.us) to the phone number if it doesn't have it yet
   * @param phone
   */
  protected ensureSuffix(phone) {
    return ensureSuffix(phone);
  }

  protected deserializeId(messageId: string): MessageId {
    const parts = messageId.split('_');
    return {
      fromMe: parts[0] === 'true',
      remote: parts[1],
      id: parts[2],
      _serialized: messageId,
    };
  }

  protected printQR(qr: QR) {
    if (!this.shouldPrintQR) {
      return;
    }
    if (!qr.raw) {
      this.logger.error(
        'QR.raw is not available, can not print it in the console',
      );
      return;
    }
    this.logger.info(
      "You can disable QR in console by setting 'WAHA_PRINT_QR=false' in your environment variables.",
    );
    qrcode.generate(qr.raw, { small: true });
  }
}

export function isNewsletter(jid: string) {
  return jid.endsWith('@newsletter');
}

export function getChannelInviteLink(code: string) {
  return `https://whatsapp.com/channel/${code}`;
}
