import {
  BeforeApplicationShutdown,
  UnprocessableEntityException,
} from '@nestjs/common';
import { WhatsappConfigService } from '@waha/config.service';
import { ISessionMeRepository } from '@waha/core/storage/ISessionMeRepository';
import { ISessionWorkerRepository } from '@waha/core/storage/ISessionWorkerRepository';
import { WAHAWebhook } from '@waha/structures/webhooks.dto';
import { waitUntil } from '@waha/utils/promiseTimeout';
import { VERSION } from '@waha/version';
import { PinoLogger } from 'nestjs-pino';
import { merge, Observable, of } from 'rxjs';

import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '../../structures/enums.dto';
import {
  SessionConfig,
  SessionDetailedInfo,
  SessionDTO,
  SessionInfo,
} from '../../structures/sessions.dto';
import { ISessionAuthRepository } from '../storage/ISessionAuthRepository';
import { ISessionConfigRepository } from '../storage/ISessionConfigRepository';
import { WhatsappSession } from './session.abc';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const AsyncLock = require('async-lock');

export abstract class SessionManager implements BeforeApplicationShutdown {
  public store: any;
  public sessionAuthRepository: ISessionAuthRepository;
  public sessionConfigRepository: ISessionConfigRepository;
  protected sessionMeRepository: ISessionMeRepository;
  protected sessionWorkerRepository: ISessionWorkerRepository;
  private lock: any;

  WAIT_STATUS_INTERVAL = 500;
  WAIT_STATUS_TIMEOUT = 5_000;

  protected constructor(
    protected config: WhatsappConfigService,
    protected log: PinoLogger,
  ) {
    this.lock = new AsyncLock({ maxPending: Infinity });
    this.log.setContext(SessionManager.name);
  }

  protected startPredefinedSessions() {
    const startSessions = this.config.startSessions;
    startSessions.forEach((sessionName) => {
      this.withLock(sessionName, async () => {
        const log = this.log.logger.child({ session: sessionName });
        log.info(`Restarting PREDEFINED session...`);
        await this.start(sessionName).catch((error) => {
          log.error(`Failed to start PREDEFINED session: ${error}`);
          log.error(error.stack);
        });
      });
    });
  }

  public withLock(name: string, fn: () => any) {
    return this.lock.acquire(name, fn);
  }

  protected abstract getEngine(engine: WAHAEngine): typeof WhatsappSession;

  protected abstract get EngineClass(): typeof WhatsappSession;

  public getSessionEvent(session: string, event: WAHAEvents): Observable<any> {
    return of();
  }

  public getSessionEvents(
    session: string,
    events: WAHAEvents[],
  ): Observable<any> {
    return merge(
      ...events.map((event) => this.getSessionEvent(session, event)),
    );
  }

  //
  // API Methods
  //
  /**
   * Either create or update
   */
  abstract exists(name: string): Promise<boolean>;

  abstract isRunning(name: string): boolean;

  abstract upsert(name: string, config?: SessionConfig): Promise<void>;

  abstract delete(name: string): Promise<void>;

  abstract start(name: string): Promise<SessionDTO>;

  abstract stop(name: string, silent: boolean): Promise<void>;

  abstract logout(name: string): Promise<void>;

  abstract unpair(name: string): Promise<void>;

  abstract getSession(name: string): WhatsappSession;

  abstract getSessionInfo(name: string): Promise<SessionDetailedInfo | null>;

  abstract getSessions(all: boolean): Promise<SessionInfo[]>;

  get workerId() {
    return this.config.workerId;
  }

  async assign(name: string) {
    await this.sessionWorkerRepository?.assign(name, this.workerId);
  }

  async unassign(name: string) {
    await this.sessionWorkerRepository?.unassign(name, this.workerId);
  }

  async getWorkingSession(sessionName: string): Promise<WhatsappSession> {
    return this.waitUntilStatus(sessionName, [WAHASessionStatus.WORKING]);
  }

  /**
   * Wait until session is in expected status
   */
  async waitUntilStatus(
    sessionName: string,
    expected: WAHASessionStatus[],
  ): Promise<WhatsappSession> {
    const session = this.getSession(sessionName);
    const valid = await waitUntil(
      async () => expected.includes(session.status),
      this.WAIT_STATUS_INTERVAL,
      this.WAIT_STATUS_TIMEOUT,
    );
    if (!valid) {
      const msg = {
        error:
          'Session status is not as expected. Try again later or restart the session',
        session: sessionName,
        status: session.status,
        expected: expected,
      };
      throw new UnprocessableEntityException(msg);
    }
    return session;
  }

  beforeApplicationShutdown(signal?: string) {
    return;
  }
}
export function populateSessionInfo(
  event: WAHAEvents,
  session: WhatsappSession,
) {
  return (payload: any): WAHAWebhook => {
    const id = payload._eventId;
    const data = { ...payload };
    delete data._eventId;
    const me = session.getSessionMeInfo();
    return {
      id: id,
      event: event,
      session: session.name,
      metadata: session.sessionConfig?.metadata,
      me: me,
      payload: data,
      engine: session.engine,
      environment: VERSION,
    };
  };
}
