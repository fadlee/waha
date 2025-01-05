import {
  BeforeApplicationShutdown,
  Logger,
  LoggerService,
} from '@nestjs/common';
import { sleep } from '@nestjs/terminus/dist/utils';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { SessionManager } from '@waha/core/abc/manager.abc';
import { WebsocketHeartbeatJob } from '@waha/nestjs/ws/WebsocketHeartbeatJob';
import { WebSocket } from '@waha/nestjs/ws/ws';
import { WAHAEvents, WAHAEventsWild } from '@waha/structures/enums.dto';
import { EventWildUnmask } from '@waha/utils/events';
import { generatePrefixedId } from '@waha/utils/ids';
import { IncomingMessage } from 'http';
import * as url from 'url';
import { Server } from 'ws';

@WebSocketGateway({
  path: '/ws',
  cors: true,
})
export class WebsocketGatewayCore
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    BeforeApplicationShutdown
{
  HEARTBEAT_INTERVAL = 10_000;

  @WebSocketServer()
  server: Server;

  private readonly logger: LoggerService;
  private heartbeat: WebsocketHeartbeatJob;
  private eventUnmask = new EventWildUnmask(WAHAEvents, WAHAEventsWild);

  constructor(private manager: SessionManager) {
    this.logger = new Logger('WebsocketGateway');
    this.heartbeat = new WebsocketHeartbeatJob(
      this.logger,
      this.HEARTBEAT_INTERVAL,
    );
  }

  handleConnection(socket: WebSocket, request: IncomingMessage, ...args): any {
    // wsc - websocket client
    const id = generatePrefixedId('wsc');
    socket.id = id;
    this.logger.debug(`New client connected: ${request.url}`);
    const params = this.getParams(request);
    const session: string = params.session;
    const events: WAHAEvents[] = params.events;
    this.logger.debug(
      `Client connected to session: '${session}', events: ${events}, ${id}`,
    );

    const sub = this.manager
      .getSessionEvents(session, events)
      .subscribe((data) => {
        this.logger.debug(`Sending data to client, event.id: ${data.id}`, data);
        socket.send(JSON.stringify(data));
      });
    socket.on('close', () => {
      this.logger.debug(`Client disconnected - ${socket.id}`);
      sub.unsubscribe();
    });
  }

  private getParams(request: IncomingMessage) {
    const query = url.parse(request.url, true).query;
    const session = (query.session as string) || '*';
    let paramsEvents = (query.events as string[]) || '*';
    // if params events string - split by ","
    if (typeof paramsEvents === 'string') {
      paramsEvents = paramsEvents.split(',');
    }
    const events = this.eventUnmask.unmask(paramsEvents);
    return { session, events };
  }

  handleDisconnect(socket: WebSocket): any {
    this.logger.debug(`Client disconnected - ${socket.id}`);
  }

  async beforeApplicationShutdown(signal?: string) {
    this.logger.log('Shutting down websocket server');
    // Allow pending messages to be sent, it can be even 1ms, just to release the event loop
    await sleep(100);
    this.server?.clients.forEach((options, client) => {
      // @ts-ignore
      this.logger.debug(`Closing client connection - ${client.id}...`);
      client.close(1001, 'Server is shutting down');
    });
    // Do not turn off heartbeat service here,
    // it's responsible for terminating the connection that is not alive
    this.logger.log('Websocket server is down');
    this.heartbeat?.stop();
  }

  afterInit(server: Server) {
    this.logger.debug('Websocket server initialized');

    this.logger.debug('Starting heartbeat service...');
    this.heartbeat.start(server);
    this.logger.debug('Heartbeat service started');
  }
}
