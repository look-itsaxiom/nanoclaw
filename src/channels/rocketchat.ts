import WebSocket from 'ws';

import { ASSISTANT_NAME, TRIGGER_PATTERN } from '../config.js';
import { readEnvFile } from '../env.js';
import { logger } from '../logger.js';
import { registerChannel, ChannelOpts } from './registry.js';
import {
  Channel,
  OnChatMetadata,
  OnInboundMessage,
  RegisteredGroup,
} from '../types.js';

export interface RocketChatChannelOpts {
  onMessage: OnInboundMessage;
  onChatMetadata: OnChatMetadata;
  registeredGroups: () => Record<string, RegisteredGroup>;
}

export class RocketChatChannel implements Channel {
  name = 'rocketchat';

  private ws: WebSocket | null = null;
  private opts: RocketChatChannelOpts;
  private url: string;
  private userId: string;
  private authToken: string;
  private connected = false;
  private msgIdCounter = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private subscribedRooms = new Set<string>();

  constructor(
    url: string,
    userId: string,
    authToken: string,
    opts: RocketChatChannelOpts,
  ) {
    this.url = url.replace(/\/$/, '');
    this.userId = userId;
    this.authToken = authToken;
    this.opts = opts;
  }

  async connect(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const wsUrl = this.url
        .replace(/^https?:\/\//, (m) =>
          m.startsWith('https') ? 'wss://' : 'ws://',
        )
        .concat('/websocket');

      this.ws = new WebSocket(wsUrl);

      this.ws.on('open', () => {
        // DDP connect
        this.send({
          msg: 'connect',
          version: '1',
          support: ['1'],
        });
      });

      let resolved = false;

      this.ws.on('message', (raw: WebSocket.RawData) => {
        const data = JSON.parse(raw.toString());

        if (data.msg === 'connected') {
          // Login with auth token
          this.send({
            msg: 'method',
            method: 'login',
            id: this.nextId(),
            params: [{ resume: this.authToken }],
          });
        }

        if (data.msg === 'result' && data.result?.id === this.userId) {
          this.connected = true;
          logger.info('Rocket.Chat bot connected');
          console.log(`\n  Rocket.Chat bot: ${this.url}`);
          console.log(`  User ID: ${this.userId}\n`);
          this.subscribeToRooms();
          if (!resolved) {
            resolved = true;
            resolve();
          }
        }

        if (data.msg === 'ping') {
          this.send({ msg: 'pong' });
        }

        if (
          data.msg === 'changed' &&
          data.collection === 'stream-room-messages'
        ) {
          this.handleMessage(data);
        }
      });

      this.ws.on('close', () => {
        this.connected = false;
        this.subscribedRooms.clear();
        if (!resolved) {
          resolved = true;
          reject(new Error('WebSocket closed before login'));
        }
        this.scheduleReconnect();
      });

      this.ws.on('error', (err) => {
        logger.error({ err: err.message }, 'Rocket.Chat WebSocket error');
        if (!resolved) {
          resolved = true;
          reject(err);
        }
      });
    });
  }

  private nextId(): string {
    return String(++this.msgIdCounter);
  }

  private send(obj: Record<string, unknown>): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }

  private subscribeToRooms(): void {
    // Subscribe to all rooms the bot has joined
    // We use stream-room-messages with __my_messages__ to get messages
    // from all rooms the user is part of
    const subId = this.nextId();
    this.send({
      msg: 'sub',
      id: subId,
      name: 'stream-room-messages',
      params: ['__my_messages__', { useCollection: false, args: [] }],
    });
    logger.info('Subscribed to Rocket.Chat room messages');
  }

  private handleMessage(data: Record<string, unknown>): void {
    const fields = data.fields as { args?: unknown[] } | undefined;
    if (!fields?.args?.[0]) return;

    const msg = fields.args[0] as Record<string, unknown>;
    const user = msg.u as { _id: string; username: string; name?: string } | undefined;

    // Ignore own messages
    if (!user || user._id === this.userId) return;

    // Ignore bot messages
    if ((msg as Record<string, unknown>).bot) return;

    const roomId = msg.rid as string;
    const chatJid = `rc:${roomId}`;
    const timestamp = new Date(
      (msg.ts as { $date: number })?.$date || Date.now(),
    ).toISOString();
    const senderName = user.name || user.username;
    const sender = user._id;
    const msgId = msg._id as string;
    let content = (msg.msg as string) || '';

    // Determine chat name from room info
    const roomName = (msg as Record<string, unknown>).roomName as string | undefined;
    const chatName = roomName ? `#${roomName}` : chatJid;

    // Translate @bot mentions into trigger format
    const botMentionPattern = new RegExp(`@${this.getBotUsername()}\\b`, 'gi');
    if (botMentionPattern.test(content)) {
      content = content.replace(botMentionPattern, '').trim();
      if (!TRIGGER_PATTERN.test(content)) {
        content = `@${ASSISTANT_NAME} ${content}`;
      }
    }

    // Handle file attachments
    const attachments = msg.attachments as
      | Array<{ title?: string; type?: string; image_url?: string; title_link?: string }>
      | undefined;
    if (attachments?.length) {
      const desc = attachments
        .map((att) => {
          if (att.image_url) return `[Image: ${att.title || 'image'}]`;
          if (att.title_link) return `[File: ${att.title || 'file'}]`;
          return `[Attachment: ${att.title || 'attachment'}]`;
        })
        .join('\n');
      content = content ? `${content}\n${desc}` : desc;
    }

    // Emit metadata
    const isGroup = !!(msg as Record<string, unknown>).channels ||
      roomId !== sender;
    this.opts.onChatMetadata(chatJid, timestamp, chatName, 'rocketchat', isGroup);

    // Only deliver for registered groups
    const group = this.opts.registeredGroups()[chatJid];
    if (!group) {
      logger.debug({ chatJid, chatName }, 'Message from unregistered Rocket.Chat room');
      return;
    }

    this.opts.onMessage(chatJid, {
      id: msgId,
      chat_jid: chatJid,
      sender,
      sender_name: senderName,
      content,
      timestamp,
      is_from_me: false,
    });

    logger.info(
      { chatJid, chatName, sender: senderName },
      'Rocket.Chat message stored',
    );
  }

  private getBotUsername(): string {
    // Cache could be added, but for now just return a default
    return 'skibot';
  }

  async sendMessage(jid: string, text: string): Promise<void> {
    const roomId = jid.replace(/^rc:/, '');

    try {
      const res = await fetch(`${this.url}/api/v1/chat.sendMessage`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Auth-Token': this.authToken,
          'X-User-Id': this.userId,
        },
        body: JSON.stringify({
          message: { rid: roomId, msg: text },
        }),
      });

      if (!res.ok) {
        const body = await res.text();
        logger.error({ jid, status: res.status, body }, 'Rocket.Chat send failed');
        return;
      }

      logger.info({ jid, length: text.length }, 'Rocket.Chat message sent');
    } catch (err) {
      logger.error({ jid, err }, 'Failed to send Rocket.Chat message');
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  ownsJid(jid: string): boolean {
    return jid.startsWith('rc:');
  }

  async disconnect(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.connected = false;
      this.ws.close();
      this.ws = null;
      logger.info('Rocket.Chat bot stopped');
    }
  }

  async setTyping(jid: string, isTyping: boolean): Promise<void> {
    if (!this.connected) return;
    const roomId = jid.replace(/^rc:/, '');
    this.send({
      msg: 'method',
      method: isTyping
        ? 'stream-notify-room'
        : 'stream-notify-room',
      id: this.nextId(),
      params: [
        `${roomId}/typing`,
        this.getBotUsername(),
        isTyping,
      ],
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    logger.info('Rocket.Chat reconnecting in 5s...');
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch((err) => {
        logger.error({ err }, 'Rocket.Chat reconnect failed');
      });
    }, 5000);
  }
}

registerChannel('rocketchat', (opts: ChannelOpts) => {
  const envVars = readEnvFile([
    'ROCKETCHAT_URL',
    'ROCKETCHAT_USER_ID',
    'ROCKETCHAT_AUTH_TOKEN',
  ]);
  const url =
    process.env.ROCKETCHAT_URL || envVars.ROCKETCHAT_URL || '';
  const userId =
    process.env.ROCKETCHAT_USER_ID || envVars.ROCKETCHAT_USER_ID || '';
  const authToken =
    process.env.ROCKETCHAT_AUTH_TOKEN || envVars.ROCKETCHAT_AUTH_TOKEN || '';

  if (!url || !userId || !authToken) {
    logger.warn(
      'Rocket.Chat: ROCKETCHAT_URL, ROCKETCHAT_USER_ID, or ROCKETCHAT_AUTH_TOKEN not set',
    );
    return null;
  }

  return new RocketChatChannel(url, userId, authToken, opts);
});
