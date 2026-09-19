/**
 * WebSocket + Redis POC - server wiring.
 *
 * THE IDEA THE WHOLE PROJECT EXISTS TO DEMONSTRATE:
 * A WebSocket is a long-lived TCP connection held by ONE server process.
 * If Alice is connected to server-A and Bob to server-B, server-A *physically
 * cannot* push anything to Bob - it doesn't hold Bob's socket. Redis is the
 * relay between the processes.
 *
 * Two surfaces share this one process, in increasing order of difficulty:
 *   /        chat   -> lib/chat.js    Pub/Sub only. Nothing is remembered.
 *   /board   canvas -> lib/board.js   Pub/Sub + shared state + conflict handling.
 *
 * Run this file twice on two ports (npm run start:a / start:b) to see it.
 */

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { createChat } from './lib/chat.js';
import { createBoard } from './lib/board.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config (all from env, so one file can be two different "servers")
// ---------------------------------------------------------------------------
const PORT = Number(process.env.PORT || 3001);
const INSTANCE = process.env.INSTANCE || 'server-A';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const ROOM = process.env.ROOM || 'room1';
// Set REDIS_DISABLED=1 for the deliberately broken build: each server talks
// only to its own clients and keeps its own private board.
const REDIS_ENABLED = process.env.REDIS_DISABLED !== '1';

// ---------------------------------------------------------------------------
// 1. A plain HTTP server, only to serve two static pages.
//    The WebSocket handshake starts life as an HTTP request, which is why the
//    WebSocket server attaches to this very same http server further down.
// ---------------------------------------------------------------------------
const PAGES = { '/': 'index.html', '/index.html': 'index.html', '/board': 'board.html' };

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];

  if (PAGES[url]) {
    const html = fs.readFileSync(path.join(__dirname, 'public', PAGES[url]));
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }
  if (url === '/favicon.ico') {
    res.writeHead(204); // no favicon; keeps the browser console clean
    res.end();
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

// ---------------------------------------------------------------------------
// 2. Redis needs TWO connections.
//    Once a connection issues SUBSCRIBE it enters "subscriber mode" and Redis
//    rejects normal commands on it (PUBLISH, HGET, HSET...). So: one connection
//    to listen with, a separate one to publish and read state with. This
//    surprises almost everyone exactly once; now it won't surprise you.
//
//    Note it's two connections TOTAL, not two per feature - a single subscriber
//    handles as many channels as you like.
// ---------------------------------------------------------------------------
let pub = null;
let sub = null;

if (REDIS_ENABLED) {
  pub = new Redis(REDIS_URL);
  sub = new Redis(REDIS_URL);
  pub.on('error', (err) => console.error(`[${INSTANCE}] redis pub error:`, err.code || err.message));
  sub.on('error', (err) => console.error(`[${INSTANCE}] redis sub error:`, err.code || err.message));
}

// ---------------------------------------------------------------------------
// 3. The WebSocket server, riding on the HTTP server above.
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server });

/** Send to every client of ONE surface connected TO THIS PROCESS ONLY. */
function broadcast(surface, raw) {
  for (const client of wss.clients) {
    // A socket can be CONNECTING/CLOSING/CLOSED; writing to those is lost.
    if (client.surface === surface && client.readyState === WebSocket.OPEN) {
      client.send(raw);
    }
  }
}

/** Reply to a single socket. Never needs Redis: we already hold this socket. */
function sendTo(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}

const deps = { pub, instance: INSTANCE, room: ROOM, broadcast, sendTo, redisEnabled: REDIS_ENABLED };
const surfaces = {
  chat: createChat(deps),
  board: createBoard(deps),
};

if (REDIS_ENABLED) {
  // One SUBSCRIBE call, every channel both surfaces care about.
  const channels = [...surfaces.chat.channels, ...surfaces.board.channels];
  await sub.subscribe(...channels);
  console.log(`[${INSTANCE}] subscribed to ${channels.join(', ')}`);

  // One listener, fanned out by channel name. Every server subscribed to a
  // channel gets every message on it - INCLUDING the server that published it.
  // That is precisely why no publish path also broadcasts locally: doing both
  // is the classic duplicate-message bug.
  sub.on('message', (channel, raw) => {
    surfaces.chat.onRedisMessage(channel, raw);
    surfaces.board.onRedisMessage(channel, raw);
  });
}

await surfaces.board.init();

wss.on('connection', async (ws, req) => {
  // Route by path: the surface is decided at handshake time and never changes.
  const url = (req.url || '/').split('?')[0];
  ws.surface = url === '/board' ? 'board' : 'chat';
  const surface = surfaces[ws.surface];

  console.log(`[${INSTANCE}] ${ws.surface} client connected`);
  await surface.onConnection(ws);

  ws.on('message', async (data) => {
    let incoming;
    try {
      incoming = JSON.parse(data.toString());
    } catch {
      return; // ignore anything that isn't JSON
    }
    try {
      await surface.onMessage(ws, incoming);
    } catch (err) {
      console.error(`[${INSTANCE}] ${ws.surface} handler error:`, err.message);
    }
  });

  ws.on('close', async () => {
    console.log(`[${INSTANCE}] ${ws.surface} client disconnected`);
    if (surface.onClose) await surface.onClose(ws);
  });

  ws.on('error', (err) => console.error(`[${INSTANCE}] socket error:`, err.message));
});

server.listen(PORT, () => {
  console.log('');
  console.log(`  ${INSTANCE}  listening on http://localhost:${PORT}`);
  console.log(`     chat   http://localhost:${PORT}/`);
  console.log(`     board  http://localhost:${PORT}/board`);
  console.log(`  redis:  ${REDIS_ENABLED ? `${REDIS_URL} room "${ROOM}"` : 'DISABLED (local-only, per-server state)'}`);
  console.log('');
});
