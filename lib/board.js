/**
 * SURFACE 2 - SHARED CANVAS: real-time collaboration.
 *
 * Chat needed exactly one Redis feature: Pub/Sub. A collaborative *document*
 * needs three, and the gap between them is the whole lesson.
 *
 *   1. Pub/Sub  - tell the other servers what just changed (the delta).
 *   2. A HASH   - the authoritative current state. A client that joins thirty
 *                 minutes late must see the same board as everyone else, and
 *                 Pub/Sub cannot do that: it has no memory. On join we HGETALL.
 *   3. Atomicity- two people dragging one shape at the same moment is a
 *                 read-modify-write race spread across two processes. A Lua
 *                 script collapses check-and-set into one atomic step.
 *
 * Ephemeral vs durable is deliberately split across two channels:
 *   board:<room>:ops      shape moves, joins, leaves -> ALSO written to the HASH
 *   board:<room>:cursors  mouse positions            -> never stored, ~20/sec/user
 * A cursor position is worthless one second later, so it never touches the HASH.
 * Writing 20 HSETs per user per second to persist a mouse pointer is a classic
 * way to melt a Redis instance.
 */

import { randomUUID } from 'node:crypto';

const PALETTE = ['#ef4444', '#f59e0b', '#10b981', '#3b82f6', '#a855f7', '#ec4899'];

const SEED_SHAPES = [
  { id: 'box1', x: 60, y: 60, w: 130, h: 80, label: 'box 1', color: '#3b82f6' },
  { id: 'box2', x: 260, y: 150, w: 130, h: 80, label: 'box 2', color: '#10b981' },
  { id: 'box3', x: 480, y: 70, w: 130, h: 80, label: 'box 3', color: '#f59e0b' },
];

export const boardChannels = (room) => [`board:${room}:ops`, `board:${room}:cursors`];

/**
 * THE CONFLICT RESOLVER, as an atomic Redis operation.
 *
 * Why Lua and not HGET-then-HSET in JavaScript? Because between your HGET and
 * your HSET, the *other* server can write. You'd overwrite a newer version and
 * never know. Redis runs a script to completion with nothing interleaved, so
 * the compare and the write cannot be split apart.
 *
 * Rule (last-write-wins with a version guard): an update whose baseVersion is
 * older than what's stored is rejected, and the caller gets the truth back.
 */
const APPLY_MOVE_LUA = `
  local raw      = redis.call('HGET', KEYS[1], ARGV[1])
  local incoming = cjson.decode(ARGV[2])
  local base     = tonumber(ARGV[3])

  if raw then
    local current = cjson.decode(raw)
    if base < current.version then
      return {0, raw}                    -- stale write: reject, hand back truth
    end
    incoming.version = current.version + 1
  else
    incoming.version = 1
  end

  local encoded = cjson.encode(incoming)
  redis.call('HSET', KEYS[1], ARGV[1], encoded)
  return {1, encoded}
`;

export function createBoard({ pub, instance, room, broadcast, sendTo, redisEnabled }) {
  const [OPS, CURSORS] = boardChannels(room);
  const SHAPES_KEY = `board:${room}:shapes`;
  const PEERS_KEY = `board:${room}:peers`;

  // Fallback state for REDIS_DISABLED=1, so the "what breaks without Redis"
  // experiment works on this surface too: each server keeps its own private
  // board, and the two instances silently diverge.
  const memShapes = new Map();
  const memPeers = new Map();

  if (redisEnabled) {
    // ioredis registers the script once and then uses EVALSHA automatically.
    pub.defineCommand('applyShapeMove', { numberOfKeys: 1, lua: APPLY_MOVE_LUA });
  }

  // --- state access: Redis-backed or in-memory, same shape of API ----------

  async function seedIfEmpty() {
    for (const s of SEED_SHAPES) {
      const shape = { ...s, version: 1, movedBy: null, movedByName: 'seed' };
      if (redisEnabled) {
        // HSETNX is atomic, so both servers can race to seed and neither wins twice.
        await pub.hsetnx(SHAPES_KEY, s.id, JSON.stringify(shape));
      } else if (!memShapes.has(s.id)) {
        memShapes.set(s.id, shape);
      }
    }
  }

  async function allShapes() {
    if (!redisEnabled) return [...memShapes.values()];
    const map = await pub.hgetall(SHAPES_KEY); // the late-joiner snapshot
    return Object.values(map).map((v) => JSON.parse(v));
  }

  async function allPeers() {
    if (!redisEnabled) return [...memPeers.values()];
    const map = await pub.hgetall(PEERS_KEY);
    return Object.values(map).map((v) => JSON.parse(v));
  }

  async function applyMove(shape, baseVersion) {
    if (!redisEnabled) {
      const current = memShapes.get(shape.id);
      if (current && baseVersion < current.version) return { accepted: false, shape: current };
      const next = { ...shape, version: current ? current.version + 1 : 1 };
      memShapes.set(shape.id, next);
      return { accepted: true, shape: next };
    }
    const [accepted, json] = await pub.applyShapeMove(
      SHAPES_KEY, shape.id, JSON.stringify(shape), baseVersion,
    );
    return { accepted: accepted === 1, shape: JSON.parse(json) };
  }

  /** Publish to all instances, or fall back to local-only when Redis is off. */
  function fanout(channel, payload) {
    const raw = JSON.stringify(payload);
    if (redisEnabled) pub.publish(channel, raw);
    else broadcast('board', raw);
  }

  return {
    channels: redisEnabled ? [OPS, CURSORS] : [],
    init: seedIfEmpty,

    onRedisMessage(channel, raw) {
      // Both board channels end up doing the same thing here - fan out to the
      // local sockets. They're separate channels so that a server could choose
      // to ignore cursors (e.g. under load) without losing shape edits.
      if (channel === OPS || channel === CURSORS) broadcast('board', raw);
    },

    async onConnection(ws) {
      const peer = {
        clientId: randomUUID().slice(0, 8),
        name: 'user-' + Math.random().toString(36).slice(2, 6),
        color: PALETTE[Math.floor(Math.random() * PALETTE.length)],
        instance,
      };
      ws.peer = peer;

      if (redisEnabled) await pub.hset(PEERS_KEY, peer.clientId, JSON.stringify(peer));
      else memPeers.set(peer.clientId, peer);

      // The join snapshot: read straight from Redis and answer this one socket.
      // No Pub/Sub involved - nobody else needs to hear about it, and a reply
      // to the sender never has to leave the process that holds the socket.
      sendTo(ws, {
        type: 'welcome',
        you: peer,
        instance,
        redis: redisEnabled,
        shapes: await allShapes(),
        peers: await allPeers(),
      });

      // Everyone else does need to hear about it, so that goes on the bus.
      fanout(OPS, { type: 'peer-join', peer });
    },

    async onMessage(ws, incoming) {
      const peer = ws.peer;
      if (!peer) return;

      if (incoming.type === 'cursor') {
        // Highest-frequency message in the app. Fire and forget: no HASH write,
        // no versioning, no ack. If one is dropped the next arrives in ~50ms.
        fanout(CURSORS, {
          type: 'cursor',
          clientId: peer.clientId,
          name: peer.name,
          color: peer.color,
          instance,
          x: Number(incoming.x) || 0,
          y: Number(incoming.y) || 0,
        });
        return;
      }

      if (incoming.type === 'move') {
        const base = Number(incoming.baseVersion) || 0;
        const proposed = {
          id: String(incoming.id),
          x: Math.round(Number(incoming.x) || 0),
          y: Math.round(Number(incoming.y) || 0),
          w: Number(incoming.w) || 130,
          h: Number(incoming.h) || 80,
          label: String(incoming.label || '').slice(0, 40),
          color: String(incoming.color || '#3b82f6'),
          version: base,
          movedBy: peer.clientId,
          movedByName: peer.name,
        };

        const { accepted, shape } = await applyMove(proposed, base);

        if (!accepted) {
          // The loser of the race is told privately, on the socket we already
          // hold. This does NOT go through Redis: only one client is wrong.
          sendTo(ws, { type: 'rejected', shape, reason: 'stale version' });
          return;
        }

        fanout(OPS, { type: 'shape', shape, origin: instance });
        return;
      }

      if (incoming.type === 'reset') {
        if (redisEnabled) await pub.del(SHAPES_KEY);
        else memShapes.clear();
        await seedIfEmpty();
        fanout(OPS, { type: 'board-reset', shapes: await allShapes(), by: peer.name });
      }
    },

    async onClose(ws) {
      const peer = ws.peer;
      if (!peer) return;
      if (redisEnabled) await pub.hdel(PEERS_KEY, peer.clientId);
      else memPeers.delete(peer.clientId);
      fanout(OPS, { type: 'peer-leave', clientId: peer.clientId, name: peer.name });
    },
  };
}
