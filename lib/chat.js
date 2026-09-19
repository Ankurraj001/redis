/**
 * SURFACE 1 - CHAT: the simplest useful WebSocket + Redis pattern.
 *
 * Pub/Sub and nothing else. There is no stored state: a message is relayed to
 * whoever happens to be connected at that instant, and then it is gone forever.
 * Join late and you see an empty room.
 *
 * Read this file first, then lib/board.js - which is what you have to build
 * once "relay it to whoever is listening" stops being good enough.
 */

export const chatChannel = (room) => `chat:${room}`;

export function createChat({ pub, instance, room, broadcast, redisEnabled }) {
  const CHANNEL = chatChannel(room);

  return {
    channels: redisEnabled ? [CHANNEL] : [],

    /** Called for every message Redis hands us on the chat channel. */
    onRedisMessage(channel, raw) {
      if (channel !== CHANNEL) return; // one sub connection serves many channels
      // THE ONLY place a chat message is pushed to a browser. Every server
      // subscribed to this channel lands here - including the one that
      // published it, which is why the publish path below doesn't broadcast.
      broadcast('chat', raw);
    },

    onConnection(ws) {
      ws.send(JSON.stringify({
        type: 'hello',
        instance,
        channel: CHANNEL,
        redis: redisEnabled,
      }));
    },

    onMessage(ws, incoming) {
      const payload = JSON.stringify({
        type: 'chat',
        user: String(incoming.user || 'anon').slice(0, 24),
        text: String(incoming.text || '').slice(0, 500),
        origin: instance, // which server first received it from a browser
        ts: Date.now(),
      });

      if (!redisEnabled) {
        // BROKEN MODE: only this process's clients see it. Clients on the other
        // port are invisible to us - we don't hold their sockets.
        broadcast('chat', payload);
        return;
      }

      // Hand it to Redis and stop. It comes back through onRedisMessage a
      // millisecond later, on every instance at once. One delivery path.
      pub.publish(CHANNEL, payload);
    },
  };
}
