# WebSockets + Redis — a learning POC

Two servers, one Redis, two surfaces — built to answer one question:

> Why does a WebSocket app need Redis at all? Got it

| Surface | URL | What it teaches |
|---|---|---|
| **Chat** | `/` | Pub/Sub, and nothing else. The minimum viable multi-server WebSocket app. |
| **Board** | `/board` | Real-time collaboration: shared state, late joiners, live cursors, conflicting edits. |

Read them in that order. Chat is the idea; the board is what the idea costs once
people are editing the same thing.

## The one idea

A WebSocket is a long-lived connection held by **one server process**. If Alice is on
server-A and Bob is on server-B, server-A *physically cannot* push anything to Bob — it
doesn't hold Bob's socket. Redis is the relay between the processes.

```
Tab 1 ──ws──> server-A ──PUBLISH──> ┌───────┐ ──message──> server-A ──ws──> Tab 1
   :3001                            │ Redis │
Tab 2 ──ws──> server-B <─SUBSCRIBE──└───────┘ ──message──> server-B ──ws──> Tab 2
   :3002
```

Every subscriber gets every message — *including the server that published it*. That's why
no publish path also broadcasts locally: the message comes back through the subscriber
anyway. Doing both is the classic duplicate-message bug.

## Run it

```bash
docker compose up -d      # Redis on :6379
npm install               # two deps: ws, ioredis

npm run start:a           # terminal 1 → http://localhost:3001
npm run start:b           # terminal 2 → http://localhost:3002
```

Open **both** ports in two windows, on `/` and on `/board`. Everything syncs across them,
and the only thing connecting them is Redis.

---

# Surface 1 — Chat (Pub/Sub only)

`lib/chat.js`, ~60 lines. A message is relayed to whoever is connected at that instant and
then it's gone. Join late, see an empty room. Each message is labelled with the server that
first received it, so a message in the :3002 tab labelled `via server-A` went A → Redis → B.

# Surface 2 — Collaborative board

`lib/board.js`. Drag the boxes; everyone sees it live, with cursors and a presence list.
Chat needed one Redis feature. Collaboration needs three:

### 1. Pub/Sub — the delta
"box2 moved to (400, 120)" goes out on `board:room1:ops`, and every server pushes it to its
own sockets.

### 2. A HASH — the state
Pub/Sub has **no memory**. A tab opened an hour from now must still see the board. So the
authoritative shapes live in a Redis HASH (`board:room1:shapes`, one field per shape) and a
joining client gets `HGETALL` as its opening snapshot. **This is the part chat doesn't have,
and it's the part that makes it collaboration instead of broadcasting.**

### 3. Atomicity — the conflict
Two people dragging one shape is a read-modify-write race spread across two processes:

```
server-A: HGET box1 -> v7                     server-B: HGET box1 -> v7
server-A: HSET box1 (v8, x=100)               server-B: HSET box1 (v8, x=700)
                                              ^ A's write is gone, nobody noticed
```

Doing the check in JavaScript cannot fix this — the other server writes *between* your HGET
and your HSET. So the compare-and-set is a **Lua script** (`APPLY_MOVE_LUA` in `lib/board.js`),
which Redis runs to completion with nothing interleaved:

```lua
if base < current.version then
  return {0, raw}              -- stale write: reject, hand back the truth
end
incoming.version = current.version + 1
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(incoming))
```

The loser is told privately on the socket the server already holds — **not** over Redis,
because only one client is wrong. Their shape snaps to the authoritative position and the
`rejected` counter in the UI ticks up.

### Ephemeral vs durable — two channels on purpose

| | channel | stored? | rate |
|---|---|---|---|
| shape moves, join/leave | `board:room1:ops` | yes, in the HASH | bursty |
| mouse cursors | `board:room1:cursors` | **never** | ~20/sec/user |

A cursor position is worthless one second later. Writing 20 HSETs per user per second to
persist a mouse pointer is a great way to melt a Redis instance. Knowing which of your data
is worth storing is most of the design work.

Client-side, both streams are **throttled** (40ms moves, 50ms cursors) and shapes move
**optimistically** — painted locally before the server answers, because waiting for the round
trip is what makes collab apps feel laggy.

---

## The experiments

The code is small; the experiments are the lesson.

### 1. Take Redis away — watch both surfaces break

```bash
npm run start:a:noredis   # terminal 1
npm run start:b:noredis   # terminal 2
```

Chat: tabs on the same port still talk, tabs on different ports don't.
Board: the two servers silently diverge into **different boards** — drag a box on :3001 and a
fresh tab on :3002 still sees it in the old spot. This is what a multi-instance deployment
looks like without a backplane, and it's the bug you hit the day you scale from one pod to two.

### 2. Win a conflict on purpose

One person with one mouse can't drag the same box in two tabs at once, so the board has an
**auto-drag box 1** checkbox. Turn it on in one tab, then grab box 1 by hand in the other and
watch the two of you fight: the box stutters between positions and the `rejected` counter
climbs. That's last-write-wins being honest about what it costs.

Under real contention it's brutal — 6 clients hammering one shape accepted 125 writes and
rejected 625. If that's unacceptable for your use case, that's the argument for per-object
locking or CRDTs.

### 3. Prove the HASH is what saves late joiners

Move some boxes, then open a **third** tab on either port. It arrives with the current board,
not an empty one. Now compare:

```bash
docker compose exec redis redis-cli HGETALL board:room1:shapes   # the durable state
docker compose exec redis redis-cli KEYS '*'                     # no cursor keys, ever
```

### 4. Watch the wire

```bash
docker compose exec redis redis-cli SUBSCRIBE board:room1:ops board:room1:cursors chat:room1
```

Drag a box and move your mouse. You're seeing exactly what one server says to the others —
and how much louder the cursor channel is than the ops channel.

### 5. Inject a message from outside

```bash
docker compose exec redis redis-cli PUBLISH chat:room1 \
  '{"type":"chat","user":"redis-cli","text":"hello from the CLI","origin":"cli","ts":0}'
```

It appears in **both** browsers. No browser sent it; the servers are just relays. The return
value (`1`, `2`, …) is how many servers were subscribed.

### 6. Kill Redis, then bring it back

```bash
docker compose stop redis     # ioredis retries; cross-server sync stops
docker compose start redis    # it reconnects and resubscribes on its own
```

### 7. Pub/Sub is not a queue

Close a tab, publish a chat message with the CLI command above, reopen the tab. **It's gone.**
Pub/Sub has zero persistence — a subscriber that wasn't listening at that instant simply
misses it. That's exactly why the board keeps its shapes in a HASH rather than trusting the
message stream. If you need replay or delivery guarantees, that's Redis **Streams**
(`XADD`/`XREAD`), not Pub/Sub.

## What to read in the code

| Where | Why it matters |
|---|---|
| `server.js` — the two `new Redis(...)` | A connection that has issued `SUBSCRIBE` can't run `PUBLISH`/`HGET`. You need a second one. Two **total**, not two per feature — one subscriber handles many channels. |
| `server.js` — `sub.on('message')` | The only path from Redis to a browser. One path in, no duplicates. |
| `lib/chat.js` — `pub.publish(...)` then stop | Hand off and return. Broadcasting here too is the duplicate-message bug. |
| `lib/board.js` — `APPLY_MOVE_LUA` | Atomic compare-and-set. The thing you cannot do correctly in application code. |
| `lib/board.js` — `allShapes()` on join | `HGETALL` as the late-joiner snapshot: the difference between chat and collaboration. |
| `lib/board.js` — `sendTo(ws, {type:'rejected'})` | Not everything belongs on the bus. A reply to the sender never leaves the process. |
| `public/board.html` — `throttle()` | mousemove fires 60–120×/sec. Forwarding all of it is how you turn a chat app into a load test. |

## Known limitations (deliberate — each is a real lesson)

- **Presence leaks if a server crashes.** `HDEL` runs in the socket's close handler, so a
  `kill -9` strands that server's peers in the HASH forever. Verified: peers stayed at 3 after
  SIGKILL. The real fix is a heartbeat — a per-peer key with `EXPIRE`, refreshed while the
  socket is alive — so dead entries expire on their own.
- **Last-write-wins loses data by design.** The rejected drag is simply discarded. Fine for
  moving boxes, not fine for text.
- **No auth, no rooms, no history, no reconnect-resync.** A reconnecting client gets a fresh
  snapshot rather than replaying what it missed.

## Reference

| | |
|---|---|
| Ports | `3001` = server-A, `3002` = server-B, `6379` = Redis |
| Pages | `/` chat, `/board` collaborative canvas |
| Channels | `chat:room1`, `board:room1:ops`, `board:room1:cursors` |
| Keys | `board:room1:shapes` (HASH), `board:room1:peers` (HASH) |
| Env vars | `PORT`, `INSTANCE`, `REDIS_URL`, `ROOM`, `REDIS_DISABLED=1` |
| Teardown | `docker compose down` |
