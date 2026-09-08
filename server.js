const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const compression = require('compression');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Gzip/deflate every HTTP response above compression's default size threshold —
// this is the marketing site and the web app's static assets (the JS bundle
// alone is ~5.4MB uncompressed). It does NOT touch Socket.io's WebSocket
// traffic (game-session sync), which is a separate transport this middleware
// never sees.
app.use(compression());
const io = new Server(server, {
  maxHttpBufferSize: 10e6, // 10 MB — needed for large custom inventory syncs
  cors: {
    origin: '*', // Lock this down to your app's domain in production
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 3000;

// Room registry: roomCode -> Map of peerId -> { socketId, role }
// Rooms are created on demand and cleaned up when empty
const rooms = new Map();

// ─── Usage stats ────────────────────────────────────────────────────────────
// Real bandwidth measurement, replacing the modeled estimate with actual
// numbers. "Bytes" here means server egress — payload size × the number of
// recipients each relay actually reaches — since that's what corresponds to
// Railway's outbound traffic, not just the size of what one client sent in.
// Purely additive instrumentation: a failure in here must never take down
// the actual relay, hence the try/catch in payloadBytes.
function payloadBytes(payload) {
  try {
    return Buffer.byteLength(JSON.stringify(payload));
  } catch {
    return 0;
  }
}

const globalStats = {
  startedAt: Date.now(),
  bytesByEvent: {
    offer: 0, answer: 0, 'ice-candidate': 0, 'broadcast-action': 0,
    'send-state-with-catalog': 0, 'send-state-without-catalog': 0,
    'broadcast-notification': 0,
  },
  countByEvent: {
    offer: 0, answer: 0, 'ice-candidate': 0, 'broadcast-action': 0,
    'send-state-with-catalog': 0, 'send-state-without-catalog': 0,
    'broadcast-notification': 0,
  },
  sessionsCompleted: 0,
  totalSessionBytes: 0,
  totalSessionSeconds: 0,
};

// Per-room running totals, keyed the same as `rooms`. Folded into
// globalStats and logged when the room closes (see handleLeave).
const roomStats = new Map();

function getRoomStats(roomCode) {
  if (!roomStats.has(roomCode)) {
    roomStats.set(roomCode, {
      bytesOut: 0,
      actionPushes: 0,
      statePushes: 0,
      statePushesWithCatalog: 0,
      peakPeers: 0,
      startedAt: Date.now(),
    });
  }
  return roomStats.get(roomCode);
}

// recipientCount is how many sockets this particular emit actually reached —
// 1 for a targeted offer/answer/ice-candidate/send-state(to), or
// room.size - 1 for a broadcast to everyone else in the room.
function recordEvent(roomCode, eventName, payload, recipientCount) {
  if (recipientCount <= 0) return;
  const bytes = payloadBytes(payload) * recipientCount;
  globalStats.bytesByEvent[eventName] = (globalStats.bytesByEvent[eventName] || 0) + bytes;
  globalStats.countByEvent[eventName] = (globalStats.countByEvent[eventName] || 0) + 1;
  if (roomCode) {
    const rs = getRoomStats(roomCode);
    rs.bytesOut += bytes;
    if (eventName === 'broadcast-action') rs.actionPushes += 1;
    if (eventName === 'send-state-with-catalog') { rs.statePushes += 1; rs.statePushesWithCatalog += 1; }
    if (eventName === 'send-state-without-catalog') rs.statePushes += 1;
  }
}

// ─── Marketing site ───────────────────────────────────────────────────────────
// site/index.html now answers requests to / (satisfying the same "Railway
// needs 200 at the domain root" requirement the old JSON handler existed
// for), plus /features, /support, /download, /contact, /privacy-policy.
// `extensions: ['html']` lets those resolve without a literal ".html" in the
// URL, matching the previous Google Sites page structure.
//
// SITE_MAINTENANCE lets the marketing site be pulled down temporarily (e.g.
// before the app has actually launched) without touching this service at
// all — the app's own signaling traffic (Socket.io, /health, /stats) is
// completely untouched either way: Socket.io attaches its own listener
// directly to the http server and never reaches this Express middleware
// chain, and /health + /stats are explicitly excluded below since Railway's
// own health checks depend on the former. Toggle via a Railway env var
// (SITE_MAINTENANCE=true), no code change or redeploy needed to flip it.
if (process.env.SITE_MAINTENANCE === 'true') {
  const maintenancePage = `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Elemental Inventory</title>
<style>
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#F5F8EF; color:#16210E; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; text-align:center; }
  div { padding:24px; }
  h1 { font-size:1.4rem; margin-bottom:8px; }
  p { color:#3d5233; }
</style></head><body>
  <div>
    <h1>Elemental Inventory</h1>
    <p>The site's offline for a bit while we get ready for launch — check back soon.</p>
  </div>
</body></html>`;
  app.get('*', (req, res, next) => {
    if (req.path === '/health' || req.path === '/stats') return next();
    res.status(503).type('html').send(maintenancePage);
  });
} else {
  app.use(express.static(path.join(__dirname, 'site'), { extensions: ['html'] }));
}

// ─── Web app ──────────────────────────────────────────────────────────────────
// The game itself, exported via `expo export -p web` with app.json's
// experiments.baseUrl set to "/app" so its bundle references /app/_expo/...
// instead of root-level paths — otherwise its assets would collide with (or
// shadow) the marketing site's own root-level files above. The catch-all
// exists because React Navigation does client-side routing inside the app;
// any /app/* path needs to come back as the same index.html and let the
// app's own JS take over, the same reason a single-page app always needs one.
//
// web-dist is gitignored for now (the web app is on hold) — only register
// these routes when a real export is actually present on disk, so /app falls
// through to Express's normal 404 instead of res.sendFile erroring on a
// missing file. Deploying a fresh export later just means the directory
// exists at boot and these routes come back on their own, no code change
// needed.
const webDistPath = path.join(__dirname, 'web-dist');
if (fs.existsSync(path.join(webDistPath, 'index.html'))) {
  app.use('/app', express.static(webDistPath));
  app.get('/app/*', (req, res) => {
    res.sendFile(path.join(webDistPath, 'index.html'));
  });
  console.log('[web app] web-dist found — /app is live');
} else {
  console.log('[web app] web-dist not found — /app is disabled for this deploy');
}

// ─── Health check ────────────────────────────────────────────────────────────
// Railway and Fly.io use this to confirm the server is alive
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    activeSessions: rooms.size,
    timestamp: new Date().toISOString()
  });
});

// ─── Usage stats ────────────────────────────────────────────────────────────
// Real (not modeled) bandwidth numbers — see the globalStats/roomStats block
// above. Public and read-only, same trust level as /health: aggregate byte
// counters only, no game state or player data.
app.get('/stats', (req, res) => {
  const totalBytesAllTime = Object.values(globalStats.bytesByEvent).reduce((a, b) => a + b, 0);
  res.json({
    uptimeSeconds: Math.floor((Date.now() - globalStats.startedAt) / 1000),
    bytesByEvent: globalStats.bytesByEvent,
    countByEvent: globalStats.countByEvent,
    totalBytesAllTime,
    totalMBAllTime: +(totalBytesAllTime / 1e6).toFixed(2),
    completedSessions: {
      count: globalStats.sessionsCompleted,
      avgBytesPerSession: globalStats.sessionsCompleted
        ? Math.round(globalStats.totalSessionBytes / globalStats.sessionsCompleted) : 0,
      avgMBPerSession: globalStats.sessionsCompleted
        ? +(globalStats.totalSessionBytes / globalStats.sessionsCompleted / 1e6).toFixed(2) : 0,
      avgDurationMinutes: globalStats.sessionsCompleted
        ? +(globalStats.totalSessionSeconds / globalStats.sessionsCompleted / 60).toFixed(1) : 0,
    },
    activeRooms: [...roomStats.entries()].map(([roomCode, s]) => ({
      roomCode,
      bytesOut: s.bytesOut,
      mbOut: +(s.bytesOut / 1e6).toFixed(2),
      actionPushes: s.actionPushes,
      statePushes: s.statePushes,
      statePushesWithCatalog: s.statePushesWithCatalog,
      peakPeers: s.peakPeers,
      currentPeers: rooms.get(roomCode)?.size ?? 0,
      ageMinutes: +((Date.now() - s.startedAt) / 60000).toFixed(1),
    })),
  });
});

// ─── Connection ───────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`Socket connected: ${socket.id}`);

  // Track which room and peer ID this socket belongs to
  let currentRoom = null;
  let currentPeerId = null;

  // ── join-room ──────────────────────────────────────────────────────────────
  // Called when a peer wants to enter a session.
  // Payload: { roomCode: string, peerId: string, role: 'host' | 'guest' }
  //
  // Responds to the joining peer with:
  //   'room-joined' -> { peers: [{ peerId, role }], roomCode }
  //
  // Broadcasts to all existing peers in the room:
  //   'peer-joined' -> { peerId, role }
  socket.on('join-room', ({ roomCode, peerId, role }) => {
    if (!roomCode || !peerId) {
      socket.emit('error', { message: 'roomCode and peerId are required' });
      return;
    }

    // Clean up any previous room this socket was in
    if (currentRoom) {
      handleLeave(socket, currentRoom, currentPeerId);
    }

    currentRoom = roomCode;
    currentPeerId = peerId;

    if (!rooms.has(roomCode)) {
      rooms.set(roomCode, new Map());
    }

    const room = rooms.get(roomCode);

    if (room.size >= 10) {
      socket.emit('error', { message: 'Room is full (max 10 peers)' });
      currentRoom = null;
      currentPeerId = null;
      return;
    }

    room.set(peerId, { socketId: socket.id, role });
    socket.join(roomCode);

    const rs = getRoomStats(roomCode);
    rs.peakPeers = Math.max(rs.peakPeers, room.size);

    // Tell the joining peer who is already in the room.
    // The client uses this list to initiate WebRTC offers to each existing peer.
    const existingPeers = [...room.entries()]
      .filter(([id]) => id !== peerId)
      .map(([id, data]) => ({ peerId: id, role: data.role }));

    socket.emit('room-joined', { peers: existingPeers, roomCode });

    // Tell everyone else a new peer has arrived
    socket.to(roomCode).emit('peer-joined', { peerId, role });

    console.log(`[${roomCode}] ${peerId} joined as ${role}. Room size: ${room.size}`);
  });

  // ── offer ──────────────────────────────────────────────────────────────────
  // The joining peer sends an SDP offer to an existing peer to initiate
  // a WebRTC connection. This server just forwards it.
  // Payload: { to: peerId, offer: RTCSessionDescriptionInit }
  socket.on('offer', ({ to, offer }) => {
    if (!validateInRoom(socket, currentRoom, currentPeerId)) return;

    const target = getPeer(currentRoom, to);
    if (!target) {
      socket.emit('error', { message: `Peer ${to} not found in room` });
      return;
    }

    io.to(target.socketId).emit('offer', { from: currentPeerId, offer });
    recordEvent(currentRoom, 'offer', offer, 1);
  });

  // ── answer ─────────────────────────────────────────────────────────────────
  // The existing peer responds to an offer with an SDP answer.
  // Payload: { to: peerId, answer: RTCSessionDescriptionInit }
  socket.on('answer', ({ to, answer }) => {
    if (!validateInRoom(socket, currentRoom, currentPeerId)) return;

    const target = getPeer(currentRoom, to);
    if (!target) {
      socket.emit('error', { message: `Peer ${to} not found in room` });
      return;
    }

    io.to(target.socketId).emit('answer', { from: currentPeerId, answer });
    recordEvent(currentRoom, 'answer', answer, 1);
  });

  // ── ice-candidate ──────────────────────────────────────────────────────────
  // ICE candidates are network address details that help peers punch through
  // NATs and firewalls. Both sides trickle these to each other during setup.
  // Payload: { to: peerId, candidate: RTCIceCandidateInit }
  socket.on('ice-candidate', ({ to, candidate }) => {
    if (!validateInRoom(socket, currentRoom, currentPeerId)) return;

    const target = getPeer(currentRoom, to);
    if (!target) return; // Silently ignore — target may have disconnected

    io.to(target.socketId).emit('ice-candidate', { from: currentPeerId, candidate });
    recordEvent(currentRoom, 'ice-candidate', candidate, 1);
  });

  // ── broadcast-action ──────────────────────────────────────────────────────
  // Relay a game action to all other peers in the room.
  // Used by both DM and players to propagate inventory/gold/etc changes.
  socket.on('broadcast-action', (action) => {
    if (!currentRoom) return;
    socket.to(currentRoom).emit('action-received', action);
    const recipients = (rooms.get(currentRoom)?.size ?? 1) - 1;
    recordEvent(currentRoom, 'broadcast-action', action, recipients);
  });

  // ── send-state ─────────────────────────────────────────────────────────────
  // Relay full game state from DM to players.
  // { to: peerId | null, state: object }
  // If `to` is null, relays to all other peers in the room.
  // If `to` is a peerId, relays only to that peer.
  socket.on('send-state', ({ to, state }) => {
    if (!currentRoom) return;
    const eventName = state && state.inventoryData !== undefined
      ? 'send-state-with-catalog' : 'send-state-without-catalog';
    if (to) {
      const target = getPeer(currentRoom, to);
      if (target) {
        io.to(target.socketId).emit('state-received', state);
        recordEvent(currentRoom, eventName, state, 1);
      }
    } else {
      socket.to(currentRoom).emit('state-received', state);
      const recipients = (rooms.get(currentRoom)?.size ?? 1) - 1;
      recordEvent(currentRoom, eventName, state, recipients);
    }
  });

  // ── broadcast-notification ─────────────────────────────────────────────────
  // Relay a UI notification to all other peers in the room.
  socket.on('broadcast-notification', (notification) => {
    if (!currentRoom) return;
    socket.to(currentRoom).emit('notification-received', notification);
    const recipients = (rooms.get(currentRoom)?.size ?? 1) - 1;
    recordEvent(currentRoom, 'broadcast-notification', notification, recipients);
  });

  // ── leave-room ─────────────────────────────────────────────────────────────
  // Peer explicitly leaves (e.g. user ends the session)
  socket.on('leave-room', () => {
    if (currentRoom && currentPeerId) {
      handleLeave(socket, currentRoom, currentPeerId);
      currentRoom = null;
      currentPeerId = null;
    }
  });

  // ── disconnect ─────────────────────────────────────────────────────────────
  // Handles unexpected disconnects: app backgrounded, network lost, etc.
  socket.on('disconnect', (reason) => {
    console.log(`Socket disconnected: ${socket.id} (${reason})`);
    if (currentRoom && currentPeerId) {
      handleLeave(socket, currentRoom, currentPeerId);
    }
  });
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

function handleLeave(socket, roomCode, peerId) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.delete(peerId);
  socket.leave(roomCode);

  // Let remaining peers know so they can clean up that peer's data channel
  socket.to(roomCode).emit('peer-left', { peerId });

  if (room.size === 0) {
    rooms.delete(roomCode);

    const s = roomStats.get(roomCode);
    if (s) {
      const durationSeconds = (Date.now() - s.startedAt) / 1000;
      globalStats.sessionsCompleted += 1;
      globalStats.totalSessionBytes += s.bytesOut;
      globalStats.totalSessionSeconds += durationSeconds;
      console.log(
        `[${roomCode}] Session ended: duration=${(durationSeconds / 60).toFixed(1)}m ` +
        `peakPeers=${s.peakPeers} totalBytes=${s.bytesOut} (${(s.bytesOut / 1e6).toFixed(2)}MB) ` +
        `actionPushes=${s.actionPushes} statePushes=${s.statePushes} ` +
        `(${s.statePushesWithCatalog} included the catalog)`
      );
      roomStats.delete(roomCode);
    } else {
      console.log(`[${roomCode}] Room closed (empty)`);
    }
  } else {
    console.log(`[${roomCode}] ${peerId} left. Room size: ${room.size}`);
  }
}

function getPeer(roomCode, peerId) {
  return rooms.get(roomCode)?.get(peerId) ?? null;
}

function validateInRoom(socket, roomCode, peerId) {
  if (!roomCode || !peerId) {
    socket.emit('error', { message: 'Not in a room' });
    return false;
  }
  return true;
}

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Signaling server listening on port ${PORT}`);
});