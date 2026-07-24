const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*', // Lock this down to your app's domain in production
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 3000;

// Room registry: roomCode -> Map of peerId -> { socketId, role }
// Rooms are created on demand and cleaned up when empty
const rooms = new Map();

// ─── Root ─────────────────────────────────────────────────────────────────────
// Required for Railway's HTTP router to confirm the service is reachable via
// the public domain. Without this, requests to / return 404 and traffic
// routing fails.
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'signaling-server' });
});

// ─── Health check ────────────────────────────────────────────────────────────
// Railway and Fly.io use this to confirm the server is alive
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    activeSessions: rooms.size,
    timestamp: new Date().toISOString()
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
  });

  // ── broadcast-action ──────────────────────────────────────────────────────
  // Relay a game action to all other peers in the room.
  // Used by both DM and players to propagate inventory/gold/etc changes.
  socket.on('broadcast-action', (action) => {
    if (!currentRoom) return;
    socket.to(currentRoom).emit('action-received', action);
  });

  // ── send-state ─────────────────────────────────────────────────────────────
  // Relay full game state from DM to players.
  // { to: peerId | null, state: object }
  // If `to` is null, relays to all other peers in the room.
  // If `to` is a peerId, relays only to that peer.
  socket.on('send-state', ({ to, state }) => {
    if (!currentRoom) return;
    if (to) {
      const target = getPeer(currentRoom, to);
      if (target) io.to(target.socketId).emit('state-received', state);
    } else {
      socket.to(currentRoom).emit('state-received', state);
    }
  });

  // ── broadcast-notification ─────────────────────────────────────────────────
  // Relay a UI notification to all other peers in the room.
  socket.on('broadcast-notification', (notification) => {
    if (!currentRoom) return;
    socket.to(currentRoom).emit('notification-received', notification);
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
    console.log(`[${roomCode}] Room closed (empty)`);
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