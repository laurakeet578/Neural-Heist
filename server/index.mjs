/**
 * Neural Heist multiplayer foundation server.
 * Networking + match state only (no runtime bundle coupling).
 */
import http from "http";
import { setInterval as nodeSetInterval } from "timers";
import { WebSocketServer } from "ws";

const PROTOCOL_VERSION = 1;
const PORT = Number(process.env.PORT || 8787);
const NH_MISSION_ID = Number(process.env.NH_MISSION_ID || 1);
const NH_MAX_MISSION = Math.max(1, Number(process.env.NH_MAX_MISSION || 6));
const NH_SLOT_COUNT = Math.max(2, Math.min(4, Number(process.env.NH_SLOT_COUNT || 2)));
const NH_TICK_HZ = Math.max(8, Math.min(60, Number(process.env.NH_TICK_HZ || 20)));
const CLIENT_IDLE_TIMEOUT_MS = Math.max(12000, Number(process.env.NH_CLIENT_IDLE_TIMEOUT_MS || 45000));
const WS_HEARTBEAT_MS = Math.max(5000, Number(process.env.NH_WS_HEARTBEAT_MS || 15000));
const CONNECTION_READY_DELAY_MS = Math.max(50, Number(process.env.NH_CONNECTION_READY_DELAY_MS || 120));
const CUTSCENE_AUTO_END_MS = Math.max(1000, Number(process.env.NH_CUTSCENE_AUTO_END_MS || 9000));

const RoomPhase = {
  LOBBY: "LOBBY",
  STARTING: "STARTING",
  RUNNING: "RUNNING",
  ENDED: "ENDED"
};

const AllowedClientEvents = new Set(["create_match", "join_match", "request_start", "request_cutscene_skip"]);
const AllowedServerEvents = new Set(["match_created", "match_joined", "match_started", "snapshot", "match_error"]);

function log(tag, payload) {
  const at = new Date().toISOString();
  if (payload !== undefined) console.log(`[WS ${at}] ${tag}`, payload);
  else console.log(`[WS ${at}] ${tag}`);
}

function clampMissionId(m) {
  const v = Number(m) | 0;
  if (!Number.isFinite(v) || v < 1) return NH_MISSION_ID;
  return Math.min(v, NH_MAX_MISSION);
}

function clampSlotCount(n) {
  const v = Number(n) | 0;
  if (!Number.isFinite(v)) return NH_SLOT_COUNT;
  return Math.max(2, Math.min(4, v));
}

function normalizeRoomCode(raw) {
  const out = String(raw || "").trim().toUpperCase();
  if (!/^[A-Z0-9]{4,8}$/.test(out)) return "";
  return out;
}

function randomRoomCode() {
  return String(1000 + ((Math.random() * 9000) | 0));
}

function randomMatchId() {
  return "m_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8);
}

function main() {
  /** @type {Map<import('ws').WebSocket, { slot: number | null, connectionState: "connecting" | "ready" | "disconnected" }>} */
  const clients = new Map();
  /** @type {Map<string, { roomCode: string, slots: Array<import('ws').WebSocket | null>, phase: string, matchId: string | null, missionId: number, state: any }>} */
  const rooms = new Map();
  /** @type {Map<import('ws').WebSocket, string>} */
  const clientRoomBySocket = new Map();

  let activeMissionId = NH_MISSION_ID;
  let activeSlotCount = NH_SLOT_COUNT;
  /** @type {null | { roomCode: string, matchId: string, missionId: number, phase: string, tick: number, startedAt: number, firstSnapshotSent: boolean, state: any }} */
  let activeMatch = null;

  function sendMatchError(ws, message, extra = {}) {
    const out = { v: PROTOCOL_VERSION, type: "match_error", message, ...extra };
    log("match_error_sent", out);
    try {
      ws.send(JSON.stringify(out));
    } catch (_) {}
  }

  function getClientMeta(ws) {
    return clients.get(ws) || null;
  }

  function sendMatchEvent(ws, type, payload) {
    if (!AllowedServerEvents.has(type)) {
      log("contract_violation_server_event", { type });
      return;
    }
    try {
      ws.send(JSON.stringify({ v: PROTOCOL_VERSION, type, ...payload }));
    } catch (_) {}
  }

  function broadcastRoomMessage(roomCode, payload) {
    if (!payload || !AllowedServerEvents.has(String(payload.type || ""))) {
      log("contract_violation_server_event", { type: payload && payload.type ? payload.type : null, roomCode });
      return;
    }
    const room = rooms.get(roomCode);
    if (!room) return;
    const json = JSON.stringify(payload);
    for (const sock of room.slots) {
      if (!sock || sock.readyState !== 1) continue;
      try {
        sock.send(json);
      } catch (_) {}
    }
  }

  function panicMatch(reason, details = {}) {
    if (!activeMatch) return;
    const { roomCode, matchId } = activeMatch;
    log("match_error", { reason, roomCode, matchId, ...details });
    const room = rooms.get(roomCode);
    if (room) {
      room.phase = RoomPhase.ENDED;
      room.matchId = null;
      broadcastRoomMessage(roomCode, {
        v: PROTOCOL_VERSION,
        type: "match_error",
        roomCode,
        matchId,
        message: "Match aborted: " + reason
      });
      room.phase = RoomPhase.LOBBY;
      room.state = null;
    }
    activeMatch = null;
  }

  function roomConnectedSlots(room) {
    return room.slots.map(Boolean);
  }

  function broadcastRoomPresence(roomCode, created) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const connectedSlots = roomConnectedSlots(room);
    const base = {
      ok: true,
      created: !!created,
      roomCode,
      phase: room.phase,
      matchId: room.matchId,
      missionId: room.missionId,
      slotCount: room.slots.length,
      connectedSlots,
      connectedCount: connectedSlots.filter(Boolean).length
    };
    for (let i = 0; i < room.slots.length; i++) {
      const sock = room.slots[i];
      if (!sock || sock.readyState !== 1) continue;
      sendMatchEvent(sock, created ? "match_created" : "match_joined", { ...base, slot: i });
    }
  }

  function removeClientFromRoom(ws) {
    const roomCode = clientRoomBySocket.get(ws);
    if (!roomCode) return;
    clientRoomBySocket.delete(ws);
    const room = rooms.get(roomCode);
    if (!room) return;
    for (let i = 0; i < room.slots.length; i++) {
      if (room.slots[i] === ws) room.slots[i] = null;
    }
    const connectedCount = room.slots.filter(Boolean).length;
    log("match_join_leave", { action: "leave", roomCode, connectedCount });
    if (activeMatch && activeMatch.roomCode === roomCode && activeMatch.phase !== RoomPhase.ENDED) {
      panicMatch("player_disconnected_during_active_match", { connectedCount });
    }
    if (connectedCount > 0) broadcastRoomPresence(roomCode, false);
    if (connectedCount <= 0) {
      if (activeMatch && activeMatch.roomCode === roomCode) panicMatch("room_empty");
      rooms.delete(roomCode);
      log("room_deleted", { roomCode });
    }
  }

  function assignClientToRoom(ws, roomCode) {
    const room = rooms.get(roomCode);
    if (!room) return { ok: false, error: "Room not found." };
    const currentCode = clientRoomBySocket.get(ws);
    if (currentCode && currentCode !== roomCode) removeClientFromRoom(ws);
    let slot = room.slots.indexOf(ws);
    if (slot < 0) slot = room.slots.findIndex((sock) => !sock);
    if (slot < 0) return { ok: false, error: "Room is full." };
    room.slots[slot] = ws;
    clientRoomBySocket.set(ws, roomCode);
    const meta = clients.get(ws);
    if (meta) meta.slot = slot;
    const connectedSlots = roomConnectedSlots(room);
    return {
      ok: true,
      roomCode,
      slot,
      slotCount: room.slots.length,
      connectedSlots,
      connectedCount: connectedSlots.filter(Boolean).length
    };
  }

  function buildAuthoritativeSnapshot(match) {
    const room = rooms.get(match.roomCode);
    if (!room) throw new Error("room_missing_for_snapshot");
    match.tick += 1;
    const connectedSlots = roomConnectedSlots(room);
    const players = connectedSlots.map((connected, slot) => ({
      id: slot,
      slot,
      connected
    }));
    return {
      v: PROTOCOL_VERSION,
      type: "snapshot",
      matchId: match.matchId,
      roomCode: match.roomCode,
      phase: match.phase,
      tick: match.tick,
      timestamp: Date.now(),
      missionId: match.missionId,
      gamePhase: match.state.gamePhase,
      cutscenePhase: match.state.cutscenePhase,
      cutscene_active: !!match.state.cutsceneActive,
      connectedSlots,
      players,
      state: {
        matchId: match.matchId,
        roomCode: match.roomCode,
        phase: match.phase,
        missionId: match.missionId,
        gamePhase: match.state.gamePhase,
        cutscenePhase: match.state.cutscenePhase,
        cutsceneActive: !!match.state.cutsceneActive,
        connectedSlots,
        players
      }
    };
  }

  function emitSnapshot(match, reason) {
    const room = rooms.get(match.roomCode);
    if (!room) throw new Error("room_missing_for_emit");
    const payload = buildAuthoritativeSnapshot(match);
    let sent = 0;
    const json = JSON.stringify(payload);
    for (const ws of room.slots) {
      if (!ws || ws.readyState !== 1) continue;
      ws.send(json);
      sent++;
    }
    log("snapshot_emission", { roomCode: match.roomCode, matchId: match.matchId, tick: payload.tick, reason, recipients: sent });
    if (sent <= 0) throw new Error("no_snapshot_recipients");
    return payload;
  }

  function startMatch(roomCode, missionId) {
    const room = rooms.get(roomCode);
    if (!room) throw new Error("room_not_found");
    if (room.phase !== RoomPhase.LOBBY) throw new Error("room_not_in_lobby");
    const connectedCount = room.slots.filter(Boolean).length;
    if (connectedCount < 2) throw new Error("minimum_players_not_met");
    if (activeMatch && (activeMatch.phase === RoomPhase.STARTING || activeMatch.phase === RoomPhase.RUNNING)) {
      throw new Error("another_match_active");
    }

    const matchId = randomMatchId();
    room.phase = RoomPhase.STARTING;
    room.matchId = matchId;
    room.missionId = missionId;
    room.state = { gamePhase: "intro_cutscene", cutscenePhase: "intro_cutscene", cutsceneActive: true };
    activeMatch = {
      roomCode,
      matchId,
      missionId,
      phase: RoomPhase.STARTING,
      tick: 0,
      startedAt: Date.now(),
      firstSnapshotSent: false,
      state: room.state
    };

    const first = emitSnapshot(activeMatch, "start_match_first_snapshot");
    if (!first || first.type !== "snapshot") throw new Error("first_snapshot_not_emitted");
    activeMatch.firstSnapshotSent = true;
    activeMatch.phase = RoomPhase.RUNNING;
    room.phase = RoomPhase.RUNNING;
    log("match_started", { roomCode, matchId, missionId, connectedCount });
    broadcastRoomMessage(roomCode, {
      v: PROTOCOL_VERSION,
      type: "match_started",
      roomCode,
      missionId,
      connectedCount,
      matchId,
      phase: activeMatch.phase
    });
    log("match_started_sent", {
      roomCode,
      matchId,
      recipients: room.slots.filter(Boolean).length,
      reason: "new_match_started"
    });
    // Critical unlock contract: after match_started, immediately send another
    // snapshot so clients waiting on first post-start snapshot can transition.
    emitSnapshot(activeMatch, "post_match_started_snapshot");
    log("match_start_success", { roomCode, matchId, missionId });
  }

  function updateCutsceneState(match, requestedPhase) {
    if (!match || match.phase !== RoomPhase.RUNNING) throw new Error("match_not_running");
    if (!match.state) match.state = { gamePhase: "playing", cutscenePhase: null, cutsceneActive: false };
    // Match state is intentionally minimal on this decoupled server.
    // Accept any allowed skip intent and advance to playable phase.
    if (match.state.cutscenePhase !== requestedPhase && match.state.gamePhase !== requestedPhase) {
      log("cutscene_skip_phase_mismatch_tolerated", {
        expectedCutscenePhase: match.state.cutscenePhase || null,
        currentGamePhase: match.state.gamePhase || null,
        requestedPhase
      });
    }
    match.state.cutscenePhase = null;
    match.state.gamePhase = "playing";
    match.state.cutsceneActive = false;
  }

  const server = http.createServer((req, res) => {
    if (req.url === "/health" || req.url === "/") {
      const connectedSlots = [];
      for (const meta of clients.values()) connectedSlots.push(meta.slot);
      const body = {
        ok: true,
        missionId: activeMissionId,
        slotCount: activeSlotCount,
        connectedPlayers: connectedSlots.length,
        connectedSlots: connectedSlots.filter((x) => x != null).sort((a, b) => a - b),
        activeMatch: activeMatch
          ? { matchId: activeMatch.matchId, roomCode: activeMatch.roomCode, phase: activeMatch.phase, tick: activeMatch.tick }
          : null
      };
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(body));
      return;
    }
    res.writeHead(404);
    res.end("not found");
  });

  const wss = new WebSocketServer({ server, path: "/ws" });

  wss.on("connection", (ws, req) => {
    clients.set(ws, { slot: null, connectionState: "connecting" });
    ws._lastSeenAt = Date.now();
    ws._isAlive = true;
    const remote = req?.socket?.remoteAddress || "unknown";
    log("connection_open", { remote });
    const readyTimer = setTimeout(() => {
      const meta = getClientMeta(ws);
      if (!meta) return;
      if (meta.connectionState === "disconnected") return;
      meta.connectionState = "ready";
      log("connection_ready", { remote });
    }, CONNECTION_READY_DELAY_MS);

    ws.on("pong", () => {
      ws._isAlive = true;
      ws._lastSeenAt = Date.now();
    });

    ws.on("message", (raw) => {
      try {
        ws._lastSeenAt = Date.now();
        let msg;
        try {
          msg = JSON.parse(String(raw));
        } catch (parseErr) {
          log("message_parse_error", { error: String(parseErr && parseErr.stack ? parseErr.stack : parseErr) });
          return;
        }
        if (!msg || msg.v !== PROTOCOL_VERSION) return;
        const type = String(msg.type || "");
        log("event_in", { type });
        if (!AllowedClientEvents.has(type)) {
          log("contract_violation_client_event", { type: type || null });
          sendMatchError(ws, "Unsupported event type.", { invalidType: type || null });
          return;
        }
        const meta = getClientMeta(ws);
        if ((type === "create_match" || type === "join_match") && (!meta || meta.connectionState !== "ready")) {
          log("connection_not_ready", { type, state: meta ? meta.connectionState : "missing" });
          sendMatchError(ws, "Connection not ready yet. Retry shortly.", { state: meta ? meta.connectionState : "connecting" });
          return;
        }

        if (type === "create_match") {
          activeMissionId = clampMissionId(msg.missionId != null ? msg.missionId : activeMissionId);
          activeSlotCount = clampSlotCount(msg.slotCount != null ? msg.slotCount : activeSlotCount);
          let roomCode = randomRoomCode();
          while (rooms.has(roomCode)) roomCode = randomRoomCode();
          rooms.set(roomCode, {
            roomCode,
            slots: new Array(activeSlotCount).fill(null),
            phase: RoomPhase.LOBBY,
            matchId: null,
            missionId: activeMissionId,
            state: null
          });
          const result = assignClientToRoom(ws, roomCode);
          log("match_create", { roomCode, slotCount: activeSlotCount });
          if (!result.ok) sendMatchError(ws, result.error || "Match create failed.");
          else broadcastRoomPresence(roomCode, true);
          return;
        }

        if (type === "join_match") {
          const roomCode = normalizeRoomCode(msg.roomCode);
          if (!roomCode) {
            sendMatchError(ws, "Invalid room code.");
            return;
          }
          const room = rooms.get(roomCode);
          if (!room) {
            sendMatchError(ws, "Room not found.");
            return;
          }
          const result = assignClientToRoom(ws, roomCode);
          log("match_join", { roomCode, connectedCount: result.connectedCount });
          if (!result.ok) sendMatchError(ws, result.error || "Join failed.");
          else broadcastRoomPresence(roomCode, false);
          return;
        }

        if (type === "request_start") {
          const roomCode = normalizeRoomCode(msg.roomCode || clientRoomBySocket.get(ws));
          log("request_start_received", {
            roomCode: roomCode || null,
            requestedMissionId: msg.missionId != null ? (msg.missionId | 0) : null
          });
          if (!roomCode) {
            sendMatchError(ws, "Invalid room code.");
            return;
          }
          const room = rooms.get(roomCode);
          if (!room) {
            sendMatchError(ws, "Room not found.");
            return;
          }
          if (room.slots[0] !== ws) {
            sendMatchError(ws, "Only host can start match.");
            return;
          }
          if (activeMatch && activeMatch.roomCode === roomCode && activeMatch.phase === RoomPhase.RUNNING) {
            const out = {
              v: PROTOCOL_VERSION,
              type: "match_started",
              roomCode,
              missionId: activeMatch.missionId,
              connectedCount: room.slots.filter(Boolean).length,
              matchId: activeMatch.matchId,
              phase: activeMatch.phase
            };
            broadcastRoomMessage(roomCode, out);
            log("match_started_sent", {
              roomCode,
              matchId: activeMatch.matchId,
              recipients: room.slots.filter(Boolean).length,
              reason: "already_running_match"
            });
            return;
          }
          const missionId = clampMissionId(msg.missionId != null ? msg.missionId : room.missionId);
          try {
            startMatch(roomCode, missionId);
          } catch (startErr) {
            const errText = String(startErr && startErr.stack ? startErr.stack : startErr);
            log("match_start_failure", { roomCode, error: errText });
            sendMatchError(ws, "Match start failed: " + errText, { roomCode });
          }
          return;
        }

        if (type === "request_cutscene_skip") {
          const roomCode = normalizeRoomCode(msg.roomCode || clientRoomBySocket.get(ws));
          const phase = String(msg.phase || "");
          const allowed = new Set([
            "intro_cutscene",
            "post_level1_cutscene",
            "post_level2_cutscene",
            "post_level3_cutscene",
            "post_level4_cutscene",
            "post_level5_cutscene"
          ]);
          if (!roomCode) {
            sendMatchError(ws, "Invalid room code.");
            return;
          }
          const room = rooms.get(roomCode);
          if (!room) {
            sendMatchError(ws, "Room not found.", { roomCode });
            return;
          }
          const requesterInRoom = room.slots.includes(ws);
          if (!requesterInRoom) {
            log("cutscene_skip_rejected_not_in_room", { roomCode });
            sendMatchError(ws, "Requester is not part of this match room.", { roomCode });
            return;
          }
          if (!allowed.has(phase)) {
            sendMatchError(ws, "Invalid cutscene phase.", { roomCode });
            return;
          }
          if (!activeMatch || activeMatch.roomCode !== roomCode || activeMatch.phase !== RoomPhase.RUNNING) {
            sendMatchError(ws, "No active running match for cutscene skip.", { roomCode });
            return;
          }
          try {
            log("cutscene_skip_request", { roomCode, phase, matchId: activeMatch.matchId });
            updateCutsceneState(activeMatch, phase);
            emitSnapshot(activeMatch, "cutscene_skip");
          } catch (skipErr) {
            const errText = String(skipErr && skipErr.stack ? skipErr.stack : skipErr);
            log("cutscene_skip_failure", { roomCode, phase, error: errText });
            sendMatchError(ws, "Cutscene skip failed.", { roomCode, phase, code: "cutscene_skip_failure" });
          }
          return;
        }
      } catch (err) {
        log("message_handler_error", { error: String(err && err.stack ? err.stack : err) });
      }
    });

    ws.on("error", (err) => {
      log("socket_error", { error: String(err && err.stack ? err.stack : err) });
    });

    ws.on("close", (code, reasonRaw) => {
      clearTimeout(readyTimer);
      const reason = reasonRaw ? String(reasonRaw) : "";
      log("connection_close", { code, reason });
      const meta = getClientMeta(ws);
      if (meta) meta.connectionState = "disconnected";
      removeClientFromRoom(ws);
      clients.delete(ws);
    });
  });

  nodeSetInterval(() => {
    for (const ws of clients.keys()) {
      if (ws.readyState !== 1) continue;
      if (!ws._isAlive) {
        try {
          ws.terminate();
        } catch (_) {}
        continue;
      }
      ws._isAlive = false;
      try {
        ws.ping();
      } catch (_) {}
    }
  }, WS_HEARTBEAT_MS);

  nodeSetInterval(() => {
    const now = Date.now();
    for (const ws of clients.keys()) {
      if ((ws._lastSeenAt || 0) + CLIENT_IDLE_TIMEOUT_MS < now) {
        try {
          ws.close();
        } catch (_) {}
      }
    }
    if (!activeMatch || activeMatch.phase !== RoomPhase.RUNNING) return;
    try {
      if (activeMatch.state && activeMatch.state.cutsceneActive && (Date.now() - activeMatch.startedAt) >= CUTSCENE_AUTO_END_MS) {
        activeMatch.state.cutsceneActive = false;
        activeMatch.state.cutscenePhase = null;
        activeMatch.state.gamePhase = "playing";
        log("cutscene_auto_end", {
          roomCode: activeMatch.roomCode,
          matchId: activeMatch.matchId,
          elapsedMs: Date.now() - activeMatch.startedAt
        });
      }
      emitSnapshot(activeMatch, "tick");
    } catch (err) {
      panicMatch("snapshot_tick_failure", { error: String(err && err.stack ? err.stack : err) });
    }
  }, Math.round(1000 / NH_TICK_HZ));

  process.on("uncaughtException", (err) => {
    log("uncaughtException", { error: String(err && err.stack ? err.stack : err) });
  });
  process.on("unhandledRejection", (err) => {
    log("unhandledRejection", { error: String(err && err.stack ? err.stack : err) });
  });

  server.listen(PORT, () => {
    console.log(`Neural Heist server listening on ${PORT} (WebSocket path /ws, health GET /health)`);
  });
}

main();
