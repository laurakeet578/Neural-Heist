/**
 * Authoritative multiplayer host for Neural Heist (Render / any Node host).
 * Serves GET /health and WebSocket /ws using the same simulation as the browser bundle.
 */
import http from "http";
import fs from "fs";
import path from "path";
import vm from "vm";
import { fileURLToPath } from "url";
import { setInterval as nodeSetInterval } from "timers";
import { WebSocketServer } from "ws";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const HTML_PATH = path.join(ROOT, "index.html");

const PORT = Number(process.env.PORT || 8787);
const NH_MISSION_ID = Number(process.env.NH_MISSION_ID || 1);
const NH_MAX_MISSION = Math.max(1, Number(process.env.NH_MAX_MISSION || 6));
const NH_SLOT_COUNT = Math.max(2, Math.min(4, Number(process.env.NH_SLOT_COUNT || 2)));
const NH_TICK_HZ = Math.max(8, Math.min(60, Number(process.env.NH_TICK_HZ || 20)));
const CLIENT_IDLE_TIMEOUT_MS = Math.max(12000, Number(process.env.NH_CLIENT_IDLE_TIMEOUT_MS || 45000));
const WS_HEARTBEAT_MS = Math.max(5000, Number(process.env.NH_WS_HEARTBEAT_MS || 15000));

function extractGameScript(html) {
  const re = /<script>([\s\S]*?)<\/script>/g;
  let m;
  let last = null;
  while ((m = re.exec(html)) !== null) last = m[1];
  if (!last) throw new Error("No <script> block found in index.html");
  return last;
}

function installHeadlessGlobals() {
  const hintEl = { textContent: "" };
  const chain = () => chain;
  const ctxStub = new Proxy(
    {},
    {
      get(_t, prop) {
        if (prop === "canvas") return canvas;
        return chain;
      }
    }
  );
  const canvas = {
    width: 1280,
    height: 720,
    style: {},
    tabindex: 0,
    focus() {},
    getBoundingClientRect() {
      return { left: 0, top: 0, width: 1280, height: 720 };
    },
    addEventListener() {},
    removeEventListener() {},
    getContext() {
      return ctxStub;
    }
  };
  const document = {
    getElementById(id) {
      if (id === "c") return canvas;
      if (id === "hint") return hintEl;
      if (id === "breachBtn") return { addEventListener() {} };
      return null;
    },
    addEventListener() {},
    createElement() {
      return { width: 64, height: 64, getContext: () => null };
    }
  };
  const store = Object.create(null);
  const localStorage = {
    getItem(k) {
      return store[k] ?? null;
    },
    setItem(k, v) {
      store[k] = String(v);
    }
  };

  globalThis.__NEURAL_HEIST_HEADLESS_SERVER__ = true;
  globalThis.window = globalThis;
  globalThis.document = document;
  globalThis.localStorage = localStorage;
  globalThis.requestAnimationFrame = () => 0;
  globalThis.cancelAnimationFrame = () => {};
  globalThis.setInterval = () => 0;
  globalThis.clearInterval = () => {};
  globalThis.AudioContext = undefined;
  globalThis.webkitAudioContext = undefined;
}

function loadGameRuntime() {
  installHeadlessGlobals();
  const html = fs.readFileSync(HTML_PATH, "utf8");
  const code = extractGameScript(html);
  vm.runInThisContext(code, { filename: "index-bundle.js" });
  const RT = globalThis.__NeuralHeistServerRuntime__;
  if (!RT) throw new Error("Game bundle did not export __NeuralHeistServerRuntime__");
  return RT;
}

function neutralInput() {
  return { left: false, right: false, up: false, down: false, sprint: false };
}

function main() {
  const RT = loadGameRuntime();
  const { Net, GameStateManager, AuthoritativeSession, NetworkCoordinator } = RT;

  /** @type {Map<import('ws').WebSocket, { slot: number }>} */
  const clients = new Map();
  /** @type {Map<string, { roomCode: string, slots: Array<import('ws').WebSocket | null> }>} */
  const rooms = new Map();
  /** @type {Map<import('ws').WebSocket, string>} */
  const roomByClient = new Map();
  let roomBootstrapped = false;
  let activeMissionId = NH_MISSION_ID;
  let activeSlotCount = NH_SLOT_COUNT;
  /** @type {null | ((payload: object) => void)} */
  let broadcastSnapshot = null;
  const RoomPhase = {
    LOBBY: "lobby",
    STARTING: "starting",
    RUNNING: "running",
    ENDED: "ended"
  };
  /** @type {null | { roomCode: string, matchId: string, phase: string, startTick: number, firstSnapshotSent: boolean, firstSnapshotTick: number, ackedSockets: Set<import('ws').WebSocket> }} */
  let activeMatch = null;

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

  function removeClientFromRoom(ws) {
    const code = roomByClient.get(ws);
    if (!code) return;
    roomByClient.delete(ws);
    const room = rooms.get(code);
    if (!room) return;
    for (let i = 0; i < room.slots.length; i++) {
      if (room.slots[i] === ws) room.slots[i] = null;
    }
    const connected = room.slots.filter(Boolean).length;
    log("room player removed", { roomCode: code, connected });
    if (activeMatch && activeMatch.roomCode === code) {
      activeMatch.ackedSockets.delete(ws);
    }
    if (connected > 0) broadcastRoomState(code, { created: false });
    if (connected <= 0) {
      if (activeMatch && activeMatch.roomCode === code) activeMatch = null;
      rooms.delete(code);
      log("room deleted (empty)", { roomCode: code });
    }
  }

  function sendRoomJoinResult(ws, payload) {
    const out = {
      v: Net.PROTOCOL_VERSION,
      type: Net.Msg.ROOM_JOIN_RESULT || "room_join_result",
      ...payload
    };
    log("room join response", out);
    ws.send(JSON.stringify(out));
  }

  function broadcastRoomState(code, opts = {}) {
    const room = rooms.get(code);
    if (!room) return;
    const connectedSlots = room.slots.map(Boolean);
    const base = {
      ok: true,
      created: !!opts.created,
      roomCode: code,
      phase: room.phase || RoomPhase.LOBBY,
      matchId: room.matchId || null,
      slotCount: room.slots.length,
      connectedSlots,
      connectedCount: connectedSlots.filter(Boolean).length
    };
    for (let i = 0; i < room.slots.length; i++) {
      const sock = room.slots[i];
      if (!sock || sock.readyState !== 1) continue;
      sendRoomJoinResult(sock, { ...base, slot: i });
    }
  }

  function broadcastRoomMessage(code, payload) {
    const room = rooms.get(code);
    if (!room) return;
    const json = JSON.stringify(payload);
    for (let i = 0; i < room.slots.length; i++) {
      const sock = room.slots[i];
      if (!sock || sock.readyState !== 1) continue;
      try { sock.send(json); } catch (e) {}
    }
  }

  function failActiveMatch(reason) {
    if (!activeMatch) return;
    const code = activeMatch.roomCode;
    const room = rooms.get(code);
    log("match fail-fast", { roomCode: code, matchId: activeMatch.matchId, reason });
    if (room) {
      room.phase = RoomPhase.ENDED;
      broadcastRoomMessage(code, {
        v: Net.PROTOCOL_VERSION,
        type: Net.Msg.ERROR,
        message: "Match aborted: " + reason,
        roomCode: code,
        matchId: activeMatch.matchId
      });
      room.phase = RoomPhase.LOBBY;
      room.matchId = null;
    }
    activeMatch = null;
  }

  function pushSnapshotNow(reason) {
    if (!GameStateManager.world) {
      log("pushSnapshotNow skipped (no world)", { reason });
      return;
    }
    try {
      AuthoritativeSession.broadcastSnapshot(GameStateManager.world);
      log("snapshot pushed", {
        reason,
        clients: clients.size,
        tick: AuthoritativeSession.serverTick | 0
      });
    } catch (err) {
      log("pushSnapshotNow error", { reason, error: String(err && err.stack ? err.stack : err) });
    }
  }

  function assignClientToRoom(ws, code, created) {
    const room = rooms.get(code);
    if (!room) return { ok: false, error: "Room not found." };
    if (roomByClient.get(ws) && roomByClient.get(ws) !== code) removeClientFromRoom(ws);
    let slot = room.slots.indexOf(ws);
    if (slot < 0) slot = room.slots.findIndex((sock) => !sock);
    if (slot < 0) return { ok: false, error: "Room is full." };
    room.slots[slot] = ws;
    roomByClient.set(ws, code);
    const connectedSlots = room.slots.map(Boolean);
    return {
      ok: true,
      created: !!created,
      roomCode: code,
      slot,
      slotCount: room.slots.length,
      connectedSlots,
      connectedCount: connectedSlots.filter(Boolean).length
    };
  }

  /** First HELLO picks mission + slot count so browser clients stay in sync with the host world. */
  function ensureRoom(hello) {
    if (roomBootstrapped) return;
    const mid = hello && hello.missionId != null ? clampMissionId(hello.missionId) : NH_MISSION_ID;
    const sc = hello && hello.slotCount != null ? clampSlotCount(hello.slotCount) : NH_SLOT_COUNT;
    activeMissionId = mid;
    activeSlotCount = sc;
    broadcastSnapshot = (payload) => {
      const runningCode = activeMatch ? activeMatch.roomCode : null;
      if (runningCode) {
        const room = rooms.get(runningCode);
        if (!room) {
          failActiveMatch("running room not found during snapshot");
          return;
        }
        room.phase = room.phase || RoomPhase.STARTING;
      }
      if (!GameStateManager.world) {
        failActiveMatch("world missing before snapshot generation");
        return;
      }
      if (!Array.isArray(AuthoritativeSession.players) || AuthoritativeSession.players.length <= 0) {
        failActiveMatch("players missing before snapshot generation");
        return;
      }
      const outPayload = runningCode
        ? { ...payload, roomCode: runningCode, matchId: activeMatch.matchId }
        : payload;
      log("snapshot generated", {
        roomCode: runningCode,
        matchId: activeMatch ? activeMatch.matchId : null,
        tick: payload && payload.tick != null ? payload.tick : null
      });
      let json;
      try {
        json = JSON.stringify(outPayload);
        log("snapshot serialized", {
          roomCode: runningCode,
          matchId: activeMatch ? activeMatch.matchId : null,
          size: json.length
        });
      } catch (err) {
        log("snapshot stringify error", { error: String(err && err.stack ? err.stack : err) });
        failActiveMatch("snapshot serialization failed");
        return;
      }
      let sent = 0;
      const targets = [];
      if (runningCode) {
        const room = rooms.get(runningCode);
        if (!room) {
          failActiveMatch("running room missing during broadcast");
          return;
        }
        for (const sock of room.slots) if (sock) targets.push(sock);
      } else {
        for (const ws of clients.keys()) targets.push(ws);
      }
      for (const ws of targets) {
        if (ws.readyState !== 1) continue;
        try {
          ws.send(json);
          sent++;
        } catch (err) {
          log("snapshot send error", { error: String(err && err.stack ? err.stack : err) });
        }
      }
      log("snapshot broadcast sent", {
        roomCode: runningCode,
        matchId: activeMatch ? activeMatch.matchId : null,
        recipients: sent
      });
      if (activeMatch && activeMatch.phase === RoomPhase.STARTING) {
        if (sent <= 0) {
          failActiveMatch("first snapshot had zero recipients");
          return;
        }
        activeMatch.firstSnapshotSent = true;
        activeMatch.firstSnapshotTick = AuthoritativeSession.serverTick | 0;
        activeMatch.phase = RoomPhase.RUNNING;
        const room = rooms.get(activeMatch.roomCode);
        if (room) room.phase = RoomPhase.RUNNING;
      }
    };
    RT.bootstrapDedicatedRoom({
      missionId: mid,
      slotCount: sc,
      onSnapshot: broadcastSnapshot
    });
    log("server game state initialized", { missionId: mid, slotCount: sc });
    roomBootstrapped = true;
  }

  function syncPresence() {
    const w = GameStateManager.world;
    if (!w || !NetworkCoordinator.connectedSlots) return;
    for (let s = 0; s < NetworkCoordinator.connectedSlots.length; s++) {
      let on = false;
      for (const meta of clients.values()) {
        if (meta.slot === s) {
          on = true;
          break;
        }
      }
      NetworkCoordinator.connectedSlots[s] = on;
    }
    if (w) AuthoritativeSession.broadcastSnapshot(w);
  }

  const server = http.createServer((req, res) => {
    if (req.url === "/health" || req.url === "/") {
      const connectedSlots = [];
      for (const meta of clients.values()) connectedSlots.push(meta.slot);
      const body = {
        ok: true,
        roomBootstrapped,
        missionId: activeMissionId,
        slotCount: activeSlotCount,
        connectedPlayers: connectedSlots.length,
        connectedSlots: connectedSlots.sort((a, b) => a - b)
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
    ws._lastSeenAt = Date.now();
    ws._isAlive = true;
    const remote = req?.socket?.remoteAddress || "unknown";
    log("connection open", { remote });
    ws.on("pong", () => {
      ws._isAlive = true;
      ws._lastSeenAt = Date.now();
    });
    ws.on("message", (raw) => {
      try {
        ws._lastSeenAt = Date.now();
        log("raw message", { raw: String(raw) });
        let msg;
        try {
          msg = JSON.parse(String(raw));
        } catch (parseErr) {
          log("message parse error", { error: String(parseErr && parseErr.stack ? parseErr.stack : parseErr), raw: String(raw) });
          return;
        }
        if (!msg || msg.v !== Net.PROTOCOL_VERSION) return;
        if (msg.type) log("message received", { type: msg.type, payload: msg });

        if (msg.type === Net.Msg.PING) {
          ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.PONG, t: msg.t }));
          return;
        }

        if (msg.type === Net.Msg.HELLO) {
          ensureRoom(msg);
          const meta = clients.get(ws);
          if (meta && meta.slot != null) return;

          const maxSlots = Math.min(
            activeSlotCount,
            NetworkCoordinator.connectedSlots?.length || activeSlotCount
          );

          let slot = -1;
          const used = new Set();
          for (const m of clients.values()) used.add(m.slot);
          for (let i = 0; i < maxSlots; i++) {
            if (!used.has(i)) {
              slot = i;
              break;
            }
          }
          if (slot < 0) {
            ws.send(JSON.stringify({
              v: Net.PROTOCOL_VERSION,
              type: Net.Msg.ERROR,
              message: "Room is full."
            }));
            ws.close(1008, "room full");
            return;
          }
          clients.set(ws, { slot });
          syncPresence();
          ws.send(JSON.stringify({
            v: Net.PROTOCOL_VERSION,
            type: Net.Msg.WELCOME,
            slot,
            missionId: activeMissionId,
            slotCount: maxSlots
          }));
          log("player joined game state", { slot });
          pushSnapshotNow("hello_welcome");
          return;
        }

        if (msg.type === (Net.Msg.ROOM_CREATE || "room_create") || msg.type === "createRoom") {
          let code = randomRoomCode();
          while (rooms.has(code)) code = randomRoomCode();
          rooms.set(code, {
            roomCode: code,
            slots: new Array(activeSlotCount).fill(null),
            phase: RoomPhase.LOBBY,
            matchId: null
          });
          log("room created", { roomCode: code, slotCount: activeSlotCount });
          const result = assignClientToRoom(ws, code, true);
          log("room create assign", result);
          if (!result.ok) sendRoomJoinResult(ws, result);
          else broadcastRoomState(code, { created: true });
          return;
        }

        if (msg.type === (Net.Msg.ROOM_JOIN || "room_join") || msg.type === "joinRoom") {
          const code = normalizeRoomCode(msg.roomCode);
          log("room join request", { requested: msg.roomCode, normalized: code });
          if (!code) {
            sendRoomJoinResult(ws, { ok: false, error: "Invalid room code." });
            return;
          }
          const room = rooms.get(code);
          if (!room) {
            log("room not found", { roomCode: code });
            sendRoomJoinResult(ws, { ok: false, error: "Room not found." });
            return;
          }
          log("room found", { roomCode: code, connectedCount: room.slots.filter(Boolean).length });
          const result = assignClientToRoom(ws, code, false);
          log("room player added", result);
          if (!result.ok) sendRoomJoinResult(ws, result);
          else broadcastRoomState(code, { created: false });
          return;
        }

        if (msg.type === (Net.Msg.START_MATCH || "start_match") || msg.type === "startMatch") {
          const code = normalizeRoomCode(msg.roomCode || roomByClient.get(ws));
          log("server received start request", { roomCode: code, missionId: msg.missionId });
          if (!code) {
            ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.ERROR, message: "Invalid room code." }));
            return;
          }
          const room = rooms.get(code);
          if (!room) {
            ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.ERROR, message: "Room not found." }));
            return;
          }
          if (activeMatch && activeMatch.roomCode !== code && (activeMatch.phase === RoomPhase.STARTING || activeMatch.phase === RoomPhase.RUNNING)) {
            ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.ERROR, message: "Another room match is already running." }));
            return;
          }
          const hostSock = room.slots[0];
          if (hostSock !== ws) {
            ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.ERROR, message: "Only host can start match." }));
            return;
          }
          const connectedCount = room.slots.filter(Boolean).length;
          if (connectedCount < 2) {
            ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.ERROR, message: "Need at least 2 players." }));
            return;
          }
          if (activeMatch && activeMatch.roomCode === code && (activeMatch.phase === RoomPhase.STARTING || activeMatch.phase === RoomPhase.RUNNING)) {
            ws.send(JSON.stringify({
              v: Net.PROTOCOL_VERSION,
              type: Net.Msg.GAME_START || "game_start",
              roomCode: code,
              missionId: activeMissionId,
              connectedCount,
              matchId: activeMatch.matchId
            }));
            return;
          }
          const missionId = clampMissionId(msg.missionId != null ? msg.missionId : activeMissionId);
          console.log("GAME START - SERVER ENTERED");
          const matchId = randomMatchId();
          room.phase = RoomPhase.STARTING;
          room.matchId = matchId;
          activeMatch = {
            roomCode: code,
            matchId,
            phase: RoomPhase.STARTING,
            startTick: AuthoritativeSession.serverTick | 0,
            firstSnapshotSent: false,
            firstSnapshotTick: -1,
            ackedSockets: new Set()
          };
          const out = {
            v: Net.PROTOCOL_VERSION,
            type: Net.Msg.GAME_START || "game_start",
            roomCode: code,
            missionId,
            connectedCount,
            matchId
          };
          log("room broadcast triggered", out);
          broadcastRoomMessage(code, out);
          pushSnapshotNow("game_start");
          return;
        }

        if (msg.type === (Net.Msg.CUTSCENE_SKIP_REQUEST || "cutscene_skip_request") || msg.type === "requestSkipCutscene") {
          const code = normalizeRoomCode(msg.roomCode || roomByClient.get(ws));
          const phase = String(msg.phase || "");
          log("skip request received", { roomCode: code, phase });
          if (!code) return;
          const room = rooms.get(code);
          if (!room) return;
          const allowed = new Set([
            "intro_cutscene",
            "post_level1_cutscene",
            "post_level2_cutscene",
            "post_level3_cutscene",
            "post_level4_cutscene",
            "post_level5_cutscene"
          ]);
          if (!allowed.has(phase)) return;
          const out = {
            v: Net.PROTOCOL_VERSION,
            type: Net.Msg.CUTSCENE_SKIPPED || "cutscene_skipped",
            roomCode: code,
            phase,
            matchId: activeMatch && activeMatch.roomCode === code ? activeMatch.matchId : null
          };
          log("server cutscene state change", out);
          broadcastRoomMessage(code, out);
          log("broadcast event sent", { type: out.type, roomCode: code, phase });
          return;
        }

        if (msg.type === (Net.Msg.SNAPSHOT_ACK || "snapshot_ack")) {
          if (!activeMatch) return;
          if (String(msg.matchId || "") !== activeMatch.matchId) return;
          const roomCode = roomByClient.get(ws);
          if (roomCode !== activeMatch.roomCode) return;
          activeMatch.ackedSockets.add(ws);
          log("snapshot ack received", {
            roomCode: activeMatch.roomCode,
            matchId: activeMatch.matchId,
            acked: activeMatch.ackedSockets.size
          });
          return;
        }

        const meta = clients.get(ws);
        if (!meta || meta.slot == null) return;
        const slot = meta.slot;

        if (msg.type === Net.Msg.INPUT && msg.payload) {
          const keys = msg.payload.keys || msg.payload;
          ws._lastInput = {
            left: !!(keys.left || keys.a || keys.arrowleft),
            right: !!(keys.right || keys.d || keys.arrowright),
            up: !!(keys.up || keys.w || keys.arrowup),
            down: !!(keys.down || keys.s || keys.arrowdown),
            sprint: !!(keys.sprint || keys.shift)
          };
          return;
        }

        if (msg.type === Net.Msg.ACTION) {
          const seq = msg.seq | 0;
          const prevSeq = ws._lastActionSeq | 0;
          if (seq > 0 && prevSeq > 0 && seq <= prevSeq) return;
          if (seq > 0) ws._lastActionSeq = seq;
          const pay = msg.payload || {};
          const actSlot = msg.slot != null ? msg.slot | 0 : slot;
          if (actSlot !== slot) return;
          RT.applyRemoteMissionAction(actSlot, pay);
          if (GameStateManager.world) AuthoritativeSession.broadcastSnapshot(GameStateManager.world);
          return;
        }
      } catch (err) {
        log("message handler error", { error: String(err && err.stack ? err.stack : err) });
      }
    });

    ws.on("error", (err) => {
      log("socket error", { error: String(err && err.stack ? err.stack : err) });
    });
    ws.on("close", (code, reasonRaw) => {
      const reason = reasonRaw ? String(reasonRaw) : "";
      log("player disconnected reason", { code, reason });
      removeClientFromRoom(ws);
      clients.delete(ws);
      syncPresence();
    });
  });

  const dt = 1 / NH_TICK_HZ;
  let warnedMissingWorld = false;
  let lastSnapshotLogAt = 0;
  let lastRebootstrapAt = 0;
  nodeSetInterval(() => {
    for (const ws of clients.keys()) {
      if (ws.readyState !== 1) continue;
      if (!ws._isAlive) {
        try { ws.terminate(); } catch (e) {}
        continue;
      }
      ws._isAlive = false;
      try { ws.ping(); } catch (e) {}
    }
  }, WS_HEARTBEAT_MS);

  nodeSetInterval(() => {
    if (!roomBootstrapped) return;
    const now = Date.now();
    for (const ws of clients.keys()) {
      if ((ws._lastSeenAt || 0) + CLIENT_IDLE_TIMEOUT_MS < now) {
        try { ws.close(); } catch (e) {}
      }
    }
    const n = AuthoritativeSession.slotCount || NH_SLOT_COUNT;
    const arr = [];
    for (let i = 0; i < n; i++) arr.push(neutralInput());
    for (const [sock, meta] of clients) {
      if (meta.slot != null && meta.slot < n && sock._lastInput) {
        arr[meta.slot] = sock._lastInput;
      }
    }
    NetworkCoordinator._authoritativeRemoteInputs = arr;
    try {
      try {
        RT.serverTick(dt);
      } catch (tickErr) {
        log("serverTick error", { error: String(tickErr && tickErr.stack ? tickErr.stack : tickErr) });
      }
      if (activeMatch && activeMatch.phase === RoomPhase.STARTING) {
        const ticksSinceStart = (AuthoritativeSession.serverTick | 0) - (activeMatch.startTick | 0);
        if (!activeMatch.firstSnapshotSent && ticksSinceStart > 2) {
          failActiveMatch("first snapshot not delivered within 2 ticks");
        }
      }
      // Safety net: always emit a snapshot each server tick while world is active.
      if (GameStateManager.world) {
        warnedMissingWorld = false;
        try {
          AuthoritativeSession.broadcastSnapshot(GameStateManager.world);
          const nowMs = Date.now();
          if (nowMs - lastSnapshotLogAt > 10000) {
            lastSnapshotLogAt = nowMs;
            log("snapshot broadcast heartbeat", {
              clients: clients.size,
              roomBootstrapped,
              serverTick: AuthoritativeSession.serverTick | 0
            });
            if (activeMatch && activeMatch.phase === RoomPhase.RUNNING) {
              const room = rooms.get(activeMatch.roomCode);
              const connected = room ? room.slots.filter(Boolean).length : 0;
              if (activeMatch.ackedSockets.size < connected) {
                log("critical desync warning: missing snapshot ACK", {
                  roomCode: activeMatch.roomCode,
                  matchId: activeMatch.matchId,
                  acked: activeMatch.ackedSockets.size,
                  connected
                });
              }
            }
          }
        } catch (snapErr) {
          log("snapshot broadcast error", { error: String(snapErr && snapErr.stack ? snapErr.stack : snapErr) });
        }
      } else {
        if (!warnedMissingWorld) {
          warnedMissingWorld = true;
          log("world missing during tick", { roomBootstrapped, clients: clients.size });
        }
        const nowMs = Date.now();
        if (roomBootstrapped && nowMs - lastRebootstrapAt > 5000) {
          lastRebootstrapAt = nowMs;
          try {
            RT.bootstrapDedicatedRoom({
              missionId: activeMissionId,
              slotCount: activeSlotCount,
              onSnapshot: broadcastSnapshot
            });
            log("world rebootstrap attempted", { missionId: activeMissionId, slotCount: activeSlotCount });
            pushSnapshotNow("world_rebootstrap");
          } catch (rebErr) {
            log("world rebootstrap error", { error: String(rebErr && rebErr.stack ? rebErr.stack : rebErr) });
          }
        }
      }
    } finally {
      NetworkCoordinator._authoritativeRemoteInputs = null;
    }
  }, Math.round(1000 / NH_TICK_HZ));

  nodeSetInterval(() => {
    if (!roomBootstrapped || clients.size <= 0) return;
    pushSnapshotNow("periodic_fallback");
  }, 1000);

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
