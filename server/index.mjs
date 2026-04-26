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
    if (connected <= 0) {
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
      const json = JSON.stringify(payload);
      for (const ws of clients.keys()) {
        if (ws.readyState === 1) ws.send(json);
      }
    };
    RT.bootstrapDedicatedRoom({
      missionId: mid,
      slotCount: sc,
      onSnapshot: broadcastSnapshot
    });
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
          log("assigned slot", { slot });
          return;
        }

        if (msg.type === (Net.Msg.ROOM_CREATE || "room_create") || msg.type === "createRoom") {
          let code = randomRoomCode();
          while (rooms.has(code)) code = randomRoomCode();
          rooms.set(code, { roomCode: code, slots: new Array(activeSlotCount).fill(null) });
          log("room created", { roomCode: code, slotCount: activeSlotCount });
          const result = assignClientToRoom(ws, code, true);
          log("room create assign", result);
          sendRoomJoinResult(ws, result);
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
          sendRoomJoinResult(ws, result);
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
      log("connection closed", { code, reason });
      removeClientFromRoom(ws);
      clients.delete(ws);
      syncPresence();
    });
  });

  const dt = 1 / NH_TICK_HZ;
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
      RT.serverTick(dt);
    } finally {
      NetworkCoordinator._authoritativeRemoteInputs = null;
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
