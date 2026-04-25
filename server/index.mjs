/**
 * Authoritative multiplayer host for Neural Heist (Render / any Node host).
 * Serves GET /health and WebSocket /ws using the same simulation as the browser bundle.
 */
import http from "http";
import fs from "fs";
import path from "path";
import vm from "vm";
import { fileURLToPath } from "url";
import { WebSocketServer } from "ws";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const HTML_PATH = path.join(ROOT, "index.html");

const PORT = Number(process.env.PORT || 8787);
const NH_MISSION_ID = Number(process.env.NH_MISSION_ID || 1);
const NH_SLOT_COUNT = Math.max(2, Math.min(4, Number(process.env.NH_SLOT_COUNT || 2)));
const NH_TICK_HZ = Math.max(8, Math.min(60, Number(process.env.NH_TICK_HZ || 20)));

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
  let roomBootstrapped = false;
  /** @type {null | ((payload: object) => void)} */
  let broadcastSnapshot = null;

  function ensureRoom() {
    if (roomBootstrapped) return;
    broadcastSnapshot = (payload) => {
      const json = JSON.stringify(payload);
      for (const ws of clients.keys()) {
        if (ws.readyState === 1) ws.send(json);
      }
    };
    RT.bootstrapDedicatedRoom({
      missionId: NH_MISSION_ID,
      slotCount: NH_SLOT_COUNT,
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
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end(roomBootstrapped ? "ok room" : "ok");
      return;
    }
    res.writeHead(404);
    res.end("not found");
  });

  const wss = new WebSocketServer({ server, path: "/ws" });

  wss.on("connection", (ws) => {
    ws.on("message", (raw) => {
      let msg;
      try {
        msg = JSON.parse(String(raw));
      } catch {
        return;
      }
      if (!msg || msg.v !== Net.PROTOCOL_VERSION) return;

      if (msg.type === Net.Msg.PING) {
        ws.send(JSON.stringify({ v: Net.PROTOCOL_VERSION, type: Net.Msg.PONG, t: msg.t }));
        return;
      }

      if (msg.type === Net.Msg.HELLO) {
        ensureRoom();
        const meta = clients.get(ws);
        if (meta && meta.slot != null) return;

        const maxSlots = Math.min(
          NH_SLOT_COUNT,
          NetworkCoordinator.connectedSlots?.length || NH_SLOT_COUNT
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
          ws.close();
          return;
        }
        clients.set(ws, { slot });
        syncPresence();
        ws.send(JSON.stringify({
          v: Net.PROTOCOL_VERSION,
          type: Net.Msg.WELCOME,
          slot,
          missionId: NH_MISSION_ID,
          slotCount: maxSlots
        }));
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
        const pay = msg.payload || {};
        const actSlot = msg.slot != null ? msg.slot | 0 : slot;
        if (actSlot !== slot) return;
        RT.applyRemoteMissionAction(actSlot, pay);
        return;
      }
    });

    ws.on("close", () => {
      clients.delete(ws);
      syncPresence();
    });
  });

  const dt = 1 / NH_TICK_HZ;
  setInterval(() => {
    if (!roomBootstrapped) return;
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

  server.listen(PORT, () => {
    console.log(`Neural Heist server listening on ${PORT} (WebSocket path /ws, health GET /health)`);
  });
}

main();
