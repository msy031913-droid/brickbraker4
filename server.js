"use strict";
/**
 * Ultimate Brick Breaker - 대결 서버
 *  - 같은 포트에서 게임(index.html)과 WebSocket 을 함께 제공합니다.
 *  - 실행:  npm install  →  npm start   (기본 포트 3000, PORT 환경변수로 변경)
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 3000;
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 4;
const MATCH_LIMIT_MS = (Number(process.env.MATCH_LIMIT_SEC) || 600) * 1000; // 대결 최대 시간 (기본 10분)
const COUNTDOWN_MS = 2800;                                                  // 클라이언트 3-2-1-START 연출 시간
const MIN_PLAYERS_TO_START = Number(process.env.MIN_PLAYERS) || 2;

const INDEX = path.join(__dirname, "index.html");
const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/" || url === "/index.html") {
    fs.readFile(INDEX, (err, data) => {
      if (err) { res.writeHead(500); return res.end("index.html 을 찾을 수 없습니다."); }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
      res.end(data);
    });
  } else if (url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  } else { res.writeHead(404); res.end("Not found"); }
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });
const rooms = new Map();
let nextId = 1;

const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const cleanName = n => String(n || "").replace(/[<>&"'`]/g, "").trim().slice(0, 12) || "Player";
const cleanCode = c => String(c || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12);

function publicRoom(room) {
  return {
    t: "room",
    code: room.code,
    state: room.state,
    fair: room.fair,
    hostId: room.hostId,
    players: [...room.players.values()].map(p => ({
      id: p.id, name: p.name, status: p.status, stage: p.stage, ms: p.ms
    }))
  };
}
function broadcast(room, obj) { for (const p of room.players.values()) send(p.ws, obj); }
function pushRoom(room) { broadcast(room, publicRoom(room)); }

function leaveRoom(ws) {
  const room = ws.room; if (!room) return;
  const p = room.players.get(ws.pid);
  ws.room = null;
  if (!p) return;
  if (room.state === "playing" && p.status === "alive") markDead(room, p, p.stage);
  room.players.delete(p.id);
  if (room.players.size === 0) { clearTimeout(room.timer); rooms.delete(room.code); return; }
  if (room.hostId === p.id) room.hostId = room.players.values().next().value.id;
  if (room.state === "playing") checkFinish(room);
  pushRoom(room);
}

function markDead(room, p, stage) {
  if (p.status !== "alive") return;
  p.status = "dead";
  p.ms = Math.max(0, Date.now() - room.startedAt - COUNTDOWN_MS);   // 서버 기준 생존 시간
  if (Number.isInteger(stage) && stage >= p.stage && stage < 10000) p.stage = stage;
}

function checkFinish(room) {
  if (room.state !== "playing") return;
  if ([...room.players.values()].some(p => p.status === "alive")) return;
  finish(room);
}

function finish(room) {
  clearTimeout(room.timer);
  const list = [...room.players.values()];
  list.forEach(p => { if (p.status === "alive") { p.status = "dead"; p.ms = MATCH_LIMIT_MS; } });
  const row = p => ({ id: p.id, name: p.name, ms: p.ms, stage: p.stage });
  const byTime = list.map(row).sort((a, b) => b.ms - a.ms || b.stage - a.stage);
  const byStage = list.map(row).sort((a, b) => b.stage - a.stage || b.ms - a.ms);
  room.state = "lobby";
  broadcast(room, { t: "results", byTime, byStage, tieTime: arr0(byTime, "ms"), tieStage: arr0(byStage, "stage"), limitHit: list.some(p => p.ms >= MATCH_LIMIT_MS) });
  list.forEach(p => { p.status = "lobby"; });
  pushRoom(room);
  function arr0(a, k) { return a.length > 1 && a[0][k] === a[1][k]; }
}

wss.on("connection", ws => {
  ws.isAlive = true;
  ws.pid = nextId++;
  ws.room = null;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.t !== "string") return;
    const room = ws.room;

    switch (m.t) {
      case "join": {
        if (room) return;
        const code = cleanCode(m.code);
        if (code.length < 3) return send(ws, { t: "error", msg: "팀 코드는 영문/숫자 3~12자로 입력해 주세요." });
        let r = rooms.get(code);
        if (!r) {
          r = { code, players: new Map(), hostId: ws.pid, state: "lobby", fair: true, seed: 0, startedAt: 0, timer: null, lastPush: 0 };
          rooms.set(code, r);
        }
        if (r.state === "playing") return send(ws, { t: "error", msg: "이미 대결이 진행 중인 방입니다. 끝난 뒤 다시 입장해 주세요." });
        if (r.players.size >= MAX_PLAYERS) return send(ws, { t: "error", msg: `방이 가득 찼습니다. (최대 ${MAX_PLAYERS}명)` });
        r.players.set(ws.pid, { id: ws.pid, ws, name: cleanName(m.name), status: "lobby", stage: 1, ms: 0 });
        ws.room = r;
        send(ws, { t: "joined", id: ws.pid, code });
        pushRoom(r);
        break;
      }
      case "fair": {
        if (!room || room.hostId !== ws.pid || room.state !== "lobby") return;
        room.fair = !!m.v; pushRoom(room); break;
      }
      case "start": {
        if (!room || room.hostId !== ws.pid || room.state !== "lobby") return;
        if (room.players.size < MIN_PLAYERS_TO_START) return send(ws, { t: "error", msg: `최소 ${MIN_PLAYERS_TO_START}명이 필요합니다.` });
        room.state = "playing";
        room.seed = (Math.random() * 2 ** 31) >>> 0;
        room.startedAt = Date.now();
        room.players.forEach(p => { p.status = "alive"; p.stage = 1; p.ms = 0; });
        clearTimeout(room.timer);
        room.timer = setTimeout(() => finish(room), MATCH_LIMIT_MS + COUNTDOWN_MS);
        broadcast(room, { t: "start", seed: room.seed, fair: room.fair, limitMs: MATCH_LIMIT_MS });
        pushRoom(room);
        break;
      }
      case "progress": {
        if (!room || room.state !== "playing") return;
        const p = room.players.get(ws.pid);
        if (!p || p.status !== "alive") return;
        let changed = false;
        if (Number.isInteger(m.stage) && m.stage > p.stage && m.stage < 10000) { p.stage = m.stage; changed = true; }
        const now = Date.now();
        if (changed || now - room.lastPush > 250) { room.lastPush = now; pushRoom(room); }
        break;
      }
      case "dead": {
        if (!room || room.state !== "playing") return;
        const p = room.players.get(ws.pid);
        if (!p) return;
        markDead(room, p, m.stage);
        pushRoom(room);
        checkFinish(room);
        break;
      }
      case "leave": leaveRoom(ws); break;
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => {});
});

setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 30000);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🎮 Ultimate Brick Breaker 대결 서버 실행 중 (포트 ${PORT})`);
  console.log(`   이 컴퓨터:        http://localhost:${PORT}`);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || []) if (i.family === "IPv4" && !i.internal) console.log(`   같은 와이파이 친구: http://${i.address}:${PORT}`);
  console.log(`   최대 ${MAX_PLAYERS}명 / 최소 ${MIN_PLAYERS_TO_START}명 / 제한 시간 ${MATCH_LIMIT_MS / 1000}초\n`);
});
