// World Map War & Create Map — 멀티 중계 서버 (Node.js)
// 실행: node relay.js  (포트: 환경변수 PORT, 기본 8787)
// 방 = 모드별 서버 1개 (1830 · 1910 · 1936 · 1962 · 현대). 첫 접속자가 방장(게임 계산), 나머지는 상태를 받아 본다.
// 방장이 나가면 가장 오래 있던 사람이 방장이 되고, 서버에 남은 마지막 상태에서 이어 간다. 아무도 없어도 상태는 남는다.
const http = require("http"), { WebSocketServer } = require("ws");
const PORT = +process.env.PORT || 8787, ROOMS = ["1830", "1910", "1936", "1962", "2014"], MAXP = +process.env.MAXP || 200;
const rooms = new Map(); for (const r of ROOMS) rooms.set(r, { id: r, socks: new Map(), host: "", snap: null, snapSeq: 0, deltas: [], nat: new Map(), names: new Map() });
let cidN = 0;
const send = (ws, m) => { try { if (ws.readyState === 1) ws.send(typeof m === "string" ? m : JSON.stringify(m)); } catch (e) {} };
const roster = (R) => [...R.socks.keys()].map((c) => ({ cid: c, name: R.names.get(c) || "?", nat: R.nat.get(c) || 0, host: c === R.host }));
const bcast = (R, m, except) => { const s = JSON.stringify(m); for (const [c, ws] of R.socks) if (c !== except) send(ws, s); };
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url.startsWith("/rooms")) { res.setHeader("content-type", "application/json"); return res.end(JSON.stringify(ROOMS.map((r) => { const R = rooms.get(r); return { id: r, players: R.socks.size, max: MAXP, running: !!R.snap || !!R.host }; }))); }
  res.end("World Map War relay OK");
});
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 * 1024 });
wss.on("connection", (ws, req) => {
  const u = new URL(req.url, "http://x"), R = rooms.get(u.searchParams.get("room") || "");
  if (!R) { send(ws, { t: "err", msg: "없는 서버" }); return ws.close(); }
  if (R.socks.size >= MAXP) { send(ws, { t: "err", msg: "서버가 꽉 찼습니다" }); return ws.close(); }
  const cid = "c" + (++cidN).toString(36) + Math.random().toString(36).slice(2, 6);
  R.socks.set(cid, ws); R.names.set(cid, (u.searchParams.get("name") || "플레이어").slice(0, 20));
  if (!R.host) R.host = cid;
  send(ws, { t: "welcome", cid, host: R.host, room: R.id, hasSnap: !!R.snap, players: roster(R) });
  if (R.host === cid && R.snap) { send(ws, { t: "snap", seq: R.snapSeq, data: R.snap }); for (const d of R.deltas) send(ws, d.raw); }
  if (R.host !== cid) send(R.socks.get(R.host), { t: "join", cid, name: R.names.get(cid) });
  bcast(R, { t: "players", players: roster(R) });
  ws.on("message", (buf) => {
    let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; }
    if (process.env.DEBUG) console.log(R.id, cid, m.t, buf.length);
    const isHost = cid === R.host;
    if (m.t === "snap" && isHost) { R.snap = m.data; R.snapSeq = m.seq; R.deltas = R.deltas.filter((d) => d.seq > m.seq); return; }
    if (m.t === "d" && isHost) { const raw = JSON.stringify(m); R.deltas.push({ seq: m.seq, raw }); if (R.deltas.length > 3000) R.deltas.shift(); for (const [c, w] of R.socks) if (c !== cid) send(w, raw); return; }
    if (m.t === "getsnap") { if (R.snap) { send(ws, { t: "snap", seq: R.snapSeq, data: R.snap }); for (const d of R.deltas) if (d.seq > R.snapSeq) send(ws, d.raw); } else send(R.socks.get(R.host), { t: "needsnap", cid }); return; }
    if (m.t === "nat" && isHost) { R.nat = new Map(Object.entries(m.nat || {})); bcast(R, { t: "players", players: roster(R) }); return; }
    if (m.t === "reset" && isHost) { R.snap = null; R.snapSeq = 0; R.deltas = []; R.nat = new Map(); bcast(R, { t: "reset" }, cid); bcast(R, { t: "players", players: roster(R) }); return; }
    if (m.to === "host") { m.from = cid; return send(R.socks.get(R.host), m); }
    if (m.to && R.socks.has(m.to) && isHost) return send(R.socks.get(m.to), m);
    if (m.to === "all" && isHost) return bcast(R, m, cid);
  });
  ws.on("close", () => {
    R.socks.delete(cid); R.names.delete(cid); R.nat.delete(cid);
    if (R.host === cid) { R.host = R.socks.size ? [...R.socks.keys()][0] : ""; if (R.host) { const h = R.socks.get(R.host); send(h, { t: "promote" }); if (R.snap) { send(h, { t: "snap", seq: R.snapSeq, data: R.snap }); for (const d of R.deltas) if (d.seq > R.snapSeq) send(h, d.raw); } } bcast(R, { t: "host", host: R.host }); }
    else if (R.host) send(R.socks.get(R.host), { t: "leave", cid });
    bcast(R, { t: "players", players: roster(R) });
  });
});
server.listen(PORT, () => console.log("relay on :" + PORT));
