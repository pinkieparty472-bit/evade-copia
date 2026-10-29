// Servidor de sala para Evade: sirve el juego y reenvía la presencia de los jugadores por WebSocket.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  if (req.url.startsWith('/health')) { res.writeHead(200); return res.end('ok'); }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (e, d) => {
    if (e) { res.writeHead(500); return res.end('falta public/index.html'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(d);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map();
let n = 0;
wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  const room = (u.searchParams.get('room') || 'main').replace(/[^\w-]/g, '').slice(0, 20) || 'main';
  if (!rooms.has(room)) rooms.set(room, { peers: new Map(), dirty: false });
  const R = rooms.get(room);
  if (R.peers.size >= 16) { ws.close(); return; }
  const id = String(++n).padStart(8, '0'); // ids crecientes: el anfitrión es el que entró primero
  const P = { ws, presence: {} };
  R.peers.set(id, P);
  ws.isAlive = true;
  ws.send(JSON.stringify({ t: 'hello', id }));
  R.dirty = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    if (m.t !== 'p' || !m.d || typeof m.d !== 'object') return;
    for (const k in m.d) {
      if (k === '__proto__' || k === 'constructor') continue;
      if (m.d[k] === null) delete P.presence[k]; else P.presence[k] = m.d[k];
    }
    R.dirty = true;
  });
  ws.on('close', () => { R.peers.delete(id); R.dirty = true; if (!R.peers.size) rooms.delete(room); });
});
// difusión a 25 Hz solo si hubo cambios
setInterval(() => {
  for (const R of rooms.values()) {
    if (!R.dirty) continue; R.dirty = false;
    const msg = JSON.stringify({ t: 'peers', list: [...R.peers].map(([peer, p]) => ({ peer, presence: p.presence })) });
    for (const p of R.peers.values()) if (p.ws.readyState === 1) p.ws.send(msg);
  }
}, 40);
// mantiene vivas las conexiones y limpia las muertas
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); } }, 25000);
server.listen(PORT, () => console.log('Evade escuchando en el puerto ' + PORT));
