/**
 * CDK Timer - WebSocket 서버
 * Node.js 내장 모듈만 사용 (외부 패키지 불필요)
 * 실행: node server.js
 */
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

const PORT   = 3000;
const PUBLIC = path.join(__dirname, 'public');

// ── 방별 상태 ──────────────────────────────────────────────
const rooms = {};

function getRoom(roomId) {
  if (!rooms[roomId]) {
    rooms[roomId] = {
      state: {
        sessions:   [{name:'세션 1', presenter:'', mins:20},{name:'세션 2', presenter:'', mins:15}],
        curIdx:     0,
        secsLeft:   20 * 60,
        countUp:    0,
        running:    false,
        mode:       'down',
        warnYellow: 5,
        warnRed:    1,
        bgColor:    '#ffffff',
      },
      ticker:  null,
      clients: new Set(), // { ws, role, readyState }
    };
  }
  return rooms[roomId];
}

// ── 타이머 ────────────────────────────────────────────────
function startTicker(roomId) {
  const room = getRoom(roomId);
  if (room.ticker) return;
  room.ticker = setInterval(() => {
    const s = room.state;
    s.mode === 'down' ? s.secsLeft-- : s.countUp++;
    broadcast(roomId, { type: 'state', state: s });
  }, 1000);
}

function stopTicker(roomId) {
  const room = getRoom(roomId);
  if (room.ticker) { clearInterval(room.ticker); room.ticker = null; }
}

// ── 브로드캐스트 ──────────────────────────────────────────
function broadcast(roomId, msg, onlyRole) {
  const room = getRoom(roomId);
  const data = JSON.stringify(msg);
  room.clients.forEach(c => {
    if (onlyRole && c.role !== onlyRole) return;
    if (c.ws.readyState === 1) sendFrame(c.ws, data);
  });
}

// ── 제어 명령 처리 ────────────────────────────────────────
function applyControl(roomId, cmd) {
  const room = getRoom(roomId);
  const s    = room.state;
  const { action, secs, idx, sessions, warnYellow, warnRed, mode, bgColor } = cmd;

  if (action === 'togglePlay') {
    s.running = !s.running;
    s.running ? startTicker(roomId) : stopTicker(roomId);
  } else if (action === 'pause') {
    s.running = false; stopTicker(roomId);
  } else if (action === 'stop') {
    s.running = false; stopTicker(roomId);
    s.secsLeft = 0; s.countUp = 0;
  } else if (action === 'reset') {
    s.running = false; stopTicker(roomId);
    s.secsLeft = (s.sessions[s.curIdx]?.mins || 0) * 60;
    s.countUp  = 0;
  } else if (action === 'next') {
    if (s.curIdx + 1 < s.sessions.length) {
      s.curIdx++;
      s.secsLeft = s.sessions[s.curIdx].mins * 60;
      s.countUp  = 0; s.running = false; stopTicker(roomId);
    }
  } else if (action === 'jump') {
    s.curIdx   = idx;
    s.secsLeft = s.sessions[idx].mins * 60;
    s.countUp  = 0; s.running = false; stopTicker(roomId);
  } else if (action === 'addTime') {
    s.secsLeft = Math.max(0, s.secsLeft + secs);
  } else if (action === 'setMode') {
    s.mode = mode; s.running = false; stopTicker(roomId);
    s.secsLeft = (s.sessions[s.curIdx]?.mins || 0) * 60;
    s.countUp  = 0;
  } else if (action === 'setBgColor') {
    s.bgColor = bgColor;
  } else if (action === 'updateSessions') {
    s.sessions   = sessions;
    s.warnYellow = warnYellow;
    s.warnRed    = warnRed;
    if (s.curIdx >= s.sessions.length) s.curIdx = 0;
    s.secsLeft = s.sessions[s.curIdx].mins * 60;
    s.countUp  = 0; s.running = false; stopTicker(roomId);
  }

  broadcast(roomId, { type: 'state', state: s });
}

// ── HTTP 서버 ─────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.webmanifest': 'application/manifest+json',
  '.ico':  'image/x-icon',
  '.png':  'image/png',
};

const server = http.createServer((req, res) => {
  let fp = path.join(PUBLIC, req.url.split('?')[0]);
  if (fp === path.join(PUBLIC, '/')) fp = path.join(PUBLIC, 'index.html');
  fs.readFile(fp, (err, data) => {
    if (err) {
      fs.readFile(path.join(PUBLIC, 'index.html'), (e2, d2) => {
        res.writeHead(e2 ? 404 : 200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(e2 ? 'Not Found' : d2);
      });
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream' });
    res.end(data);
  });
});

// ── WebSocket 핸드셰이크 ──────────────────────────────────
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  const p      = new URLSearchParams(req.url.split('?')[1] || '');
  const roomId = p.get('room') || 'default';
  const role   = p.get('role') || 'speaker';
  const room   = getRoom(roomId);
  const client = { ws: socket, role, readyState: 1 };
  room.clients.add(client);

  // 접속 즉시 현재 상태 전송
  sendFrame(socket, JSON.stringify({ type: 'init', state: room.state }));

  // 핑 (30초마다 연결 유지)
  const pingInterval = setInterval(() => {
    try { if (socket.writable) socket.write(Buffer.from([0x89, 0x00])); }
    catch(e) { clearInterval(pingInterval); }
  }, 30000);

  let buf = Buffer.alloc(0);
  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const msg = parseFrame(buf);
      if (!msg) break;
      buf = buf.slice(msg.consumed);
      if (msg.opcode === 0x8) { socket.destroy(); break; } // close
      if (msg.opcode === 0xA) continue; // pong
      if (msg.text) {
        try {
          const data = JSON.parse(msg.text);
          handleMsg(roomId, role, data);
        } catch(e) {}
      }
    }
  });

  socket.on('close', () => {
    clearInterval(pingInterval);
    client.readyState = 3;
    room.clients.delete(client);
  });
  socket.on('error', () => {
    clearInterval(pingInterval);
    client.readyState = 3;
    room.clients.delete(client);
  });
});

// ── 메시지 핸들러 ─────────────────────────────────────────
function handleMsg(roomId, role, data) {
  if (!data || !data.type) return;

  if (data.type === 'control' && role === 'admin') {
    applyControl(roomId, data);
    return;
  }
  if (data.type === 'message' && role === 'admin') {
    const to = data.to;
    const pkg = JSON.stringify({ type: 'message', text: data.text, to });
    getRoom(roomId).clients.forEach(c => {
      if (!to || to === 'all' || c.role === to) {
        if (c.ws.readyState === 1) sendFrame(c.ws, pkg);
      }
    });
  }
}

// ── WebSocket 프레임 파싱 / 인코딩 ───────────────────────
function parseFrame(buf) {
  if (buf.length < 2) return null;
  const masked = (buf[1] & 0x80) !== 0;
  let len = buf[1] & 0x7f;
  let offset = 2;
  if (len === 126) {
    if (buf.length < 4) return null;
    len = buf.readUInt16BE(2); offset = 4;
  } else if (len === 127) {
    if (buf.length < 10) return null;
    len = Number(buf.readBigUInt64BE(2)); offset = 10;
  }
  const total = offset + (masked ? 4 : 0) + len;
  if (buf.length < total) return null;
  const mask    = masked ? buf.slice(offset, offset + 4) : null;
  const payload = buf.slice(offset + (masked ? 4 : 0), total);
  if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
  return { opcode: buf[0] & 0x0f, text: payload.toString(), consumed: total };
}

function sendFrame(socket, data) {
  try {
    if (!socket.writable) return;
    const payload = Buffer.from(data, 'utf8');
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x81, len]);
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x81; header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x81; header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    socket.write(Buffer.concat([header, payload]));
  } catch(e) {}
}

// ── 서버 시작 ─────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  // 로컬 IP 자동 감지
  const nets = require('os').networkInterfaces();
  const ips  = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
    }
  }

  console.log('\n✅ CDK Timer 서버 실행 중\n');
  console.log('📡 태블릿 접속 URL:');
  ips.forEach(ip => {
    console.log(`\n   연자  : http://${ip}:${PORT}?room=A&role=speaker`);
    console.log(`   좌장  : http://${ip}:${PORT}?room=A&role=chair`);
    console.log(`   관리자: http://${ip}:${PORT}?room=A&role=admin`);
  });
  console.log('\n   room=A 를 room=B, room=C 로 바꾸면 다른 방\n');
  console.log('⛔ 종료하려면 Ctrl+C\n');
});
