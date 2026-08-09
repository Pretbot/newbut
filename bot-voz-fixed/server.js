// La librería tiktok-live-connector ha cambiado el nombre de su clase principal
// entre versiones (WebcastPushConnection en v1.x, TikTokLiveConnection en v2.x).
// Detectamos cuál está disponible para no depender de una versión exacta.
const tiktokLib = require('tiktok-live-connector');
const TikTokConnectionClass = tiktokLib.TikTokLiveConnection || tiktokLib.WebcastPushConnection;
if (!TikTokConnectionClass) {
    throw new Error('No se encontró TikTokLiveConnection ni WebcastPushConnection en tiktok-live-connector. Revisa la versión instalada.');
}

// Extrae uniqueId/nickname sin importar si vienen planos o anidados en data.user (según versión de la librería)
function extractUser(data) {
    const u = data.user || data;
    return {
        uniqueId: u.uniqueId || data.uniqueId || '',
        nickname: u.nickname || data.nickname || u.uniqueId || data.uniqueId || 'alguien',
    };
}

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

// ─── Logger ───────────────────────────────────────────────────────────────────
const log = {
    info:  (msg, ...args) => console.log(`[${new Date().toISOString()}] [INFO]  ${msg}`, ...args),
    warn:  (msg, ...args) => console.warn(`[${new Date().toISOString()}] [WARN]  ${msg}`, ...args),
    error: (msg, ...args) => console.error(`[${new Date().toISOString()}] [ERROR] ${msg}`, ...args),
    ok:    (msg, ...args) => console.log(`[${new Date().toISOString()}] [OK]    ${msg}`, ...args),
};

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin1234';

// ─── Apodos ───────────────────────────────────────────────────────────────────
const fs   = require('fs');
const path_apodos = path.join(__dirname, 'apodos.json');

function loadApodos() {
    try {
        if (fs.existsSync(path_apodos)) {
            const raw = fs.readFileSync(path_apodos, 'utf8');
            return JSON.parse(raw); // { "usuario_largo_123": "Juanito", ... }
        }
    } catch (e) { log.error('Error cargando apodos:', e.message); }
    return {};
}

function saveApodos(apodos) {
    try { fs.writeFileSync(path_apodos, JSON.stringify(apodos, null, 2), 'utf8'); }
    catch (e) { log.error('Error guardando apodos:', e.message); }
}

let apodos = loadApodos();

// ─── Estado de donadores ──────────────────────────────────────────────────────
// username (lowercase) → timestamp del último regalo recibido
const donadores = new Map();

// ─── Anuncios programados ─────────────────────────────────────────────────────
let anuncioIndex    = 0;
let anuncioInterval = null;

function startAnuncios() {
    if (anuncioInterval) clearInterval(anuncioInterval);
    const cfg = globalConfig.anuncios;
    if (!cfg.activo || !cfg.frases.length) return;
    const ms = (cfg.intervalo || 5) * 60 * 1000;
    anuncioInterval = setInterval(() => {
        const frases = globalConfig.anuncios.frases;
        if (!frases.length) return;
        let frase;
        if (globalConfig.anuncios.orden === 'aleatorio') {
            frase = frases[Math.floor(Math.random() * frases.length)];
        } else {
            frase = frases[anuncioIndex % frases.length];
            anuncioIndex++;
        }
        log.ok('Anuncio automático: ' + frase.slice(0, 60));
        io.emit('admin-mensaje', { texto: frase, esAnuncio: true });
    }, ms);
    log.ok('Anuncios cada ' + cfg.intervalo + ' min, modo: ' + cfg.orden);
}

// Devuelve el apodo si existe, o el username original
function resolveNombre(username) {
    const key = username.toLowerCase().trim();
    return apodos[key] || username;
}

// ─── Configuracion global ─────────────────────────────────────────────────────
let globalConfig = {
    maxQueue:   50,
    maxWords:   0,
    maxChars:   0,
    ttsRate:    1.1,
    ttsPitch:   1.0,
    botMuted:   false,
    pagePaused: false,
    pauseMsg:   '',
    requireGift: false,
    vipList:    [],
    keywords:   [],
    anuncios:   { activo: false, intervalo: 5, orden: 'secuencial', frases: [] },
};

// ─── App Setup ────────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    transports: ['websocket', 'polling'],
});

// ─── Rooms ────────────────────────────────────────────────────────────────────
const rooms = new Map();
const RECONNECT_DELAY_MS = 5_000;
const MAX_RECONNECTS     = 5;

// ─── Enqueue comentario ───────────────────────────────────────────────────────
function enqueue(username, usuario, mensaje) {
    const room = rooms.get(username);
    if (!room) return;
    if (room.queue.length >= globalConfig.maxQueue) {
        log.warn(`[${username}] Cola llena (${globalConfig.maxQueue}), descartando: ${usuario}`);
        return;
    }
    const apodo    = resolveNombre(usuario);
    const userKey  = (usuario || '').toLowerCase().trim();
    const origKey  = (usuario || '').toLowerCase().trim();
    const esVIP    = globalConfig.vipList.map(v => v.toLowerCase().trim()).includes(userKey);
    const dono     = donadores.has(userKey) || donadores.has(origKey);

    // ── Filtro de donación ───────────────────────────────────────────────────
    // Si requireGift está activo, solo leen VIP o quienes hayan donado
    if (globalConfig.requireGift && !esVIP && !dono) {
        log.info(`[${username}] Ignorado (sin regalo): ${usuario}`);
        // Igual emitimos el comentario para que se vea en el feed, pero sin TTS
        io.to(username).emit('nuevo-comentario', { usuario: apodo, usuarioOriginal: usuario, mensaje, sinTTS: true });
        return;
    }

    room.queue.push({ usuario: apodo, usuarioOriginal: usuario, mensaje });
    io.to(username).emit('nuevo-comentario', { usuario: apodo, usuarioOriginal: usuario, mensaje, esVIP });
    log.ok(`[${username}] ${apodo} (${usuario}): ${mensaje.slice(0, 50)}`);

    // ── Respuestas automáticas a keywords ────────────────────────────────────
    if (globalConfig.keywords.length) {
        const msLow = mensaje.toLowerCase();
        for (const kw of globalConfig.keywords) {
            if (!kw.palabra || !kw.respuesta) continue;
            if (msLow.includes(kw.palabra.toLowerCase())) {
                setTimeout(() => {
                    log.ok(`Keyword match: "${kw.palabra}" → "${kw.respuesta}"`);
                    const respuestaFinal = kw.respuesta.replace('{usuario}', apodo);
                    io.to(username).emit('admin-mensaje', { texto: respuestaFinal, esKeyword: true });
                }, 800);
                break; // solo primera coincidencia
            }
        }
    }
}

// ─── Enqueue regalo ───────────────────────────────────────────────────────────
function enqueueGift(username, usuario) {
    const room = rooms.get(username);
    if (!room) return;
    if (room.queue.length >= globalConfig.maxQueue) return;
    const apodo = resolveNombre(usuario);
    io.to(username).emit('nuevo-regalo', { usuario: apodo, usuarioOriginal: usuario });
    log.ok(`[${username}] REGALO de ${apodo} (${usuario})`);
}

// ─── Enqueue seguidor ─────────────────────────────────────────────────────────
function enqueueFollow(username, usuario) {
    const room = rooms.get(username);
    if (!room) return;
    const apodo = resolveNombre(usuario);
    io.to(username).emit('nuevo-follow', { usuario: apodo, usuarioOriginal: usuario });
    log.ok(`[${username}] FOLLOW de ${apodo} (${usuario})`);
}

// ─── Like ─────────────────────────────────────────────────────────────────────
const likeThrottle = new Map(); // evita spam: un aviso cada 30s por usuario
function handleLike(username, usuario, likeCount) {
    const key = `${username}:${usuario}`;
    const now = Date.now();
    if (likeThrottle.has(key) && now - likeThrottle.get(key) < 30_000) return;
    likeThrottle.set(key, now);
    const apodo = resolveNombre(usuario);
    io.to(username).emit('nuevo-like', { usuario: apodo, usuarioOriginal: usuario, likeCount });
    log.ok(`[${username}] LIKE x${likeCount} de ${apodo} (${usuario})`);
}

// ─── Share ────────────────────────────────────────────────────────────────────
function handleShare(username, usuario) {
    const apodo = resolveNombre(usuario);
    io.to(username).emit('nuevo-share', { usuario: apodo, usuarioOriginal: usuario });
    log.ok(`[${username}] SHARE de ${apodo} (${usuario})`);
}

// ─── Conexion TikTok Live ─────────────────────────────────────────────────────
function connectToLive(username, reconnectCount = 0) {
    const room = rooms.get(username);
    if (!room) return;

    const connOptions = {};
    if (process.env.TIKTOK_SESSION_ID) connOptions.sessionId = process.env.TIKTOK_SESSION_ID;
    // API key de Euler Stream (recomendado desde tiktok-live-connector v2 para evitar
    // límites de conexión anónima). Regístrate gratis en https://www.eulerstream.com
    if (process.env.EULER_API_KEY) connOptions.signApiKey = process.env.EULER_API_KEY;

    const conn = new TikTokConnectionClass(username, connOptions);
    room.connection = conn;

    conn.connect()
        .then(state => {
            log.ok(`Conectado a @${username} (roomId: ${state.roomId})`);
            io.to(username).emit('status', { type: 'connected', message: `Conectado al Live de @${username}` });
        })
        .catch(err => {
            log.error(`No se pudo conectar a @${username}: ${err.message}`);
            io.to(username).emit('status', { type: 'error', message: `Error al conectar: ${err.message}` });
            scheduleReconnect(username, reconnectCount);
        });

    // Comentarios de chat
    conn.on('chat', (data) => {
        const { nickname, uniqueId } = extractUser(data);
        // usamos uniqueId como clave interna, pero mantenemos nickname para mostrar
        enqueue(username, uniqueId || nickname, data.comment);
    });

    // Regalos — solo se procesa cuando el regalo está "completado" (streakFinished)
    // para no repetir el agradecimiento en cada tick del streak
    conn.on('gift', (data) => {
        const { nickname, uniqueId } = extractUser(data);
        const userKey1 = uniqueId.toLowerCase().trim();
        const userKey2 = nickname.toLowerCase().trim();
        if (userKey1) donadores.set(userKey1, Date.now());
        if (userKey2) donadores.set(userKey2, Date.now());
        // giftType 1 = regalo de streak (se repite), esperamos a que termine
        if (data.giftType === 1 && !data.repeatEnd) return;
        enqueueGift(username, uniqueId || nickname);
    });

    // Nuevos seguidores — evento dedicado de la librería (si existe)
    conn.on('follow', (data) => {
        const { nickname, uniqueId } = extractUser(data);
        enqueueFollow(username, uniqueId || nickname);
    });

    // Likes
    conn.on('like', (data) => {
        const { nickname, uniqueId } = extractUser(data);
        handleLike(username, uniqueId || nickname, data.likeCount || 1);
    });

    // Shares y follows también pueden llegar como evento 'social' según la versión
    conn.on('social', (data) => {
        const { nickname, uniqueId } = extractUser(data);
        const displayType = data.displayType || '';
        if (displayType.includes('share')) {
            handleShare(username, uniqueId || nickname);
        } else if (displayType.includes('follow')) {
            enqueueFollow(username, uniqueId || nickname);
        }
    });

    conn.on('disconnected', () => {
        log.warn(`[${username}] Desconectado`);
        io.to(username).emit('status', { type: 'reconnecting', message: `Reconectando a @${username}...` });
        scheduleReconnect(username, reconnectCount);
    });

    conn.on('error', (err) => log.error(`[${username}]: ${err.message}`));
    conn.on('streamEnd', () => {
        log.warn(`[${username}] El live terminó`);
        io.to(username).emit('status', { type: 'reconnecting', message: `El live de @${username} terminó` });
    });
}

function scheduleReconnect(username, previousCount) {
    const room = rooms.get(username);
    if (!room || room.sockets.size === 0) return;
    if (previousCount >= MAX_RECONNECTS) {
        io.to(username).emit('status', { type: 'failed', message: `Sin reconexion tras ${MAX_RECONNECTS} intentos` });
        return;
    }
    const delay = RECONNECT_DELAY_MS * (previousCount + 1);
    room.reconnectTimer = setTimeout(() => {
        if (rooms.has(username) && rooms.get(username).sockets.size > 0)
            connectToLive(username, previousCount + 1);
    }, delay);
}

// ─── Socket.IO ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
    log.info(`Socket conectado: ${socket.id}`);
    let currentRoom = null;
    let isAdmin = false;

    socket.emit('config-update', globalConfig);

    socket.on('join-live', (username) => {
        if (!username || typeof username !== 'string') {
            socket.emit('status', { type: 'error', message: 'Username invalido' });
            return;
        }
        username = username.trim().replace(/^@/, '').toLowerCase();
        if (currentRoom) leaveRoom(socket, currentRoom);
        currentRoom = username;
        socket.join(username);

        if (!rooms.has(username)) {
            rooms.set(username, { connection: null, sockets: new Set([socket.id]), queue: [], reconnectTimer: null, reconnectCount: 0 });
            connectToLive(username);
        } else {
            rooms.get(username).sockets.add(socket.id);
            socket.emit('status', { type: 'connected', message: `Unido al Live de @${username}` });
        }
    });

    socket.on('leave-live', () => {
        if (currentRoom) { leaveRoom(socket, currentRoom); currentRoom = null; }
    });

    socket.on('admin-login', (password) => {
        if (password === ADMIN_PASSWORD) {
            isAdmin = true;
            socket.join('admins');
            socket.emit('admin-auth', { ok: true, config: globalConfig });
            socket.emit('apodos-update', apodos);
            log.ok(`Admin autenticado: ${socket.id}`);
        } else {
            socket.emit('admin-auth', { ok: false });
            log.warn(`Contrasena incorrecta desde ${socket.id}`);
        }
    });

    socket.on('admin-set-config', (newConfig) => {
        if (!isAdmin) { socket.emit('status', { type: 'error', message: 'No autorizado' }); return; }
        const rules = {
            maxQueue: [1,   500],
            maxWords: [0,   100],
            maxChars: [0,   500],
            ttsRate:  [0.5, 3.0],
            ttsPitch: [0.0, 2.0],
        };
        for (const [key, [min, max]] of Object.entries(rules)) {
            if (newConfig[key] !== undefined) {
                const val = parseFloat(newConfig[key]);
                if (!isNaN(val) && val >= min && val <= max) globalConfig[key] = val;
            }
        }
        log.ok('Config actualizada:', JSON.stringify(globalConfig));
        io.emit('config-update', globalConfig);
        socket.emit('admin-config-saved', globalConfig);
    });

    // Admin: disparar sonido a todos
    socket.on('admin-play-sound', (soundId) => {
        if (!isAdmin) return;
        const valid = ['aplausos','redoble','fanfarria','campana','risas','fail',
                       'rimshot','suspenso','abucheo','alerta','levelup','moneda',
                       'gameover','explosion','sirena'];
        if (!valid.includes(soundId)) return;
        io.emit('play-sound', soundId);
        log.ok(`Admin disparó sonido: ${soundId}`);
    });

    // Admin: enviar mensaje TTS a todos
    socket.on('admin-tts', (mensaje) => {
        if (!isAdmin) return;
        if (typeof mensaje !== 'string' || !mensaje.trim()) return;
        const texto = mensaje.trim().slice(0, 300);
        io.emit('admin-mensaje', { texto });
        log.ok(`Admin TTS: ${texto.slice(0, 60)}`);
    });

    // ─── Pausar/reanudar página (solo admin) ─────────────────────────────────
    socket.on('admin-toggle-pause', ({ mensaje }) => {
        if (!isAdmin) return;
        globalConfig.pagePaused = !globalConfig.pagePaused;
        globalConfig.pauseMsg   = (typeof mensaje === 'string') ? mensaje.trim().slice(0, 200) : '';
        log.ok(`Página ${globalConfig.pagePaused ? 'PAUSADA' : 'REANUDADA'} — msg: "${globalConfig.pauseMsg}"`);
        io.emit('config-update', globalConfig);
    });

    // ─── Silenciar/activar bot (solo admin) ──────────────────────────────────
    socket.on('admin-toggle-mute', () => {
        if (!isAdmin) return;
        globalConfig.botMuted = !globalConfig.botMuted;
        log.ok(`Bot ${globalConfig.botMuted ? 'SILENCIADO' : 'ACTIVADO'}`);
        io.emit('config-update', globalConfig);
    });

    // ─── Apodos (solo admin) ──────────────────────────────────────────────────
    socket.on('admin-set-apodo', ({ username, apodo }) => {
        if (!isAdmin) { socket.emit('status', { type: 'error', message: 'No autorizado' }); return; }
        if (typeof username !== 'string' || !username.trim()) return;
        const key = username.trim().replace(/^@/, '').toLowerCase();
        const valor = (apodo || '').trim();
        if (valor) {
            apodos[key] = valor;
            log.ok(`Apodo asignado: ${key} → ${valor}`);
        } else {
            delete apodos[key];
            log.ok(`Apodo eliminado: ${key}`);
        }
        saveApodos(apodos);
        io.to('admins').emit('apodos-update', apodos);
    });

    socket.on('admin-get-apodos', () => {
        if (!isAdmin) return;
        socket.emit('apodos-update', apodos);
    });

    socket.on('admin-delete-apodo', (username) => {
        if (!isAdmin) return;
        const key = (username || '').trim().replace(/^@/, '').toLowerCase();
        if (apodos[key]) {
            delete apodos[key];
            saveApodos(apodos);
            log.ok(`Apodo eliminado: ${key}`);
        }
        io.to('admins').emit('apodos-update', apodos);
    });

    // ─── Música (solo admin) ──────────────────────────────────────────────────
    socket.on('admin-music-share', (payload) => {
        if (!isAdmin) return;
        if (!payload || typeof payload !== 'object') return;
        log.ok(`Admin música → ${payload.action} "${payload.title || payload.url || ''}"`);
        io.emit('music-command', payload);
    });

    // ─── Admin: guardar config de anuncios ───────────────────────────────────
    socket.on('admin-set-anuncios', (cfg) => {
        if (!isAdmin) return;
        if (typeof cfg.activo    === 'boolean') globalConfig.anuncios.activo    = cfg.activo;
        if (typeof cfg.intervalo === 'number')  globalConfig.anuncios.intervalo = Math.max(1, Math.min(120, cfg.intervalo));
        if (typeof cfg.orden     === 'string')  globalConfig.anuncios.orden     = cfg.orden;
        if (Array.isArray(cfg.frases))          globalConfig.anuncios.frases    = cfg.frases.slice(0, 50).map(f => String(f).slice(0, 300));
        anuncioIndex = 0;
        startAnuncios();
        log.ok('Config anuncios actualizada:', JSON.stringify(globalConfig.anuncios));
        io.emit('config-update', globalConfig);
    });

    // ─── Admin: guardar keywords ──────────────────────────────────────────────
    socket.on('admin-set-keywords', (kws) => {
        if (!isAdmin) return;
        if (!Array.isArray(kws)) return;
        globalConfig.keywords = kws.slice(0, 30).map(k => ({
            palabra:   String(k.palabra  || '').slice(0, 50).toLowerCase().trim(),
            respuesta: String(k.respuesta || '').slice(0, 200).trim(),
        })).filter(k => k.palabra && k.respuesta);
        log.ok('Keywords actualizadas:', globalConfig.keywords.length);
        io.emit('config-update', globalConfig);
    });

    // ─── Admin: config VIP y requireGift ─────────────────────────────────────
    socket.on('admin-set-filtro', (data) => {
        if (!isAdmin) return;
        if (typeof data.requireGift === 'boolean') globalConfig.requireGift = data.requireGift;
        if (Array.isArray(data.vipList)) {
            globalConfig.vipList = data.vipList.slice(0, 100).map(v => String(v).replace(/^@/,'').trim().toLowerCase()).filter(Boolean);
        }
        log.ok('Filtro regalo:', globalConfig.requireGift, 'VIPs:', globalConfig.vipList.length);
        io.emit('config-update', globalConfig);
    });

    socket.on('disconnect', () => {
        if (currentRoom) leaveRoom(socket, currentRoom);
    });
});

function leaveRoom(socket, username) {
    socket.leave(username);
    const room = rooms.get(username);
    if (!room) return;
    room.sockets.delete(socket.id);
    if (room.sockets.size === 0) {
        if (room.reconnectTimer) clearTimeout(room.reconnectTimer);
        if (room.connection) try { room.connection.disconnect(); } catch (_) {}
        rooms.delete(username);
        log.info(`Room eliminado: ${username}`);
    }
}

app.get('/health', (_req, res) => {
    res.json({
        status: 'ok',
        config: globalConfig,
        rooms: [...rooms.keys()].map(u => ({
            username: u,
            sockets: rooms.get(u).sockets.size,
            queueLength: rooms.get(u).queue.length,
        })),
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    log.ok(`Servidor en puerto ${PORT}`);
    log.info(`Admin panel -> http://localhost:${PORT}/admin.html`);
    log.warn(`Contrasena admin: ${ADMIN_PASSWORD}`);
});

// ─── Auto-ping (evita que Render Free pause el servicio) ──────────────────────
if (process.env.RENDER_EXTERNAL_URL) {
    const https = require('https');
    const PING_INTERVAL_MS = 10 * 60 * 1000; // cada 10 minutos

    setInterval(() => {
        const url = `${process.env.RENDER_EXTERNAL_URL}/health`;
        https.get(url, (res) => {
            log.info(`Auto-ping OK: ${res.statusCode} → ${url}`);
        }).on('error', (err) => {
            log.warn(`Auto-ping fallido: ${err.message}`);
        });
    }, PING_INTERVAL_MS);

    log.info(`Auto-ping activado → ${process.env.RENDER_EXTERNAL_URL}/health (cada 10 min)`);
}
