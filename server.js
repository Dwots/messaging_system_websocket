const cluster = require('cluster');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const SERVER_WORKER_COUNT = Math.max(1, os.cpus().length);
const ROLE = process.env.WORKER_ROLE || 'server';
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'data', 'messages.sqlite');
const DB_POOL_SIZE = Math.max(1, Number(process.env.DB_POOL_SIZE || 2));
const DB_BATCH_SIZE = Math.max(1, Number(process.env.DB_BATCH_SIZE || 500));
const DB_FLUSH_INTERVAL = Math.max(10, Number(process.env.DB_FLUSH_INTERVAL || 100));
const DB_FETCH_BATCH_SIZE = Math.max(1, Number(process.env.DB_FETCH_BATCH_SIZE || 5000));

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function sendJson(ws, payload, callback) {
  if (ws.readyState !== WebSocket.OPEN) {
    if (callback) {
      callback(new Error('WebSocket is not open'));
    }
    return;
  }

  ws.send(JSON.stringify(payload), callback);
}

function normalizeText(text) {
  if (text === undefined || text === null) {
    return '';
  }

  return String(text);
}

if (cluster.isPrimary) {
  const clients = new Map();
  const workerRoles = new Map();
  const restartHistory = [];
  let dbWorker = null;
  let shuttingDown = false;

  function forkRole(role) {
    const worker = cluster.fork({ WORKER_ROLE: role });
    workerRoles.set(worker.id, role);

    if (role === 'db') {
      dbWorker = worker;
    }

    return worker;
  }

  function sendToDb(message) {
    if (!dbWorker || dbWorker.isDead()) {
      console.error('DB worker is not available');
      return;
    }

    dbWorker.send(message);
  }

  function storeOffline(message) {
    sendToDb({
      type: 'store_offline',
      message: {
        from: message.from,
        to: message.to,
        messageId: message.messageId,
        text: normalizeText(message.text),
        createdAt: message.createdAt || Date.now(),
      },
    });
  }

  console.log(`Server primary ${process.pid} is running`);
  console.log(`Starting ${SERVER_WORKER_COUNT} WebSocket workers on port ${PORT}`);
  console.log(`Starting one DB worker with SQLite file ${DB_FILE}`);

  forkRole('db');

  for (let i = 0; i < SERVER_WORKER_COUNT; i += 1) {
    forkRole('server');
  }

  cluster.on('message', (worker, message) => {
    if (!message || typeof message.type !== 'string') {
      return;
    }

    const role = workerRoles.get(worker.id);

    if (role === 'db') {
      if (message.type === 'db-response' && message.replyToWorkerId) {
        const targetWorker = cluster.workers[message.replyToWorkerId];

        if (targetWorker) {
          targetWorker.send({
            type: 'offline-messages',
            requestId: message.requestId,
            name: message.name,
            messages: message.messages || [],
            error: message.error,
          });
        }
      }

      if (message.type === 'db-error') {
        console.error(`DB worker error: ${message.error}`);
      }

      return;
    }

    if (message.type === 'register-request') {
      const { requestId, name } = message;

      if (!name) {
        worker.send({
          type: 'register-response',
          requestId,
          ok: false,
          error: 'Некорректное имя клиента',
        });
        return;
      }

      const existingWorkerId = clients.get(name);

      if (existingWorkerId && existingWorkerId !== worker.id) {
        const existingWorker = cluster.workers[existingWorkerId];

        if (existingWorker) {
          existingWorker.send({
            type: 'kick-client',
            name,
            reason: 'Клиент переподключился в другом воркере',
          });
        }
      }

      clients.set(name, worker.id);
      worker.send({ type: 'register-response', requestId, ok: true, name });
      return;
    }

    if (message.type === 'unregister') {
      const currentWorkerId = clients.get(message.name);

      if (currentWorkerId === worker.id) {
        clients.delete(message.name);
      }
      return;
    }

    if (message.type === 'route-message') {
      const outbound = {
        from: message.from,
        to: message.to,
        messageId: message.messageId,
        text: normalizeText(message.text),
        createdAt: Date.now(),
      };

      if (!outbound.from || !outbound.to || outbound.from === outbound.to) {
        worker.send({
          type: 'delivery-error',
          to: outbound.from,
          messageId: outbound.messageId,
          error: 'Некорректный получатель',
        });
        return;
      }

      const targetWorkerId = clients.get(outbound.to);
      const targetWorker = targetWorkerId ? cluster.workers[targetWorkerId] : null;

      if (!targetWorker) {
        storeOffline(outbound);
        return;
      }

      targetWorker.send({ type: 'deliver-message', message: outbound });
      return;
    }

    if (message.type === 'target-missing') {
      const currentWorkerId = clients.get(message.to);

      if (currentWorkerId === worker.id) {
        clients.delete(message.to);
      }

      storeOffline(message.message);
      return;
    }

    if (message.type === 'fetch-offline') {
      sendToDb({
        type: 'fetch_offline',
        requestId: message.requestId,
        name: message.name,
        replyToWorkerId: worker.id,
      });
      return;
    }

    if (message.type === 'delete-offline') {
      sendToDb({
        type: 'delete_offline',
        ids: message.ids,
      });
    }
  });

  process.on('SIGINT', () => {
    shuttingDown = true;

    for (const worker of Object.values(cluster.workers)) {
      worker.kill('SIGINT');
    }

    process.exit(130);
  });

  cluster.on('exit', (worker, code, signal) => {
    const role = workerRoles.get(worker.id);
    workerRoles.delete(worker.id);

    for (const [name, workerId] of clients) {
      if (workerId === worker.id) {
        clients.delete(name);
      }
    }

    if (shuttingDown) {
      return;
    }

    const now = Date.now();
    restartHistory.push(now);

    while (restartHistory.length > 0 && now - restartHistory[0] > 5000) {
      restartHistory.shift();
    }

    if (restartHistory.length > (SERVER_WORKER_COUNT + 1) * 2) {
      console.error('Too many worker crashes in 5 seconds. Stop restarting.');
      shuttingDown = true;
      process.exit(1);
    }

    console.log(
      `${role || 'unknown'} worker ${worker.process.pid} exited (code=${code}, signal=${signal}). Starting replacement.`,
    );
    forkRole(role || 'server');
  });

} else if (ROLE === 'db') {
  runDbWorker();
} else {
  runServerWorker();
}

function runDbWorker() {
  fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });

  const pool = Array.from({ length: DB_POOL_SIZE }, () => new DatabaseSync(DB_FILE));
  let poolIndex = 0;
  let offlineBatch = [];
  let deleteBatch = [];

  for (const db of pool) {
    db.exec('PRAGMA journal_mode = WAL');
    db.exec('PRAGMA synchronous = NORMAL');
    db.exec('PRAGMA busy_timeout = 5000');
  }

  pool[0].exec(`
    CREATE TABLE IF NOT EXISTS offline_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      from_name TEXT NOT NULL,
      to_name TEXT NOT NULL,
      message_id TEXT NOT NULL UNIQUE,
      text TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_offline_messages_to_name
      ON offline_messages (to_name, id);
  `);

  const insertOfflineStatement = pool[0].prepare(`
    INSERT OR IGNORE INTO offline_messages
      (from_name, to_name, message_id, text, created_at)
    VALUES (?, ?, ?, ?, ?)
  `);

  function nextConnection() {
    const db = pool[poolIndex];
    poolIndex = (poolIndex + 1) % pool.length;
    return db;
  }

  function flushOfflineBatch() {
    if (offlineBatch.length === 0) {
      return;
    }

    const batch = offlineBatch;
    offlineBatch = [];

    pool[0].exec('BEGIN IMMEDIATE');

    try {
      for (const message of batch) {
        insertOfflineStatement.run(
          message.from,
          message.to,
          message.messageId,
          normalizeText(message.text),
          message.createdAt || Date.now(),
        );
      }

      pool[0].exec('COMMIT');
    } catch (error) {
      pool[0].exec('ROLLBACK');
      throw error;
    }
  }

  function fetchOffline(name) {
    flushOfflineBatch();
    flushDeleteBatch();

    const db = nextConnection();
    const statement = db.prepare(`
      SELECT
        id,
        from_name AS "from",
        to_name AS "to",
        message_id AS messageId,
        text,
        created_at AS createdAt
      FROM offline_messages
      WHERE to_name = ?
      ORDER BY id
      LIMIT ?
      OFFSET ?
    `);

    const messages = [];
    let offset = 0;

    while (true) {
      const rows = statement.all(name, DB_FETCH_BATCH_SIZE, offset);
      messages.push(...rows);

      if (rows.length < DB_FETCH_BATCH_SIZE) {
        break;
      }

      offset += rows.length;
    }

    return messages;
  }

  function deleteOfflineNow(ids) {
    flushOfflineBatch();

    const uniqueIds = [...new Set((ids || []).filter(Number.isInteger))];

    if (uniqueIds.length === 0) {
      return;
    }

    const db = pool[0];
    db.exec('BEGIN IMMEDIATE');

    try {
      for (let index = 0; index < uniqueIds.length; index += 500) {
        const chunk = uniqueIds.slice(index, index + 500);
        const placeholders = chunk.map(() => '?').join(',');
        db.prepare(`DELETE FROM offline_messages WHERE id IN (${placeholders})`).run(...chunk);
      }

      db.exec('COMMIT');
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    }
  }

  function flushDeleteBatch() {
    if (deleteBatch.length === 0) {
      return;
    }

    const ids = deleteBatch;
    deleteBatch = [];
    deleteOfflineNow(ids);
  }

  const flushTimer = setInterval(() => {
    try {
      flushOfflineBatch();
      flushDeleteBatch();
    } catch (error) {
      process.send({ type: 'db-error', error: error.message });
    }
  }, DB_FLUSH_INTERVAL);

  process.on('message', (message) => {
    if (!message || typeof message.type !== 'string') {
      return;
    }

    try {
      if (message.type === 'store_offline') {
        offlineBatch.push(message.message);

        if (offlineBatch.length >= DB_BATCH_SIZE) {
          flushOfflineBatch();
        }
        return;
      }

      if (message.type === 'fetch_offline') {
        const messages = fetchOffline(message.name);

        process.send({
          type: 'db-response',
          op: 'fetch_offline',
          requestId: message.requestId,
          replyToWorkerId: message.replyToWorkerId,
          name: message.name,
          messages,
        });
        return;
      }

      if (message.type === 'delete_offline') {
        deleteBatch.push(...(message.ids || []));

        if (deleteBatch.length >= DB_BATCH_SIZE) {
          flushDeleteBatch();
        }
      }
    } catch (error) {
      process.send({
        type: 'db-error',
        requestId: message.requestId,
        replyToWorkerId: message.replyToWorkerId,
        error: error.message,
      });
    }
  });

  process.on('SIGINT', () => {
    clearInterval(flushTimer);

    try {
      flushOfflineBatch();
      flushDeleteBatch();
    } finally {
      for (const db of pool) {
        db.close();
      }
    }

    process.exit(130);
  });

  console.log(`DB worker ${process.pid} started`);
}

function runServerWorker() {
  const localClients = new Map();
  const pendingRegistrations = new Map();
  let nextRequestId = 1;

  const server = http.createServer();
  const wss = new WebSocketServer({ server });

  function sendOfflineMessages(name, messages) {
    const target = localClients.get(name);

    if (!target || target.readyState !== WebSocket.OPEN) {
      return;
    }

    for (const message of messages) {
      sendJson(target, {
        type: 'message',
        from: message.from,
        to: message.to,
        messageId: message.messageId,
        text: message.text,
        offline: true,
        offlineId: message.id,
      });
    }
  }

  function deliverLocalMessage(message) {
    const target = localClients.get(message.to);

    if (!target || target.readyState !== WebSocket.OPEN) {
      return false;
    }

    sendJson(target, {
      type: 'message',
      from: message.from,
      to: message.to,
      messageId: message.messageId,
      text: message.text,
      offline: false,
    });

    return true;
  }

  process.on('message', (message) => {
    if (!message || typeof message.type !== 'string') {
      return;
    }

    if (message.type === 'register-response') {
      const pending = pendingRegistrations.get(message.requestId);

      if (!pending) {
        return;
      }

      pendingRegistrations.delete(message.requestId);

      if (!message.ok) {
        sendJson(pending.ws, { type: 'register-error', error: message.error });
        pending.ws.close(1008, 'register failed');
        return;
      }

      pending.ws.clientName = pending.name;
      localClients.set(pending.name, pending.ws);
      sendJson(pending.ws, { type: 'register-ok', name: pending.name });

      const requestId = `${process.pid}-offline-${nextRequestId}`;
      nextRequestId += 1;
      process.send({ type: 'fetch-offline', requestId, name: pending.name });
      return;
    }

    if (message.type === 'deliver-message') {
      if (!deliverLocalMessage(message.message)) {
        process.send({
          type: 'target-missing',
          to: message.message.to,
          message: message.message,
        });
      }
      return;
    }

    if (message.type === 'offline-messages') {
      if (message.error) {
        console.error(`Offline fetch error for ${message.name}: ${message.error}`);
        return;
      }

      sendOfflineMessages(message.name, message.messages || []);
      return;
    }

    if (message.type === 'delivery-error') {
      const source = localClients.get(message.to);

      if (source) {
        sendJson(source, {
          type: 'send-error',
          messageId: message.messageId,
          error: message.error,
        });
      }
      return;
    }

    if (message.type === 'kick-client') {
      const target = localClients.get(message.name);

      if (target) {
        target.close(4001, message.reason || 'duplicate registration');
      }
    }
  });

  wss.on('connection', (ws) => {
    ws.on('message', (raw) => {
      const message = safeJsonParse(raw.toString());

      if (!message || typeof message.type !== 'string') {
        sendJson(ws, { type: 'error', error: 'Ожидался JSON с полем type' });
        return;
      }

      if (!ws.clientName && message.type !== 'register') {
        sendJson(ws, {
          type: 'register-error',
          error: 'Первым сообщением должна быть регистрация',
        });
        ws.close(1008, 'registration required');
        return;
      }

      if (message.type === 'register') {
        if (ws.clientName) {
          sendJson(ws, { type: 'register-error', error: 'Клиент уже зарегистрирован' });
          return;
        }

        const name = typeof message.name === 'string' ? message.name.trim() : '';

        if (!name) {
          sendJson(ws, { type: 'register-error', error: 'Некорректное имя клиента' });
          return;
        }

        const requestId = `${process.pid}-register-${nextRequestId}`;
        nextRequestId += 1;
        pendingRegistrations.set(requestId, { ws, name });
        process.send({ type: 'register-request', requestId, name });
        return;
      }

      if (message.type === 'send') {
        const to = typeof message.to === 'string' ? message.to : '';
        const outbound = {
          from: ws.clientName,
          to,
          messageId: message.messageId,
          text: normalizeText(message.text),
        };

        if (!outbound.to || outbound.from === outbound.to) {
          sendJson(ws, {
            type: 'send-error',
            messageId: outbound.messageId,
            error: 'Некорректный получатель',
          });
          return;
        }

        if (deliverLocalMessage(outbound)) {
          return;
        }

        process.send({
          type: 'route-message',
          ...outbound,
        });
        return;
      }

      if (message.type === 'message-ack') {
        if (Number.isInteger(message.offlineId)) {
          process.send({ type: 'delete-offline', ids: [message.offlineId] });
        }
      }
    });

    ws.on('close', () => {
      if (ws.clientName) {
        localClients.delete(ws.clientName);
        process.send({ type: 'unregister', name: ws.clientName });
      }

      for (const [requestId, pending] of pendingRegistrations) {
        if (pending.ws === ws) {
          pendingRegistrations.delete(requestId);
        }
      }
    });

    ws.on('error', (error) => {
      console.error(`WebSocket error in worker ${process.pid}: ${error.message}`);
    });
  });

  server.on('error', (error) => {
    console.error(`HTTP server error in worker ${process.pid}: ${error.message}`);
    process.exit(1);
  });

  server.listen(PORT, () => {
    console.log(`Server worker ${process.pid} listening on port ${PORT}`);
  });
}
