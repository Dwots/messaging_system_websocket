const WebSocket = require('ws');

const messageCount = Number(process.env.MESSAGE_COUNT || 100000);
const serverUrl = process.env.SERVER_URL || 'ws://localhost:8080';
const progressInterval = Number(process.env.PROGRESS_INTERVAL || 1000);
const batchSize = Number(process.env.BATCH_SIZE || 1000);
const maxBufferedAmount = Number(process.env.MAX_BUFFERED_AMOUNT || 16 * 1024 * 1024);
const reconnectBaseMs = Number(process.env.RECONNECT_BASE_MS || 500);
const reconnectMaxMs = Number(process.env.RECONNECT_MAX_MS || 10000);
const reconnectMinMs = Number(process.env.RECONNECT_MIN_MS || 2000);
const randomDisconnectsEnabled = process.env.RANDOM_DISCONNECTS !== '0';
const disconnectMinMs = Number(process.env.DISCONNECT_MIN_MS || 5000);
const disconnectMaxMs = Number(process.env.DISCONNECT_MAX_MS || 30000);

let name = '';
let otherClients = [];
let runId = '';
let socket = null;
let progressTimer = null;
let reconnectTimer = null;
let randomDisconnectTimer = null;
let sendScheduled = false;
let initialized = false;
let started = false;
let registered = false;
let everRegistered = false;
let sentDone = false;
let stopping = false;
let randomCloseRequested = false;
let reconnectAttempt = 0;
let nextMessageSeq = 0;
let inFlightSends = 0;
let sentCount = 0;
let receivedCount = 0;

const sentByTarget = {};
const receivedBySource = {};
const receivedMessageIds = new Set();

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function incrementCounter(target, key) {
  target[key] = (target[key] || 0) + 1;
}

function notify(payload) {
  if (process.send) {
    process.send({ name, ...payload });
  }
}

function sendStats(type = 'stats') {
  notify({
    type,
    sentCount,
    receivedCount,
    sentByTarget: { ...sentByTarget },
    receivedBySource: { ...receivedBySource },
  });
}

function randomTarget() {
  return otherClients[Math.floor(Math.random() * otherClients.length)];
}

function isCurrentRunMessage(messageId) {
  return typeof messageId === 'string' && messageId.startsWith(`${runId}:`);
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function clearRandomDisconnectTimer() {
  if (randomDisconnectTimer) {
    clearTimeout(randomDisconnectTimer);
    randomDisconnectTimer = null;
  }
}

function scheduleSend(delay = 0) {
  if (sendScheduled || stopping) {
    return;
  }

  sendScheduled = true;

  const run = () => {
    sendScheduled = false;
    sendBatch();
  };

  if (delay > 0) {
    setTimeout(run, delay);
  } else {
    setImmediate(run);
  }
}

function maybeMarkSentDone() {
  if (!sentDone && sentCount >= messageCount) {
    sentDone = true;
    sendStats('sent-done');
  }
}

function sendBatch() {
  if (!started || sentDone || stopping) {
    return;
  }

  if (!registered || !socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }

  let sentInBatch = 0;

  while (
    sentCount + inFlightSends < messageCount &&
    sentInBatch < batchSize &&
    socket.readyState === WebSocket.OPEN &&
    socket.bufferedAmount < maxBufferedAmount
  ) {
    const to = randomTarget();

    if (!to) {
      notify({ type: 'worker-error', error: 'Нет доступных получателей' });
      return;
    }

    const messageId = `${runId}:${name}-${nextMessageSeq}`;
    nextMessageSeq += 1;
    inFlightSends += 1;
    sentInBatch += 1;

    socket.send(
      JSON.stringify({
        type: 'send',
        to,
        messageId,
        text: 'payload',
      }),
      (error) => {
        inFlightSends -= 1;

        if (!error && !stopping) {
          sentCount += 1;
          incrementCounter(sentByTarget, to);
        }

        maybeMarkSentDone();

        if (!sentDone && started && !stopping) {
          scheduleSend(error ? 100 : 0);
        }
      },
    );
  }

  maybeMarkSentDone();

  if (!sentDone && sentCount + inFlightSends < messageCount) {
    scheduleSend(socket.bufferedAmount >= maxBufferedAmount ? 10 : 0);
  }
}

function scheduleReconnect(useRandomDelay = false) {
  if (stopping || reconnectTimer) {
    return;
  }

  const delay = useRandomDelay
    ? randomBetween(reconnectMinMs, reconnectMaxMs)
    : Math.min(reconnectMaxMs, reconnectBaseMs * 2 ** reconnectAttempt);

  if (!useRandomDelay) {
    reconnectAttempt += 1;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function scheduleRandomDisconnect() {
  if (!randomDisconnectsEnabled || stopping) {
    return;
  }

  clearRandomDisconnectTimer();

  randomDisconnectTimer = setTimeout(() => {
    if (!socket || socket.readyState !== WebSocket.OPEN || stopping) {
      return;
    }

    randomCloseRequested = true;
    registered = false;
    socket.close(4000, 'random reconnect');
  }, randomBetween(disconnectMinMs, disconnectMaxMs));
}

function connect() {
  if (stopping || !initialized) {
    return;
  }

  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
    return;
  }

  clearReconnectTimer();
  socket = new WebSocket(serverUrl);

  socket.on('open', () => {
    socket.send(JSON.stringify({ type: 'register', name }));
  });

  socket.on('message', (raw) => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      notify({ type: 'worker-error', error: `Invalid JSON from server: ${raw}` });
      return;
    }

    if (message.type === 'register-ok') {
      registered = true;
      reconnectAttempt = 0;
      scheduleRandomDisconnect();

      if (!everRegistered) {
        everRegistered = true;
        notify({ type: 'registered' });
      }

      if (started) {
        scheduleSend();
      }
      return;
    }

    if (message.type === 'register-error' || message.type === 'send-error' || message.type === 'error') {
      notify({ type: 'worker-error', error: message.error });
      return;
    }

    if (message.type === 'message') {
      if (message.offlineId !== undefined && socket.readyState === WebSocket.OPEN) {
        socket.send(
          JSON.stringify({
            type: 'message-ack',
            messageId: message.messageId,
            offlineId: message.offlineId,
          }),
        );
      }

      if (!isCurrentRunMessage(message.messageId)) {
        return;
      }

      if (!receivedMessageIds.has(message.messageId)) {
        receivedMessageIds.add(message.messageId);
        receivedCount += 1;
        incrementCounter(receivedBySource, message.from);
      }
    }
  });

  socket.on('close', () => {
    registered = false;
    clearRandomDisconnectTimer();
    sendStats();

    const useRandomDelay = randomCloseRequested;
    randomCloseRequested = false;
    socket = null;

    if (!stopping) {
      scheduleReconnect(useRandomDelay);
    }
  });

  socket.on('error', (error) => {
    if (!stopping) {
      notify({ type: 'worker-error', error: error.message });
    }
  });
}

process.on('message', (message) => {
  if (!message || typeof message.type !== 'string') {
    return;
  }

  if (message.type === 'init') {
    if (initialized) {
      return;
    }

    initialized = true;
    name = message.name;
    otherClients = Array.isArray(message.otherClients) ? message.otherClients : [];
    runId = typeof message.runId === 'string' ? message.runId : '';

    progressTimer = setInterval(() => {
      sendStats();
    }, progressInterval);

    connect();
    return;
  }

  if (message.type === 'start') {
    if (!started) {
      started = true;
      scheduleSend();
    }
    return;
  }

  if (message.type === 'stop') {
    stopping = true;
    clearReconnectTimer();
    clearRandomDisconnectTimer();
    sendStats();

    if (progressTimer) {
      clearInterval(progressTimer);
    }

    if (socket) {
      socket.close(1000, 'done');
    }

    setTimeout(() => process.exit(0), 100);
  }
});

process.on('SIGINT', () => {
  stopping = true;
  clearReconnectTimer();
  clearRandomDisconnectTimer();

  if (progressTimer) {
    clearInterval(progressTimer);
  }

  if (socket) {
    socket.close();
  }

  process.exit(130);
});
