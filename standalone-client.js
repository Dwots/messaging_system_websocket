const fs = require('fs');
const path = require('path');
const readline = require('readline');
const WebSocket = require('ws');

const name = process.argv[2];
const initialTargets = (process.argv[3] || '')
  .split(',')
  .map((item) => item.trim())
  .filter(Boolean);

const serverUrl = process.env.SERVER_URL || 'ws://localhost:8080';
const stateDir = path.join(__dirname, '.client-state');
const stateFile = name ? path.join(stateDir, `${name}.json`) : '';
const reconnectBaseMs = Number(process.env.RECONNECT_BASE_MS || 500);
const reconnectMaxMs = Number(process.env.RECONNECT_MAX_MS || 10000);

if (!name) {
  console.log('Usage: node standalone-client.js <client-name> [target-1,target-2]');
  process.exit(1);
}

fs.mkdirSync(stateDir, { recursive: true });

let socket = null;
let registered = false;
let reconnectAttempt = 0;
let nextMessageSeq = 0;
let sentCount = 0;
let receivedCount = 0;
let targets = [...initialTargets];
let stopping = false;

const sentByTarget = {};
const receivedBySource = {};
const receivedMessageIds = new Set();

function loadState() {
  if (!fs.existsSync(stateFile)) {
    return;
  }

  const state = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  nextMessageSeq = state.nextMessageSeq || 0;
  sentCount = state.sentCount || 0;
  receivedCount = state.receivedCount || 0;
  Object.assign(sentByTarget, state.sentByTarget || {});
  Object.assign(receivedBySource, state.receivedBySource || {});

  if (Array.isArray(state.targets) && targets.length === 0) {
    targets = state.targets;
  }

  for (const messageId of state.receivedMessageIds || []) {
    receivedMessageIds.add(messageId);
  }
}

function saveState() {
  const state = {
    name,
    serverUrl,
    targets,
    nextMessageSeq,
    sentCount,
    receivedCount,
    sentByTarget,
    receivedBySource,
    receivedMessageIds: [...receivedMessageIds],
  };

  fs.writeFileSync(stateFile, JSON.stringify(state, null, 2));
}

function incrementCounter(target, key) {
  target[key] = (target[key] || 0) + 1;
}

function connect() {
  if (stopping) {
    return;
  }

  socket = new WebSocket(serverUrl);

  socket.on('open', () => {
    socket.send(JSON.stringify({ type: 'register', name }));
  });

  socket.on('message', (raw) => {
    const message = JSON.parse(raw.toString());

    if (message.type === 'register-ok') {
      registered = true;
      reconnectAttempt = 0;
      console.log(`registered as ${name}`);
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

      if (!receivedMessageIds.has(message.messageId)) {
        receivedMessageIds.add(message.messageId);
        receivedCount += 1;
        incrementCounter(receivedBySource, message.from);
        saveState();
      }

      console.log(`[${message.from}] ${message.text}`);
      return;
    }

    if (message.type === 'register-error' || message.type === 'send-error' || message.type === 'error') {
      console.log(`server error: ${message.error}`);
    }
  });

  socket.on('close', (code, reasonBuffer) => {
    registered = false;
    socket = null;
    saveState();

    const reason = reasonBuffer.toString();

    if (stopping) {
      return;
    }

    if (code === 4001) {
      console.log(`disconnected by server: ${reason || 'duplicate registration'}`);
      console.log('another client with the same name is connected; stop this manual client');
      stopping = true;
      process.exit(1);
    }

    const delay = Math.min(reconnectMaxMs, reconnectBaseMs * 2 ** reconnectAttempt);
    reconnectAttempt += 1;
    console.log(`disconnected (code=${code}, reason=${reason || '-'}), reconnect in ${delay} ms`);
    setTimeout(connect, delay);
  });

  socket.on('error', (error) => {
    console.log(`socket error: ${error.message}`);
  });
}

function sendMessage(to, text) {
  if (!registered || !socket || socket.readyState !== WebSocket.OPEN) {
    console.log('client is not connected yet');
    return;
  }

  const messageId = `${name}-manual-${nextMessageSeq}`;
  nextMessageSeq += 1;

  socket.send(
    JSON.stringify({
      type: 'send',
      to,
      messageId,
      text,
    }),
    (error) => {
      if (error) {
        console.log(`send failed: ${error.message}`);
        return;
      }

      sentCount += 1;
      incrementCounter(sentByTarget, to);
      saveState();
      console.log(`sent to ${to}`);
    },
  );
}

loadState();
connect();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: '> ',
});

console.log(`manual client ${name}`);
console.log('commands: send <to> <text>, targets <a,b,c>, stats, quit');
rl.prompt();

rl.on('line', (line) => {
  const trimmed = line.trim();

  if (!trimmed) {
    rl.prompt();
    return;
  }

  if (trimmed === 'quit' || trimmed === 'exit') {
    stopping = true;
    saveState();

    if (socket) {
      socket.close(1000, 'manual stop');
    }

    process.exit(0);
  }

  if (trimmed === 'stats') {
    console.log({
      sentCount,
      receivedCount,
      sentByTarget,
      receivedBySource,
      targets,
    });
    rl.prompt();
    return;
  }

  if (trimmed.startsWith('targets ')) {
    targets = trimmed
      .slice('targets '.length)
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
    saveState();
    console.log(`targets: ${targets.join(', ')}`);
    rl.prompt();
    return;
  }

  if (trimmed.startsWith('send ')) {
    const [, to, ...textParts] = trimmed.split(' ');
    const text = textParts.join(' ');

    if (!to || !text) {
      console.log('usage: send <to> <text>');
    } else {
      sendMessage(to, text);
    }

    rl.prompt();
    return;
  }

  if (targets.length > 0) {
    const target = targets[Math.floor(Math.random() * targets.length)];
    sendMessage(target, trimmed);
  } else {
    console.log('unknown command');
  }

  rl.prompt();
});

process.on('SIGINT', () => {
  stopping = true;
  saveState();

  if (socket) {
    socket.close(1000, 'SIGINT');
  }

  process.exit(130);
});
