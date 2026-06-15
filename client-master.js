const { fork } = require('child_process');
const path = require('path');

const CLIENT_COUNT = Number(process.env.CLIENT_COUNT || 10);
const MESSAGE_COUNT = Number(process.env.MESSAGE_COUNT || 100000);
const SERVER_URL = process.env.SERVER_URL || 'ws://localhost:8080';
const PROGRESS_INTERVAL = Number(process.env.PROGRESS_INTERVAL || 1000);
const RUN_ID = process.env.RUN_ID || `run-${Date.now()}-${process.pid}`;

const names = Array.from({ length: CLIENT_COUNT }, (_, index) => `client-${index}`);
const workers = new Map();
const stats = new Map();

let registeredCount = 0;
let finishedSendingCount = 0;
let finished = false;

const expectedTotal = CLIENT_COUNT * MESSAGE_COUNT;

console.log(`runId=${RUN_ID}`);

function ensureStats(name) {
  if (!stats.has(name)) {
    stats.set(name, {
      sentCount: 0,
      receivedCount: 0,
      sentByTarget: {},
      receivedBySource: {},
    });
  }

  return stats.get(name);
}

function totals() {
  let sentCount = 0;
  let receivedCount = 0;

  for (const item of stats.values()) {
    sentCount += item.sentCount;
    receivedCount += item.receivedCount;
  }

  return { sentCount, receivedCount };
}

function pairKey(from, to) {
  return `${from}->${to}`;
}

function pairSummary() {
  const pairs = new Map();

  for (const [from, item] of stats) {
    for (const [to, count] of Object.entries(item.sentByTarget || {})) {
      const key = pairKey(from, to);
      const pair = pairs.get(key) || { from, to, sentCount: 0, receivedCount: 0 };
      pair.sentCount += count;
      pairs.set(key, pair);
    }
  }

  for (const [to, item] of stats) {
    for (const [from, count] of Object.entries(item.receivedBySource || {})) {
      const key = pairKey(from, to);
      const pair = pairs.get(key) || { from, to, sentCount: 0, receivedCount: 0 };
      pair.receivedCount += count;
      pairs.set(key, pair);
    }
  }

  const mismatches = [...pairs.values()].filter((pair) => pair.sentCount !== pair.receivedCount);

  return {
    pairs,
    mismatches,
    match: mismatches.length === 0,
  };
}

function maybeFinish() {
  if (finished) {
    return;
  }

  const total = totals();
  const pairStats = pairSummary();

  if (
    finishedSendingCount === CLIENT_COUNT &&
    total.sentCount === expectedTotal &&
    total.receivedCount === expectedTotal &&
    pairStats.match
  ) {
    finished = true;
    console.log('Final statistics:');
    console.log(`sent=${total.sentCount}`);
    console.log(`received=${total.receivedCount}`);
    console.log(`match=${total.sentCount === total.receivedCount}`);
    console.log(`pairMatch=${pairStats.match}`);
    console.log(`pairs=${pairStats.pairs.size}`);

    for (const child of workers.values()) {
      child.send({ type: 'stop' });
    }

    setTimeout(() => process.exit(0), 200);
  }
}

function startAllClients() {
  console.log('All clients registered. Starting message sending.');

  for (const child of workers.values()) {
    child.send({ type: 'start' });
  }
}

for (const name of names) {
  const child = fork(path.join(__dirname, 'client-worker.js'), [], {
    env: {
      ...process.env,
      MESSAGE_COUNT: String(MESSAGE_COUNT),
      SERVER_URL,
      PROGRESS_INTERVAL: String(PROGRESS_INTERVAL),
    },
  });

  workers.set(name, child);
  ensureStats(name);

  child.on('message', (message) => {
    if (!message || typeof message.type !== 'string') {
      return;
    }

    if (message.type === 'registered') {
      registeredCount += 1;
      console.log(`registered ${message.name} (${registeredCount}/${CLIENT_COUNT})`);

      if (registeredCount === CLIENT_COUNT) {
        startAllClients();
      }
      return;
    }

    if (message.type === 'stats' || message.type === 'sent-done') {
      const item = ensureStats(message.name);
      item.sentCount = message.sentCount;
      item.receivedCount = message.receivedCount;
      item.sentByTarget = message.sentByTarget || {};
      item.receivedBySource = message.receivedBySource || {};

      if (message.type === 'sent-done') {
        finishedSendingCount += 1;
        console.log(
          `sent done ${message.name} (${finishedSendingCount}/${CLIENT_COUNT}), sent=${message.sentCount}`,
        );
      }

      maybeFinish();
      return;
    }

    if (message.type === 'worker-error') {
      console.error(`${message.name}: ${message.error}`);
    }
  });

  child.on('exit', (code, signal) => {
    if (!finished && code !== 0) {
      console.error(`${name} exited unexpectedly (code=${code}, signal=${signal})`);
    }
  });
}

for (const [name, child] of workers) {
  const otherClients = names.filter((clientName) => clientName !== name);

  child.send({
    type: 'init',
    name,
    otherClients,
    runId: RUN_ID,
  });
}

const progressTimer = setInterval(() => {
  const total = totals();
  const pairStats = pairSummary();
  console.log(
    `progress sent=${total.sentCount}/${expectedTotal}, received=${total.receivedCount}/${expectedTotal}, pairMismatches=${pairStats.mismatches.length}`,
  );

  maybeFinish();
}, 1000);

process.on('exit', () => {
  clearInterval(progressTimer);
});

process.on('SIGINT', () => {
  for (const child of workers.values()) {
    child.kill('SIGINT');
  }

  process.exit(130);
});
