#!/usr/bin/env node
'use strict';

const fs = require('fs');

function main() {
  const inPath = process.argv[2];
  const outPath = process.argv[3];
  if (!inPath || !outPath) {
    console.error('Usage: ua-tour-analyze.js <input.json> <output.json>');
    process.exit(1);
  }
  const data = JSON.parse(fs.readFileSync(inPath, 'utf8'));
  const nodes = data.nodes || [];
  const edges = data.edges || [];
  const layers = data.layers || [];

  const nodeById = new Map();
  for (const n of nodes) nodeById.set(n.id, n);

  // Fan-in / fan-out
  const fanIn = new Map();
  const fanOut = new Map();
  for (const n of nodes) { fanIn.set(n.id, 0); fanOut.set(n.id, 0); }
  const fwdAdj = new Map(); // for BFS via imports/calls
  for (const n of nodes) fwdAdj.set(n.id, []);
  for (const e of edges) {
    if (fanIn.has(e.target)) fanIn.set(e.target, fanIn.get(e.target) + 1);
    if (fanOut.has(e.source)) fanOut.set(e.source, fanOut.get(e.source) + 1);
    if (fwdAdj.has(e.source)) fwdAdj.get(e.source).push(e);
  }

  const fanInRanking = nodes.map(n => ({ id: n.id, fanIn: fanIn.get(n.id) || 0, name: n.name }))
    .sort((a, b) => b.fanIn - a.fanIn).slice(0, 20);
  const fanOutRanking = nodes.map(n => ({ id: n.id, fanOut: fanOut.get(n.id) || 0, name: n.name }))
    .sort((a, b) => b.fanOut - a.fanOut).slice(0, 20);

  // Entry point detection
  const entryNames = new Set(['index.ts','index.js','main.ts','main.js','app.ts','app.js','server.ts','server.js','mod.rs','main.go','main.py','main.rs','manage.py','app.py','wsgi.py','asgi.py','run.py','__main__.py','Application.java','Main.java','Program.cs','config.ru','index.php','App.swift','Application.kt','main.cpp','main.c']);
  const fanOutVals = nodes.map(n => fanOut.get(n.id) || 0).sort((a,b)=>a-b);
  const fanInVals = nodes.map(n => fanIn.get(n.id) || 0).sort((a,b)=>a-b);
  const fanOutTop10 = fanOutVals.length ? fanOutVals[Math.floor(fanOutVals.length * 0.9)] : 0;
  const fanInBottom25 = fanInVals.length ? fanInVals[Math.floor(fanInVals.length * 0.25)] : 0;

  const candidates = [];
  for (const n of nodes) {
    let score = 0;
    const fp = n.filePath || '';
    const depth = fp.split('/').length;
    if (n.type === 'document') {
      if (n.name === 'README.md' && depth === 1) score += 5;
      else if (/\.md$/i.test(n.name) && depth === 1) score += 2;
    } else if (n.type === 'file') {
      if (entryNames.has(n.name)) score += 3;
      if (depth <= 2) score += 1;
      if ((fanOut.get(n.id) || 0) >= fanOutTop10 && fanOutTop10 > 0) score += 1;
      if ((fanIn.get(n.id) || 0) <= fanInBottom25) score += 1;
    }
    if (score > 0) candidates.push({ id: n.id, score, name: n.name, summary: n.summary || '' });
  }
  candidates.sort((a, b) => b.score - a.score);
  const entryPointCandidates = candidates.slice(0, 5);

  // BFS from top code entry point
  const codeCandidates = candidates.filter(c => nodeById.get(c.id).type === 'file');
  const startNode = codeCandidates.length ? codeCandidates[0].id : (nodes.find(n=>n.type==='file')||{}).id;
  const bfsOrder = [];
  const depthMap = {};
  if (startNode) {
    const q = [startNode];
    depthMap[startNode] = 0;
    while (q.length) {
      const cur = q.shift();
      bfsOrder.push(cur);
      for (const e of (fwdAdj.get(cur) || [])) {
        if (e.type !== 'imports' && e.type !== 'calls') continue;
        if (!(e.target in depthMap)) {
          depthMap[e.target] = depthMap[cur] + 1;
          q.push(e.target);
        }
      }
    }
  }
  const byDepth = {};
  for (const id of bfsOrder) {
    const d = depthMap[id];
    (byDepth[d] = byDepth[d] || []).push(id);
  }

  // Non-code inventory
  const nonCodeFiles = { documentation: [], infrastructure: [], data: [], config: [] };
  for (const n of nodes) {
    const rec = { id: n.id, name: n.name, type: n.type, summary: n.summary || '' };
    if (n.type === 'document') nonCodeFiles.documentation.push(rec);
    else if (['service','pipeline','resource'].includes(n.type)) nonCodeFiles.infrastructure.push(rec);
    else if (['table','schema','endpoint'].includes(n.type)) nonCodeFiles.data.push(rec);
    else if (n.type === 'config') nonCodeFiles.config.push(rec);
  }

  // Clusters via bidirectional imports/calls
  const pairKey = (a, b) => [a, b].sort().join('||');
  const dir = new Map(); // "a->b" for imports/calls
  for (const e of edges) {
    if (e.type === 'imports' || e.type === 'calls') dir.set(e.source + '->' + e.target, true);
  }
  const clusterSet = new Map();
  for (const e of edges) {
    if (e.type !== 'imports' && e.type !== 'calls') continue;
    if (dir.get(e.target + '->' + e.source)) {
      const k = pairKey(e.source, e.target);
      clusterSet.set(k, new Set([e.source, e.target]));
    }
  }
  const clusters = [];
  for (const s of clusterSet.values()) {
    clusters.push({ nodes: [...s], edgeCount: 2 });
  }
  clusters.sort((a, b) => b.edgeCount - a.edgeCount);

  // Node summary index
  const nodeSummaryIndex = {};
  for (const n of nodes) nodeSummaryIndex[n.id] = { name: n.name, type: n.type, summary: n.summary || '' };

  const out = {
    scriptCompleted: true,
    entryPointCandidates,
    fanInRanking,
    fanOutRanking,
    bfsTraversal: { startNode, order: bfsOrder, depthMap, byDepth },
    nonCodeFiles,
    clusters: clusters.slice(0, 10),
    layers: { count: layers.length, list: layers.map(l => ({ id: l.id, name: l.name, description: l.description })) },
    nodeSummaryIndex,
    totalNodes: nodes.length,
    totalEdges: edges.length
  };
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
}

try { main(); } catch (err) { console.error(err.stack || String(err)); process.exit(1); }
