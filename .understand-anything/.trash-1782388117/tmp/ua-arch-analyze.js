#!/usr/bin/env node
'use strict';
const fs = require('fs');

function main() {
  const inPath = process.argv[2];
  const outPath = process.argv[3];
  if (!inPath || !outPath) { console.error('usage: analyze <in> <out>'); process.exit(1); }
  const data = JSON.parse(fs.readFileSync(inPath, 'utf8'));
  const fileNodes = data.fileNodes || [];
  const importEdges = data.importEdges || [];
  const allEdges = data.allEdges || [];

  const byId = {};
  fileNodes.forEach(n => { byId[n.id] = n; });
  const paths = fileNodes.map(n => n.filePath || '');

  // common prefix by path segments
  function commonPrefix(ps) {
    if (!ps.length) return '';
    const splits = ps.map(p => p.split('/'));
    const first = splits[0];
    let i = 0;
    for (; i < first.length - 1; i++) {
      if (!splits.every(s => s[i] === first[i])) break;
    }
    return first.slice(0, i).join('/');
  }
  const prefix = commonPrefix(paths);

  function groupKey(fp) {
    let rel = fp;
    if (prefix && rel.startsWith(prefix + '/')) rel = rel.slice(prefix.length + 1);
    const parts = rel.split('/');
    return parts.length > 1 ? parts[0] : 'root';
  }

  // A. directory groups
  const directoryGroups = {};
  fileNodes.forEach(n => {
    const g = groupKey(n.filePath || '');
    (directoryGroups[g] = directoryGroups[g] || []).push(n.id);
  });

  // B. node type groups
  const nodeTypeGroups = {};
  fileNodes.forEach(n => {
    (nodeTypeGroups[n.type] = nodeTypeGroups[n.type] || []).push(n.id);
  });

  // C. fan in/out (imports only)
  const fileFanIn = {}, fileFanOut = {};
  fileNodes.forEach(n => { fileFanIn[n.id] = 0; fileFanOut[n.id] = 0; });
  importEdges.forEach(e => {
    if (fileFanOut[e.source] !== undefined) fileFanOut[e.source]++;
    if (fileFanIn[e.target] !== undefined) fileFanIn[e.target]++;
  });

  // D. cross-category edges
  const crossMap = {};
  allEdges.forEach(e => {
    const s = byId[e.source], t = byId[e.target];
    if (!s || !t) return;
    if (s.type === t.type) return;
    const k = s.type + '|' + t.type + '|' + e.type;
    crossMap[k] = (crossMap[k] || 0) + 1;
  });
  const crossCategoryEdges = Object.entries(crossMap).map(([k, c]) => {
    const [fromType, toType, edgeType] = k.split('|');
    return { fromType, toType, edgeType, count: c };
  });

  // E. inter-group imports
  const interMap = {};
  importEdges.forEach(e => {
    const gs = groupKey((byId[e.source] || {}).filePath || '');
    const gt = groupKey((byId[e.target] || {}).filePath || '');
    if (gs === gt) return;
    const k = gs + '|' + gt;
    interMap[k] = (interMap[k] || 0) + 1;
  });
  const interGroupImports = Object.entries(interMap).map(([k, c]) => {
    const [from, to] = k.split('|');
    return { from, to, count: c };
  });

  // F. intra-group density
  const intraGroupDensity = {};
  Object.keys(directoryGroups).forEach(g => {
    let internal = 0, total = 0;
    const ids = new Set(directoryGroups[g]);
    importEdges.forEach(e => {
      const gs = groupKey((byId[e.source] || {}).filePath || '');
      const gt = groupKey((byId[e.target] || {}).filePath || '');
      if (gs === g || gt === g) total++;
      if (ids.has(e.source) && ids.has(e.target)) internal++;
    });
    intraGroupDensity[g] = { internalEdges: internal, totalEdges: total, density: total ? internal / total : 0 };
  });

  // G. pattern matching
  const dirPatterns = [
    [['routes','api','controllers','endpoints','handlers'],'api'],
    [['services','core','lib','domain','logic'],'service'],
    [['models','db','data','persistence','repository','entities'],'data'],
    [['components','views','pages','ui','layouts','screens'],'ui'],
    [['utils','helpers','common','shared','tools'],'utility'],
    [['config','constants','env','settings'],'config'],
    [['__tests__','test','tests','spec','specs'],'test'],
    [['types','interfaces','schemas','contracts','dtos'],'types'],
  ];
  function matchDir(name) {
    for (const [list, label] of dirPatterns) if (list.includes(name)) return label;
    return null;
  }
  function matchFile(fp, type) {
    const base = fp.split('/').pop();
    if (/\.(test|spec)\.[^.]+$/.test(base)) return 'test';
    if (/\.d\.ts$/.test(base)) return 'types';
    if (/\.(md|rst)$/.test(base)) return 'documentation';
    if (base === 'package.json' || /\.(toml|lock)$/.test(base)) return 'config';
    if (type === 'config') return 'config';
    if (type === 'document') return 'documentation';
    if (type === 'service') return 'infrastructure';
    if (type === 'pipeline') return 'ci-cd';
    return null;
  }
  const patternMatches = {};
  Object.keys(directoryGroups).forEach(g => {
    const m = matchDir(g);
    if (m) patternMatches[g] = m;
  });
  const filePatternMatches = {};
  fileNodes.forEach(n => {
    const m = matchFile(n.filePath || '', n.type);
    if (m) filePatternMatches[n.id] = m;
  });

  // H. deployment topology
  const infraFiles = [];
  let hasDockerfile=false, hasCompose=false, hasK8s=false, hasTerraform=false, hasCI=false;
  fileNodes.forEach(n => {
    const fp = n.filePath || '';
    const b = fp.split('/').pop();
    if (b === 'Dockerfile') { hasDockerfile = true; infraFiles.push(fp); }
    if (/docker-compose/.test(b)) { hasCompose = true; infraFiles.push(fp); }
    if (/\.tf$/.test(b)) { hasTerraform = true; infraFiles.push(fp); }
    if (/\.ya?ml$/.test(b) && /(k8s|kube|deploy)/.test(fp)) { hasK8s = true; infraFiles.push(fp); }
    if (/\.github\/workflows|gitlab-ci|Jenkinsfile/.test(fp)) { hasCI = true; infraFiles.push(fp); }
  });
  const deploymentTopology = { hasDockerfile, hasCompose, hasK8s, hasTerraform, hasCI, infraFiles };

  // I. data pipeline
  const dataPipeline = { schemaFiles: [], migrationFiles: [], dataModelFiles: [], apiHandlerFiles: [] };

  // J. doc coverage
  const docFiles = fileNodes.filter(n => n.type === 'document' || /\.(md|rst)$/.test(n.filePath || ''));
  const groupsWithDocs = new Set();
  docFiles.forEach(n => groupsWithDocs.add(groupKey(n.filePath || '')));
  const totalGroups = Object.keys(directoryGroups).length;
  const undocumented = Object.keys(directoryGroups).filter(g => !groupsWithDocs.has(g));
  const docCoverage = {
    groupsWithDocs: groupsWithDocs.size,
    totalGroups,
    coverageRatio: totalGroups ? groupsWithDocs.size / totalGroups : 0,
    undocumentedGroups: undocumented,
  };

  // K. dependency direction
  const dirPairs = {};
  interGroupImports.forEach(e => { dirPairs[e.from + '|' + e.to] = e.count; });
  const seen = new Set();
  const dependencyDirection = [];
  interGroupImports.forEach(e => {
    const rev = dirPairs[e.to + '|' + e.from] || 0;
    const key = [e.from, e.to].sort().join('|');
    if (seen.has(key)) return;
    seen.add(key);
    if (e.count >= rev) dependencyDirection.push({ dependent: e.from, dependsOn: e.to });
    else dependencyDirection.push({ dependent: e.to, dependsOn: e.from });
  });

  const filesPerGroup = {};
  Object.keys(directoryGroups).forEach(g => filesPerGroup[g] = directoryGroups[g].length);
  const nodeTypeCounts = {};
  Object.keys(nodeTypeGroups).forEach(t => nodeTypeCounts[t] = nodeTypeGroups[t].length);

  const out = {
    scriptCompleted: true,
    commonPrefix: prefix,
    directoryGroups,
    nodeTypeGroups,
    crossCategoryEdges,
    interGroupImports,
    intraGroupDensity,
    patternMatches,
    filePatternMatches,
    deploymentTopology,
    dataPipeline,
    docCoverage,
    dependencyDirection,
    fileStats: { totalFileNodes: fileNodes.length, filesPerGroup, nodeTypeCounts },
    fileFanIn,
    fileFanOut,
  };
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log('done');
}
try { main(); } catch (e) { console.error(e); process.exit(1); }
