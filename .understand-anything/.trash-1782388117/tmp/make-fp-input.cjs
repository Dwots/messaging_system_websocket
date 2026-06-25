const fs = require('fs');
const [scanPath, outPath, root, hash] = process.argv.slice(2);
const s = JSON.parse(fs.readFileSync(scanPath, 'utf8'));
const files = (s.files || []).map(f => f.path || f.filePath || f).filter(Boolean);
const input = {
  projectRoot: root.split('\\').join('/'),
  sourceFilePaths: files,
  gitCommitHash: hash
};
fs.writeFileSync(outPath, JSON.stringify(input, null, 2));
console.log('fingerprint sourceFilePaths:', files.length);
