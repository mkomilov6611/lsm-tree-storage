/**
 * LSM-Tree Storage Engine — Web UI Server
 *
 * Express server that exposes the engine via a REST API and serves
 * a single-page dashboard for visual exploration.
 */

import express from 'express';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import LSMTree from '../src/lsm-tree.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = 3000;

// Parse optional --data flag for custom data directory
const dataArgIdx = process.argv.indexOf('--data');
const dataDir = dataArgIdx !== -1 && process.argv[dataArgIdx + 1]
  ? join(process.cwd(), process.argv[dataArgIdx + 1])
  : undefined;

// Shared engine instance
const db = new LSMTree(dataDir);

app.use(express.json());
app.use(express.static(join(__dirname, 'public')));

/* ---- REST API --------------------------------------------------- */

// Get engine stats
app.get('/api/stats', (_req, res) => {
  res.json(db.stats());
});

// Put a key-value pair
app.post('/api/put', (req, res) => {
  const { key, value } = req.body;
  if (!key || value === undefined) {
    return res.status(400).json({ error: 'key and value are required' });
  }
  db.put(String(key), String(value));
  res.json({ ok: true });
});

// Get a value by key
app.get('/api/get/:key', (req, res) => {
  const val = db.get(req.params.key);
  res.json({ key: req.params.key, value: val });
});

// Delete a key
app.delete('/api/delete/:key', (req, res) => {
  db.delete(req.params.key);
  res.json({ ok: true });
});

// Range scan
app.get('/api/scan', (req, res) => {
  const { start, end } = req.query;
  if (!start || !end) {
    return res.status(400).json({ error: 'start and end query params required' });
  }
  const results = db.scan(String(start), String(end));
  res.json({ results, count: results.length });
});

// Force flush
app.post('/api/flush', (_req, res) => {
  db.flush();
  res.json({ ok: true, message: 'MemTable flushed to Level 0 SSTable' });
});

// Inspector — detailed MemTable + SSTable data
app.get('/api/inspect', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  res.json(db.inspect(limit));
});

// Generate sample data
app.post('/api/generate', (req, res) => {
  const count = parseInt(req.query.count) || 200;

  const firstNames = ['Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Hank','Ivy','Jack','Karen','Leo','Mona','Nate','Olivia','Pete','Quinn','Rose','Sam','Tina'];
  const lastNames = ['Smith','Johnson','Lee','Brown','Garcia','Martinez','Davis','Wilson','Anderson','Taylor','Thomas','Moore','Jackson','White'];
  const cities = ['New York','London','Tokyo','Paris','Berlin','Sydney','Toronto','Seoul','Mumbai','Dubai','Singapore','Tashkent','Istanbul','Rome'];
  const departments = ['Engineering','Design','Marketing','Sales','Finance','HR','Operations','Product','Research','Support'];

  const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
  const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
  const pad = (n, w) => String(n).padStart(w, '0');

  const baseId = Date.now() % 100000;
  for (let i = 0; i < count; i++) {
    const id = pad(baseId + i, 5);
    const first = pick(firstNames);
    const last = pick(lastNames);
    db.put(`user:${id}:name`, `${first} ${last}`);
    db.put(`user:${id}:email`, `${first.toLowerCase()}.${last.toLowerCase()}@example.com`);
    db.put(`user:${id}:age`, String(rand(18, 65)));
    db.put(`user:${id}:city`, pick(cities));
    db.put(`user:${id}:dept`, pick(departments));
  }

  res.json({ ok: true, generated: count, entries: count * 5 });
});

/* ---- Start ------------------------------------------------------ */

app.listen(PORT, () => {
  console.log(`\n🚀 LSM-Tree Dashboard running at \x1b[36mhttp://localhost:${PORT}\x1b[0m\n`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  db.close();
  process.exit(0);
});
