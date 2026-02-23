#!/usr/bin/env node

/**
 * LSM-Tree Storage Engine — Data Generator
 *
 * Generates sample data for experimenting with the storage engine.
 * Usage:
 *   node scripts/generate-data.js              # default: 1000 entries
 *   node scripts/generate-data.js --count 5000 # custom count
 *   node scripts/generate-data.js --flush      # auto-flush MemTable after insert
 */

import { existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import LSMTree from '../src/lsm-tree.js';

// ---- Parse CLI args ----
const args = process.argv.slice(2);
const countIdx = args.indexOf('--count');
const count = countIdx !== -1 && args[countIdx + 1]
  ? parseInt(args[countIdx + 1], 10)
  : 1000;
const shouldFlush = args.includes('--flush');
const DATA_DIR = join(process.cwd(), 'data');

// ---- Sample data pools ----
const firstNames = [
  'Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank',
  'Ivy', 'Jack', 'Karen', 'Leo', 'Mona', 'Nate', 'Olivia', 'Pete',
  'Quinn', 'Rose', 'Sam', 'Tina', 'Uma', 'Vince', 'Wendy', 'Xander', 'Yara', 'Zane',
];

const lastNames = [
  'Smith', 'Johnson', 'Lee', 'Brown', 'Garcia', 'Martinez', 'Davis',
  'Wilson', 'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson', 'White',
  'Harris', 'Clark', 'Lewis', 'Hall', 'Young', 'King',
];

const cities = [
  'New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto',
  'Seoul', 'Mumbai', 'São Paulo', 'Dubai', 'Singapore', 'Amsterdam',
  'Stockholm', 'Tashkent', 'Istanbul', 'Rome', 'Madrid', 'Vienna', 'Prague',
];

const departments = [
  'Engineering', 'Design', 'Marketing', 'Sales', 'Finance', 'HR',
  'Operations', 'Legal', 'Product', 'Research', 'Support', 'DevOps',
];

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const pad = (n, w) => String(n).padStart(w, '0');

// ---- Generate ----
console.log('\n\x1b[36m╔════════════════════════════════════════════╗');
console.log('║   LSM-Tree Data Generator                  ║');
console.log('╚════════════════════════════════════════════╝\x1b[0m\n');

// Clean previous data
if (existsSync(DATA_DIR)) {
  rmSync(DATA_DIR, { recursive: true, force: true });
  console.log('  \x1b[33m⟳ Cleared previous data\x1b[0m');
}

const db = new LSMTree(DATA_DIR);
const start = performance.now();

for (let i = 0; i < count; i++) {
  const id = pad(i, 5);
  const first = pick(firstNames);
  const last = pick(lastNames);

  // User profile
  db.put(`user:${id}:name`, `${first} ${last}`);
  db.put(`user:${id}:email`, `${first.toLowerCase()}.${last.toLowerCase()}@example.com`);
  db.put(`user:${id}:age`, String(randInt(18, 65)));
  db.put(`user:${id}:city`, pick(cities));
  db.put(`user:${id}:dept`, pick(departments));
  db.put(`user:${id}:score`, String(randInt(50, 100)));

  // A few deletes to produce tombstones
  if (i > 10 && Math.random() < 0.05) {
    const delId = pad(randInt(0, i - 1), 5);
    db.delete(`user:${delId}:score`);
  }
}

// Optional explicit flush
if (shouldFlush) {
  db.flush();
  console.log('  \x1b[32m⚡ MemTable flushed to disk\x1b[0m');
}

const elapsed = (performance.now() - start).toFixed(1);
const stats = db.stats();

console.log(`\n\x1b[32m▶ Generated ${count} users (${count * 6} key-value pairs)\x1b[0m`);
console.log(`  ✓ ${elapsed}ms elapsed`);
console.log(`\n\x1b[36m Engine State\x1b[0m`);
console.log(`  MemTable : ${stats.memTable.entries} entries`);
console.log(`  SSTables : ${stats.totalSSTables} total`);
console.log(`  Levels   : ${stats.levels.filter(l => l.sstables > 0).length} active`);
console.log(`\n  Explore with: \x1b[36mnpm start\x1b[0m  or  \x1b[36mnpm run ui\x1b[0m\n`);

db.close();
