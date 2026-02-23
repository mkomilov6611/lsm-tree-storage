/**
 * LSM-Tree Storage Engine — Performance Benchmark
 *
 * Measures operations per second for:
 *  - Sequential writes
 *  - Random reads
 *  - Range scans
 */

import { existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import LSMTree from '../src/lsm-tree.js';

const BENCH_DIR = join(process.cwd(), 'bench_data');

// Clean up previous run
if (existsSync(BENCH_DIR)) rmSync(BENCH_DIR, { recursive: true, force: true });

const db = new LSMTree(BENCH_DIR);

const WRITE_COUNT = 10_000;
const READ_COUNT = 10_000;
const SCAN_COUNT = 1_000;

console.log(`
\x1b[1m\x1b[35m╔══════════════════════════════════════════╗
║      LSM-Tree Storage Engine Benchmark   ║
╚══════════════════════════════════════════╝\x1b[0m
`);

/* ---- Sequential Writes ------------------------------------------ */
console.log(`\x1b[33m▶ Sequential Writes:\x1b[0m ${WRITE_COUNT.toLocaleString()} ops`);

const writeStart = performance.now();
for (let i = 0; i < WRITE_COUNT; i++) {
  db.put(`bench_key_${String(i).padStart(6, '0')}`, `value_${i}_${'x'.repeat(20)}`);
}
const writeMs = performance.now() - writeStart;
const writeOps = Math.round(WRITE_COUNT / (writeMs / 1000));

console.log(`  \x1b[32m✓\x1b[0m ${writeMs.toFixed(1)}ms — \x1b[1m${writeOps.toLocaleString()} ops/sec\x1b[0m`);

/* ---- Random Reads ----------------------------------------------- */
console.log(`\x1b[33m▶ Random Reads:\x1b[0m ${READ_COUNT.toLocaleString()} ops`);

const readStart = performance.now();
for (let i = 0; i < READ_COUNT; i++) {
  const idx = Math.floor(Math.random() * WRITE_COUNT);
  const val = db.get(`bench_key_${String(idx).padStart(6, '0')}`);
  if (val === null) throw new Error(`Missing key at index ${idx}`);
}
const readMs = performance.now() - readStart;
const readOps = Math.round(READ_COUNT / (readMs / 1000));

console.log(`  \x1b[32m✓\x1b[0m ${readMs.toFixed(1)}ms — \x1b[1m${readOps.toLocaleString()} ops/sec\x1b[0m`);

/* ---- Range Scans ------------------------------------------------ */
console.log(`\x1b[33m▶ Range Scans:\x1b[0m ${SCAN_COUNT.toLocaleString()} ops (50-key windows)`);

const scanStart = performance.now();
for (let i = 0; i < SCAN_COUNT; i++) {
  const start = Math.floor(Math.random() * (WRITE_COUNT - 50));
  const startKey = `bench_key_${String(start).padStart(6, '0')}`;
  const endKey = `bench_key_${String(start + 50).padStart(6, '0')}`;
  db.scan(startKey, endKey);
}
const scanMs = performance.now() - scanStart;
const scanOps = Math.round(SCAN_COUNT / (scanMs / 1000));

console.log(`  \x1b[32m✓\x1b[0m ${scanMs.toFixed(1)}ms — \x1b[1m${scanOps.toLocaleString()} ops/sec\x1b[0m`);

/* ---- Stats ------------------------------------------------------ */
const stats = db.stats();
console.log(`
\x1b[1m Final Engine State\x1b[0m
  MemTable : ${stats.memTable.entries} entries
  SSTables : ${stats.totalSSTables} total
  Levels   : ${stats.levels.filter(l => l.sstables > 0).length} active
`);

// Cleanup
db.close();
rmSync(BENCH_DIR, { recursive: true, force: true });
console.log('\x1b[32m✓ Benchmark complete & cleaned up\x1b[0m\n');
