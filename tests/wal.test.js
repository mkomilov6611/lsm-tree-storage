/**
 * Tests — WAL (Write-Ahead Log)
 */

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, rmSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import WAL from '../src/wal.js';

const TEST_DIR = join(process.cwd(), 'test_data_wal');

describe('WAL', () => {
  if (!existsSync(TEST_DIR)) mkdirSync(TEST_DIR, { recursive: true });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(TEST_DIR, { recursive: true });
  });

  it('should append and recover entries', () => {
    const walPath = join(TEST_DIR, 'test.log');
    const wal = new WAL(walPath);

    wal.append('PUT', 'name', 'Alice');
    wal.append('PUT', 'age', '30');
    wal.append('DELETE', 'name', '__TOMBSTONE__');

    const entries = wal.recover();
    assert.equal(entries.length, 3);
    assert.equal(entries[0].op, 'PUT');
    assert.equal(entries[0].key, 'name');
    assert.equal(entries[0].value, 'Alice');
    assert.equal(entries[2].op, 'DELETE');
  });

  it('should clear the log', () => {
    const walPath = join(TEST_DIR, 'test_clear.log');
    const wal = new WAL(walPath);

    wal.append('PUT', 'x', '1');
    wal.clear();

    const entries = wal.recover();
    assert.equal(entries.length, 0);
  });

  it('should recover from an empty log', () => {
    const walPath = join(TEST_DIR, 'empty.log');
    const wal = new WAL(walPath);

    const entries = wal.recover();
    assert.equal(entries.length, 0);
  });

  it('should handle values containing pipe characters', () => {
    const walPath = join(TEST_DIR, 'pipes.log');
    const wal = new WAL(walPath);

    wal.append('PUT', 'data', 'a|b|c');
    const entries = wal.recover();
    assert.equal(entries[0].value, 'a|b|c');
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });
});
