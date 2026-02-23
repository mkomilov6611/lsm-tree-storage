/**
 * Tests — LSMTree (full integration)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import LSMTree from '../src/lsm-tree.js';

const TEST_DIR = join(process.cwd(), 'test_data_lsm');

describe('LSMTree', () => {
  let db;

  beforeEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    db = new LSMTree(TEST_DIR);
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  it('should put and get a value', () => {
    db.put('name', 'Alice');
    assert.equal(db.get('name'), 'Alice');
  });

  it('should return null for missing key', () => {
    assert.equal(db.get('missing'), null);
  });

  it('should update a key', () => {
    db.put('name', 'Alice');
    db.put('name', 'Bob');
    assert.equal(db.get('name'), 'Bob');
  });

  it('should delete a key', () => {
    db.put('name', 'Alice');
    db.delete('name');
    assert.equal(db.get('name'), null);
  });

  it('should scan a range', () => {
    db.put('a', '1');
    db.put('b', '2');
    db.put('c', '3');
    db.put('d', '4');
    const result = db.scan('b', 'c');
    assert.equal(result.length, 2);
    assert.equal(result[0].key, 'b');
    assert.equal(result[1].key, 'c');
  });

  it('should flush MemTable to SSTable and still serve reads', () => {
    db.put('x', '10');
    db.put('y', '20');
    db.flush();

    // Data should now be on disk, not in MemTable
    assert.equal(db.memTable.count, 0);
    assert.equal(db.get('x'), '10');
    assert.equal(db.get('y'), '20');
  });

  it('should recover from WAL after crash', () => {
    db.put('name', 'Alice');
    db.put('age', '30');
    // Simulate crash: don't call flush() or close()
    // Re-open the database
    const db2 = new LSMTree(TEST_DIR);
    assert.equal(db2.get('name'), 'Alice');
    assert.equal(db2.get('age'), '30');
  });

  it('should handle delete across flush boundary', () => {
    db.put('key1', 'val1');
    db.flush();
    // key1 is now in SSTable
    db.delete('key1');
    // Delete is in MemTable as tombstone
    assert.equal(db.get('key1'), null);

    // Flush the tombstone
    db.flush();
    assert.equal(db.get('key1'), null);
  });

  it('should return stats', () => {
    db.put('a', '1');
    const stats = db.stats();
    assert.ok(stats.memTable);
    assert.equal(stats.memTable.entries, 1);
    assert.ok(stats.levels);
  });

  it('should handle many writes triggering auto-flush', () => {
    // Write enough data to trigger at least one auto-flush (64KB threshold)
    for (let i = 0; i < 2000; i++) {
      db.put(`key_${String(i).padStart(5, '0')}`, `value_${'x'.repeat(30)}_${i}`);
    }
    // Should have flushed to SSTables
    const stats = db.stats();
    assert.ok(stats.totalSSTables > 0, 'Expected at least one SSTable after bulk writes');

    // Verify random reads
    assert.equal(db.get('key_00000'), `value_${'x'.repeat(30)}_0`);
    assert.equal(db.get('key_01000'), `value_${'x'.repeat(30)}_1000`);
    assert.equal(db.get('key_01999'), `value_${'x'.repeat(30)}_1999`);
  });

  it('should return correct scan results across MemTable and SSTables', () => {
    // Populate and flush some data
    db.put('fruit:apple', 'red');
    db.put('fruit:banana', 'yellow');
    db.flush();

    // Add more data to MemTable
    db.put('fruit:cherry', 'dark red');
    db.put('fruit:banana', 'green'); // update

    const results = db.scan('fruit:a', 'fruit:d');
    assert.equal(results.length, 3);
    assert.equal(results.find(r => r.key === 'fruit:banana').value, 'green');
  });
});
