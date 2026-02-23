/**
 * Tests — MemTable (Skip List)
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import MemTable from '../src/memtable.js';
import { TOMBSTONE } from '../lsm.config.js';

describe('MemTable', () => {
  let mem;

  beforeEach(() => {
    mem = new MemTable();
  });

  it('should put and get a value', () => {
    mem.put('name', 'Alice');
    assert.equal(mem.get('name'), 'Alice');
  });

  it('should return null for missing key', () => {
    assert.equal(mem.get('missing'), null);
  });

  it('should update existing key', () => {
    mem.put('name', 'Alice');
    mem.put('name', 'Bob');
    assert.equal(mem.get('name'), 'Bob');
  });

  it('should delete a key with a tombstone', () => {
    mem.put('name', 'Alice');
    mem.delete('name');
    assert.equal(mem.get('name'), TOMBSTONE);
  });

  it('should return sorted entries', () => {
    mem.put('c', '3');
    mem.put('a', '1');
    mem.put('b', '2');
    const entries = mem.entries();
    assert.deepEqual(entries.map(e => e.key), ['a', 'b', 'c']);
  });

  it('should scan a range', () => {
    mem.put('a', '1');
    mem.put('b', '2');
    mem.put('c', '3');
    mem.put('d', '4');
    const result = mem.scan('b', 'c');
    assert.deepEqual(result.map(e => e.key), ['b', 'c']);
  });

  it('should track entry count', () => {
    mem.put('a', '1');
    mem.put('b', '2');
    assert.equal(mem.count, 2);
  });

  it('should track byte size', () => {
    mem.put('k', 'v');      // 1 + 1 = 2 bytes
    assert.ok(mem.size > 0);
    const sizeBefore = mem.size;
    mem.put('k', 'longer'); // update — size should change
    assert.ok(mem.size > sizeBefore);
  });

  it('should clear all entries', () => {
    mem.put('a', '1');
    mem.put('b', '2');
    mem.clear();
    assert.equal(mem.count, 0);
    assert.equal(mem.size, 0);
    assert.equal(mem.get('a'), null);
  });

  it('should handle many keys', () => {
    for (let i = 0; i < 1000; i++) {
      mem.put(`key_${String(i).padStart(4, '0')}`, `val_${i}`);
    }
    assert.equal(mem.count, 1000);
    assert.equal(mem.get('key_0500'), 'val_500');

    const entries = mem.entries();
    for (let i = 1; i < entries.length; i++) {
      assert.ok(entries[i - 1].key < entries[i].key, 'entries should be sorted');
    }
  });
});
