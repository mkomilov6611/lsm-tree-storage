/**
 * Tests — SSTable (Writer + Reader)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { SSTableWriter, SSTableReader } from '../src/sstable.js';

const TEST_DIR = join(process.cwd(), 'test_data_sstable');

describe('SSTable', () => {
  beforeEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  it('should write and read back entries', () => {
    const entries = [
      { key: 'apple', value: 'red' },
      { key: 'banana', value: 'yellow' },
      { key: 'cherry', value: 'dark red' },
    ];

    const path = join(TEST_DIR, 'test1.sst');
    SSTableWriter.write(entries, path);

    const reader = SSTableReader.open(path);
    assert.equal(reader.get('apple'), 'red');
    assert.equal(reader.get('banana'), 'yellow');
    assert.equal(reader.get('cherry'), 'dark red');
    assert.equal(reader.get('missing'), null);
  });

  it('should support range scans', () => {
    const entries = [];
    for (let i = 0; i < 100; i++) {
      entries.push({
        key: `key_${String(i).padStart(3, '0')}`,
        value: `val_${i}`,
      });
    }

    const path = join(TEST_DIR, 'test2.sst');
    SSTableWriter.write(entries, path);

    const reader = SSTableReader.open(path);
    const results = reader.scan('key_010', 'key_015');
    assert.equal(results.length, 6);
    assert.equal(results[0].key, 'key_010');
    assert.equal(results[5].key, 'key_015');
  });

  it('should return all entries via entries()', () => {
    const entries = [
      { key: 'x', value: '1' },
      { key: 'y', value: '2' },
      { key: 'z', value: '3' },
    ];

    const path = join(TEST_DIR, 'test3.sst');
    SSTableWriter.write(entries, path);

    const reader = SSTableReader.open(path);
    const all = reader.entries();
    assert.equal(all.length, 3);
    assert.deepEqual(all, entries);
  });

  it('should handle large SSTables with sparse index', () => {
    const entries = [];
    for (let i = 0; i < 500; i++) {
      entries.push({
        key: `k_${String(i).padStart(4, '0')}`,
        value: `value_is_${i}_with_some_padding_data`,
      });
    }

    const path = join(TEST_DIR, 'test_large.sst');
    SSTableWriter.write(entries, path);

    const reader = SSTableReader.open(path);
    // Check random access via sparse index
    assert.equal(reader.get('k_0000'), 'value_is_0_with_some_padding_data');
    assert.equal(reader.get('k_0250'), 'value_is_250_with_some_padding_data');
    assert.equal(reader.get('k_0499'), 'value_is_499_with_some_padding_data');
    assert.equal(reader.get('k_9999'), null);
  });
});
