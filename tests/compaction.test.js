/**
 * Tests — Compaction (Size-Tiered Strategy)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, rmSync, mkdirSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { SSTableWriter, SSTableReader } from '../src/sstable.js';
import Compaction from '../src/compaction.js';
import { TOMBSTONE, MAX_LEVELS } from '../lsm.config.js';

const TEST_DIR = join(process.cwd(), 'test_data_compaction');

describe('Compaction', () => {
  beforeEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  /**
   * Helper: write an SSTable with the given entries and return
   * the { path, reader } object expected by Compaction.run().
   */
  function createSSTable(entries, filename) {
    const path = join(TEST_DIR, filename);
    SSTableWriter.write(entries, path);
    return { path, reader: SSTableReader.open(path) };
  }

  /** Build an empty levels map */
  function emptyLevels() {
    const levels = new Map();
    for (let i = 0; i < MAX_LEVELS; i++) levels.set(i, []);
    return levels;
  }

  it('should not compact when level has fewer tables than SIZE_RATIO', () => {
    const levels = emptyLevels();
    // Add only 2 SSTables to Level 0 (threshold is 4)
    levels.get(0).push(
      createSSTable([{ key: 'a', value: '1' }], 'L0_001.sst'),
      createSSTable([{ key: 'b', value: '2' }], 'L0_002.sst'),
    );

    const result = Compaction.run(levels, TEST_DIR);
    assert.equal(result.get(0).length, 2, 'Level 0 should still have 2 tables');
    assert.equal(result.get(1).length, 0, 'Level 1 should remain empty');
  });

  it('should compact Level 0 into Level 1 when threshold is reached', () => {
    const levels = emptyLevels();
    // Add 4 SSTables (= SIZE_RATIO) to Level 0
    levels.get(0).push(
      createSSTable([{ key: 'a', value: '1' }, { key: 'd', value: '4' }], 'L0_001.sst'),
      createSSTable([{ key: 'b', value: '2' }], 'L0_002.sst'),
      createSSTable([{ key: 'c', value: '3' }], 'L0_003.sst'),
      createSSTable([{ key: 'e', value: '5' }], 'L0_004.sst'),
    );

    const result = Compaction.run(levels, TEST_DIR);

    assert.equal(result.get(0).length, 0, 'Level 0 should be empty after compaction');
    assert.equal(result.get(1).length, 1, 'Level 1 should have 1 merged SSTable');

    // Verify merged data
    const reader = result.get(1)[0].reader;
    assert.equal(reader.get('a'), '1');
    assert.equal(reader.get('b'), '2');
    assert.equal(reader.get('c'), '3');
    assert.equal(reader.get('d'), '4');
    assert.equal(reader.get('e'), '5');
  });

  it('should resolve duplicate keys (first/newest SSTable wins)', () => {
    const levels = emptyLevels();
    // Table at index 0 is "newest" — its value for 'x' should win
    levels.get(0).push(
      createSSTable([{ key: 'x', value: 'NEW' }], 'L0_004.sst'),
      createSSTable([{ key: 'x', value: 'OLD_1' }], 'L0_003.sst'),
      createSSTable([{ key: 'x', value: 'OLD_2' }], 'L0_002.sst'),
      createSSTable([{ key: 'y', value: 'Y' }], 'L0_001.sst'),
    );

    const result = Compaction.run(levels, TEST_DIR);
    const reader = result.get(1)[0].reader;
    assert.equal(reader.get('x'), 'NEW');
    assert.equal(reader.get('y'), 'Y');
  });

  it('should drop tombstones at the bottom-most level', () => {
    const levels = emptyLevels();
    // Level 0 has 4 tables, one with a tombstone
    levels.get(0).push(
      createSSTable([{ key: 'a', value: TOMBSTONE }], 'L0_004.sst'),
      createSSTable([{ key: 'b', value: '2' }], 'L0_003.sst'),
      createSSTable([{ key: 'c', value: '3' }], 'L0_002.sst'),
      createSSTable([{ key: 'd', value: '4' }], 'L0_001.sst'),
    );
    // No older levels → tombstones should be dropped

    const result = Compaction.run(levels, TEST_DIR);
    const reader = result.get(1)[0].reader;
    assert.equal(reader.get('a'), null, 'Tombstoned key should be dropped');
    assert.equal(reader.get('b'), '2');
  });

  it('should preserve tombstones when older levels have data', () => {
    const levels = emptyLevels();
    // Level 0: 4 tables, one with a tombstone
    levels.get(0).push(
      createSSTable([{ key: 'a', value: TOMBSTONE }], 'L0_004.sst'),
      createSSTable([{ key: 'b', value: '2' }], 'L0_003.sst'),
      createSSTable([{ key: 'c', value: '3' }], 'L0_002.sst'),
      createSSTable([{ key: 'd', value: '4' }], 'L0_001.sst'),
    );
    // Level 2 has data → tombstone must be preserved
    levels.get(2).push(
      createSSTable([{ key: 'a', value: 'old_val' }], 'L2_001.sst'),
    );

    const result = Compaction.run(levels, TEST_DIR);
    const reader = result.get(1)[0].reader;
    // Tombstone should still be present
    assert.equal(reader.get('a'), TOMBSTONE);
  });

  it('should remove old SSTable files from disk after compaction', () => {
    const levels = emptyLevels();
    const oldPaths = [];
    for (let i = 0; i < 4; i++) {
      const filename = `L0_00${i}.sst`;
      const table = createSSTable([{ key: `k${i}`, value: `v${i}` }], filename);
      levels.get(0).push(table);
      oldPaths.push(table.path);
    }

    Compaction.run(levels, TEST_DIR);

    // Old files should be deleted
    for (const p of oldPaths) {
      assert.equal(existsSync(p), false, `Old SSTable ${p} should be deleted`);
    }

    // New merged file should exist
    const remaining = readdirSync(TEST_DIR).filter(f => f.endsWith('.sst'));
    assert.equal(remaining.length, 1, 'Should have exactly 1 merged SSTable');
    assert.ok(remaining[0].startsWith('L1_'), 'Merged file should be at Level 1');
  });

  it('should produce sorted entries in the merged SSTable', () => {
    const levels = emptyLevels();
    levels.get(0).push(
      createSSTable([{ key: 'z', value: '26' }, { key: 'm', value: '13' }].sort((a,b) => a.key < b.key ? -1 : 1), 'L0_004.sst'),
      createSSTable([{ key: 'a', value: '1' }, { key: 'f', value: '6' }], 'L0_003.sst'),
      createSSTable([{ key: 'c', value: '3' }], 'L0_002.sst'),
      createSSTable([{ key: 'p', value: '16' }], 'L0_001.sst'),
    );

    const result = Compaction.run(levels, TEST_DIR);
    const entries = result.get(1)[0].reader.entries();
    for (let i = 1; i < entries.length; i++) {
      assert.ok(entries[i - 1].key < entries[i].key, `Expected sorted: ${entries[i-1].key} < ${entries[i].key}`);
    }
  });
});
