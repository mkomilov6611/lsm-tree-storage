/**
 * Compaction — Size-Tiered Compaction Strategy.
 *
 * When a level accumulates too many SSTables (≥ SIZE_RATIO), they
 * are merged into a single SSTable at the next level. During the
 * merge, duplicate keys are resolved (newest wins) and fully
 * shadowed tombstones are dropped.
 */

import { unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { SSTableWriter, SSTableReader } from './sstable.js';
import { TOMBSTONE, SIZE_RATIO, MAX_LEVELS } from '../lsm.config.js';

export default class Compaction {
  /**
   * Check each level and compact if the SSTable count exceeds SIZE_RATIO.
   *
   * @param {Map<number, { path: string, reader: SSTableReader }[]>} levels
   *        Map of level → array of SSTable metadata objects.
   * @param {string} dataDir
   * @returns {Map<number, { path: string, reader: SSTableReader }[]>}
   *          Updated levels map after compaction.
   */
  static run(levels, dataDir) {
    for (let lvl = 0; lvl < MAX_LEVELS - 1; lvl++) {
      const tables = levels.get(lvl) || [];

      if (tables.length < SIZE_RATIO) continue;

      // ---- Gather all entries from this level ----
      const allEntries = Compaction._kWayMerge(tables.map(t => t.reader));

      // ---- Determine if there are older levels below ----
      let hasOlderLevels = false;
      for (let older = lvl + 1; older < MAX_LEVELS; older++) {
        if ((levels.get(older) || []).length > 0) {
          hasOlderLevels = true;
          break;
        }
      }

      // Drop tombstones only when there's no older data that might still
      // reference the key (i.e., this is the bottom-most level with data).
      const merged = hasOlderLevels
        ? allEntries
        : allEntries.filter(e => e.value !== TOMBSTONE);

      // ---- Write merged SSTable to the next level ----
      const nextLvl = lvl + 1;
      const ts = Date.now();
      const newPath = join(dataDir, `L${nextLvl}_${ts}.sst`);

      if (merged.length > 0) {
        SSTableWriter.write(merged, newPath);
      }

      // ---- Remove old SSTables from disk ----
      for (const table of tables) {
        try {
          unlinkSync(table.path);
        } catch { /* file may already be gone */ }
      }

      // ---- Update level maps ----
      levels.set(lvl, []);

      if (merged.length > 0) {
        const nextTables = levels.get(nextLvl) || [];
        nextTables.push({ path: newPath, reader: SSTableReader.open(newPath) });
        levels.set(nextLvl, nextTables);
      }
    }

    return levels;
  }

  /**
   * K-way merge of sorted SSTable iterators.
   * When the same key appears in multiple tables, the first occurrence
   * wins (tables are ordered newest → oldest within a level, but within
   * one level they're all the same "age" so we just keep the latest).
   *
   * Since entries within each SSTable are already sorted, we do a
   * standard merge-sort merge.
   *
   * @param {SSTableReader[]} readers
   * @returns {{ key: string, value: string }[]}
   */
  static _kWayMerge(readers) {
    // Load all entry arrays
    const iterators = readers.map(r => ({ entries: r.entries(), idx: 0 }));

    /** @type {{ key: string, value: string }[]} */
    const merged = [];

    // Simple multi-way merge using a min-pick loop
    while (true) {
      let minKey = null;
      let minVal = null;
      let minSources = []; // indices of iterators sharing the minKey

      for (let i = 0; i < iterators.length; i++) {
        const it = iterators[i];
        if (it.idx >= it.entries.length) continue;

        const entry = it.entries[it.idx];

        if (minKey === null || entry.key < minKey) {
          minKey = entry.key;
          minVal = entry.value;
          minSources = [i];
        } else if (entry.key === minKey) {
          // Duplicate key — take the value from the newer SSTable (lower index)
          minSources.push(i);
        }
      }

      if (minKey === null) break; // all iterators exhausted

      merged.push({ key: minKey, value: minVal });

      // Advance all iterators that had the minKey
      for (const i of minSources) {
        iterators[i].idx++;
      }
    }

    return merged;
  }
}
