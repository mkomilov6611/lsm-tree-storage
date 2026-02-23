/**
 * LSMTree — Main orchestrator for the LSM-tree storage engine.
 *
 * Coordinates the MemTable, WAL, SSTables, and Compaction to provide
 * a unified key-value store with put / get / delete / scan operations.
 */

import { existsSync, mkdirSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

import MemTable from './memtable.js';
import WAL from './wal.js';
import { SSTableWriter, SSTableReader } from './sstable.js';
import Compaction from './compaction.js';
import {
  DATA_DIR,
  MEMTABLE_SIZE_THRESHOLD,
  TOMBSTONE,
  MAX_LEVELS,
} from '../lsm.config.js';

export default class LSMTree {
  /**
   * @param {string} [dataDir] – directory for WAL + SSTables
   */
  constructor(dataDir = DATA_DIR) {
    this.dataDir = dataDir;

    // Ensure data directory exists
    if (!existsSync(this.dataDir)) {
      mkdirSync(this.dataDir, { recursive: true });
    }

    // ---- Core components ----
    this.memTable = new MemTable();
    this.wal = new WAL(join(this.dataDir, 'wal.log'));

    /**
     * Map<level, { path, reader }[]>
     * Level 0 = newest (freshly flushed), higher = older/compacted.
     * Within each level array, index 0 is the newest SSTable.
     */
    this.levels = new Map();
    for (let i = 0; i < MAX_LEVELS; i++) {
      this.levels.set(i, []);
    }

    // ---- Bootstrap ----
    this._loadExistingSSTables();
    this._recoverWAL();
  }

  /* ================================================================ */
  /*  Write path                                                       */
  /* ================================================================ */

  /**
   * Insert or update a key-value pair.
   * @param {string} key
   * @param {string} value
   */
  put(key, value) {
    this.wal.append('PUT', key, value);
    this.memTable.put(key, value);
    this._maybeFlush();
  }

  /**
   * Delete a key (tombstone marker).
   * @param {string} key
   */
  delete(key) {
    this.wal.append('DELETE', key, TOMBSTONE);
    this.memTable.delete(key);
    this._maybeFlush();
  }

  /* ================================================================ */
  /*  Read path                                                        */
  /* ================================================================ */

  /**
   * Retrieve the value for a key.
   * Checks MemTable first, then SSTables from newest to oldest level.
   *
   * @param {string} key
   * @returns {string|null} The value, or null if not found / deleted.
   */
  get(key) {
    // 1. Check MemTable (hot data)
    const memVal = this.memTable.get(key);
    if (memVal !== null) {
      return memVal === TOMBSTONE ? null : memVal;
    }

    // 2. Check SSTables level-by-level, newest first
    for (let lvl = 0; lvl < MAX_LEVELS; lvl++) {
      const tables = this.levels.get(lvl) || [];
      // Within a level, newest table is at index 0
      for (const table of tables) {
        const val = table.reader.get(key);
        if (val !== null) {
          return val === TOMBSTONE ? null : val;
        }
      }
    }

    return null;
  }

  /**
   * Range scan — returns sorted entries where startKey <= key <= endKey.
   * Merges results from MemTable and all SSTables, newest wins.
   *
   * @param {string} startKey
   * @param {string} endKey
   * @returns {{ key: string, value: string }[]}
   */
  scan(startKey, endKey) {
    /** @type {Map<string, string>} newest value per key */
    const merged = new Map();

    // Scan SSTables from oldest to newest so that newer values overwrite
    for (let lvl = MAX_LEVELS - 1; lvl >= 0; lvl--) {
      const tables = this.levels.get(lvl) || [];
      for (let i = tables.length - 1; i >= 0; i--) {
        for (const { key, value } of tables[i].reader.scan(startKey, endKey)) {
          merged.set(key, value);
        }
      }
    }

    // MemTable entries are newest — overwrite any SSTable values
    for (const { key, value } of this.memTable.scan(startKey, endKey)) {
      merged.set(key, value);
    }

    // Filter out tombstones and return sorted
    return Array.from(merged.entries())
      .filter(([, v]) => v !== TOMBSTONE)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, value]) => ({ key, value }));
  }

  /* ================================================================ */
  /*  Flush & Compaction                                               */
  /* ================================================================ */

  /** Flush MemTable to a new Level-0 SSTable if threshold exceeded. */
  _maybeFlush() {
    if (this.memTable.size >= MEMTABLE_SIZE_THRESHOLD) {
      this.flush();
    }
  }

  /**
   * Force-flush the current MemTable to a Level-0 SSTable.
   */
  flush() {
    const entries = this.memTable.entries();
    if (entries.length === 0) return;

    const ts = Date.now();
    const sstPath = join(this.dataDir, `L0_${ts}.sst`);

    SSTableWriter.write(entries, sstPath);

    // Add to level 0 (front = newest)
    const l0 = this.levels.get(0);
    l0.unshift({ path: sstPath, reader: SSTableReader.open(sstPath) });

    // Reset MemTable & WAL
    this.memTable.clear();
    this.wal.clear();

    // Trigger compaction check
    this.levels = Compaction.run(this.levels, this.dataDir);
  }

  /* ================================================================ */
  /*  Bootstrap & Recovery                                             */
  /* ================================================================ */

  /** Load existing SSTable files from disk on startup. */
  _loadExistingSSTables() {
    if (!existsSync(this.dataDir)) return;

    const files = readdirSync(this.dataDir)
      .filter(f => f.endsWith('.sst'))
      .sort(); // sorted by name → by timestamp within each level

    for (const file of files) {
      const match = file.match(/^L(\d+)_(\d+)\.sst$/);
      if (!match) continue;

      const level = parseInt(match[1], 10);
      const filePath = join(this.dataDir, file);

      try {
        const reader = SSTableReader.open(filePath);
        const tables = this.levels.get(level) || [];
        tables.push({ path: filePath, reader });
        this.levels.set(level, tables);
      } catch (err) {
        console.error(`Warning: skipping corrupt SSTable ${file}: ${err.message}`);
      }
    }

    // Within each level, sort newest first (higher timestamp = newer)
    for (const [, tables] of this.levels) {
      tables.sort((a, b) => {
        const tsA = parseInt(a.path.match(/_(\d+)\.sst$/)[1], 10);
        const tsB = parseInt(b.path.match(/_(\d+)\.sst$/)[1], 10);
        return tsB - tsA; // newest first
      });
    }
  }

  /** Replay WAL entries into the MemTable (crash recovery). */
  _recoverWAL() {
    const entries = this.wal.recover();
    for (const { op, key, value } of entries) {
      if (op === 'PUT') {
        this.memTable.put(key, value);
      } else if (op === 'DELETE') {
        this.memTable.delete(key);
      }
    }
  }

  /* ================================================================ */
  /*  Statistics                                                       */
  /* ================================================================ */

  /**
   * Return engine statistics for CLI / UI display.
   */
  stats() {
    const levelStats = [];
    let totalSSTables = 0;
    let totalSizeBytes = 0;

    for (let lvl = 0; lvl < MAX_LEVELS; lvl++) {
      const tables = this.levels.get(lvl) || [];
      let levelSize = 0;

      for (const t of tables) {
        try {
          const s = statSync(t.path);
          levelSize += s.size;
        } catch { /* file may be gone after compaction */ }
      }

      totalSSTables += tables.length;
      totalSizeBytes += levelSize;

      levelStats.push({
        level: lvl,
        sstables: tables.length,
        sizeBytes: levelSize,
      });
    }

    return {
      memTable: {
        entries: this.memTable.count,
        sizeBytes: this.memTable.size,
      },
      levels: levelStats,
      totalSSTables,
      totalSizeBytes,
    };
  }

  /**
   * Gracefully close the engine — flush any remaining MemTable data.
   */
  close() {
    if (this.memTable.count > 0) {
      this.flush();
    }
  }

  /* ================================================================ */
  /*  Inspector (for Web UI)                                           */
  /* ================================================================ */

  /**
   * Return detailed internal data for the inspector panel.
   * @param {number} [limit=100] – max entries to return per component
   */
  inspect(limit = 100) {
    // ---- MemTable entries ----
    const memEntries = this.memTable.entries().slice(0, limit);

    // ---- SSTable details ----
    const sstables = [];
    for (let lvl = 0; lvl < MAX_LEVELS; lvl++) {
      const tables = this.levels.get(lvl) || [];
      for (const t of tables) {
        let fileSize = 0;
        try { fileSize = statSync(t.path).size; } catch {}

        const filename = t.path.split('/').pop();
        const allEntries = t.reader.entries();
        const sample = allEntries.slice(0, limit);

        sstables.push({
          filename,
          level: lvl,
          entryCount: t.reader.entryCount,
          sizeBytes: fileSize,
          sparseIndexSize: t.reader.sparseIndex.length,
          bloomFilterBits: t.reader.bloom.size,
          entries: sample,
        });
      }
    }

    return { memTable: memEntries, sstables };
  }
}
