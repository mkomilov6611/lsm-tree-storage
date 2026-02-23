/**
 * WAL — Write-Ahead Log for crash recovery.
 *
 * Every mutation (put / delete) is appended to an on-disk log file
 * before modifying the MemTable. On recovery the log is replayed
 * to rebuild the MemTable to its pre-crash state.
 *
 * Format (one entry per line):
 *   <timestamp>|<op>|<key>|<value>\n
 */

import { existsSync, mkdirSync, appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';

export default class WAL {
  /**
   * @param {string} filePath — full path to the WAL file (e.g. data/wal.log)
   */
  constructor(filePath) {
    this.filePath = filePath;

    // Ensure parent directory exists
    const dir = dirname(filePath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    // Create the file if it doesn't exist
    if (!existsSync(this.filePath)) {
      writeFileSync(this.filePath, '');
    }
  }

  /**
   * Append a mutation record to the log.
   * Uses synchronous I/O to guarantee the write reaches disk before
   * the in-memory MemTable is mutated.
   *
   * @param {'PUT'|'DELETE'} op
   * @param {string} key
   * @param {string} value
   */
  append(op, key, value) {
    const ts = Date.now();
    const line = `${ts}|${op}|${key}|${value}\n`;
    appendFileSync(this.filePath, line, 'utf8');
  }

  /**
   * Replay the log and return an ordered list of operations.
   * The caller is responsible for feeding these into a fresh MemTable.
   *
   * @returns {{ op: string, key: string, value: string }[]}
   */
  recover() {
    if (!existsSync(this.filePath)) return [];

    const content = readFileSync(this.filePath, 'utf8').trim();
    if (content.length === 0) return [];

    const entries = [];

    for (const line of content.split('\n')) {
      if (!line) continue;
      // Format: timestamp|op|key|value
      const pipeIdx1 = line.indexOf('|');
      const pipeIdx2 = line.indexOf('|', pipeIdx1 + 1);
      const pipeIdx3 = line.indexOf('|', pipeIdx2 + 1);

      if (pipeIdx1 === -1 || pipeIdx2 === -1 || pipeIdx3 === -1) continue;

      const op = line.substring(pipeIdx1 + 1, pipeIdx2);
      const key = line.substring(pipeIdx2 + 1, pipeIdx3);
      const value = line.substring(pipeIdx3 + 1);

      entries.push({ op, key, value });
    }

    return entries;
  }

  /**
   * Clear the WAL (called after a successful MemTable flush to SSTable).
   */
  clear() {
    writeFileSync(this.filePath, '');
  }
}
