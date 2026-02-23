/**
 * SSTable — Sorted String Table.
 *
 * Immutable on-disk file containing sorted key-value pairs with a
 * sparse index and an embedded Bloom filter for fast lookups.
 *
 * Binary format:
 * ┌───────────────────────────────────────────────┐
 * │ Header: magic(4B) | version(1B) | count(4B)   │
 * ├───────────────────────────────────────────────┤
 * │ Data Block: repeated                          │
 * │   keyLen(2B) | key | valueLen(4B) | value     │
 * ├───────────────────────────────────────────────┤
 * │ Index Block: repeated                         │
 * │   keyLen(2B) | key | offset(4B)               │
 * ├───────────────────────────────────────────────┤
 * │ Bloom Filter Block: serialized BloomFilter    │
 * ├───────────────────────────────────────────────┤
 * │ Footer:                                       │
 * │   dataOffset(4B) | indexOffset(4B)            │
 * │   bloomOffset(4B) | footerMagic(4B)           │
 * └───────────────────────────────────────────────┘
 */

import {
  writeFileSync,
  readFileSync,
  existsSync,
} from 'node:fs';
import BloomFilter from './bloom-filter.js';
import {
  SSTABLE_MAGIC,
  SSTABLE_VERSION,
  SPARSE_INDEX_INTERVAL,
} from '../lsm.config.js';

/* ================================================================== */
/*  SSTable Writer                                                    */
/* ================================================================== */

export class SSTableWriter {
  /**
   * Write a sorted array of { key, value } entries to an SSTable file.
   *
   * @param {{ key: string, value: string }[]} entries – MUST be sorted by key
   * @param {string} filePath
   */
  static write(entries, filePath) {
    const buffers = [];
    let offset = 0;

    /* ---- Header ------------------------------------------------- */
    const header = Buffer.alloc(9);
    header.writeUInt32LE(SSTABLE_MAGIC, 0);
    header.writeUInt8(SSTABLE_VERSION, 4);
    header.writeUInt32LE(entries.length, 5);
    buffers.push(header);
    offset += 9;

    const dataOffset = offset;

    /* ---- Data Block --------------------------------------------- */
    const bloom = new BloomFilter();
    const sparseIndex = []; // { key, offset }

    for (let i = 0; i < entries.length; i++) {
      const { key, value } = entries[i];
      bloom.add(key);

      // Sparse index: save an entry every SPARSE_INDEX_INTERVAL keys
      if (i % SPARSE_INDEX_INTERVAL === 0) {
        sparseIndex.push({ key, offset });
      }

      const keyBuf = Buffer.from(key, 'utf8');
      const valBuf = Buffer.from(value, 'utf8');

      const entryHeader = Buffer.alloc(6);
      entryHeader.writeUInt16LE(keyBuf.length, 0);
      entryHeader.writeUInt32LE(valBuf.length, 2);

      buffers.push(entryHeader, keyBuf, valBuf);
      offset += 6 + keyBuf.length + valBuf.length;
    }

    const indexOffset = offset;

    /* ---- Index Block -------------------------------------------- */
    for (const entry of sparseIndex) {
      const keyBuf = Buffer.from(entry.key, 'utf8');
      const idxEntry = Buffer.alloc(6);
      idxEntry.writeUInt16LE(keyBuf.length, 0);
      idxEntry.writeUInt32LE(entry.offset, 2);

      buffers.push(idxEntry, keyBuf);
      offset += 6 + keyBuf.length;
    }

    const bloomOffset = offset;

    /* ---- Bloom Filter Block ------------------------------------- */
    const bloomBuf = bloom.serialize();
    buffers.push(bloomBuf);
    offset += bloomBuf.length;

    /* ---- Footer ------------------------------------------------- */
    const footer = Buffer.alloc(16);
    footer.writeUInt32LE(dataOffset, 0);
    footer.writeUInt32LE(indexOffset, 4);
    footer.writeUInt32LE(bloomOffset, 8);
    footer.writeUInt32LE(SSTABLE_MAGIC, 12);
    buffers.push(footer);

    /* ---- Flush to disk ------------------------------------------ */
    writeFileSync(filePath, Buffer.concat(buffers));
  }
}

/* ================================================================== */
/*  SSTable Reader                                                    */
/* ================================================================== */

export class SSTableReader {
  /**
   * Open an existing SSTable file and load its index + bloom filter
   * into memory for efficient lookups.
   *
   * @param {string} filePath
   * @returns {SSTableReader}
   */
  static open(filePath) {
    const reader = new SSTableReader();
    reader.filePath = filePath;
    reader.buf = readFileSync(filePath);
    reader._parseFooter();
    reader._parseBloom();
    reader._parseIndex();
    return reader;
  }

  /* ---- Internal parsers ----------------------------------------- */

  _parseFooter() {
    const f = this.buf;
    const footerStart = f.length - 16;
    this.dataOffset = f.readUInt32LE(footerStart);
    this.indexOffset = f.readUInt32LE(footerStart + 4);
    this.bloomOffset = f.readUInt32LE(footerStart + 8);
    const magic = f.readUInt32LE(footerStart + 12);
    if (magic !== SSTABLE_MAGIC) {
      throw new Error(`Invalid SSTable file: bad footer magic in ${this.filePath}`);
    }
    // Read header
    const headerMagic = f.readUInt32LE(0);
    if (headerMagic !== SSTABLE_MAGIC) {
      throw new Error(`Invalid SSTable file: bad header magic in ${this.filePath}`);
    }
    this.entryCount = f.readUInt32LE(5);
  }

  _parseBloom() {
    const bloomBuf = this.buf.subarray(this.bloomOffset, this.buf.length - 16);
    this.bloom = BloomFilter.deserialize(bloomBuf);
  }

  _parseIndex() {
    this.sparseIndex = [];
    let pos = this.indexOffset;
    const end = this.bloomOffset;

    while (pos < end) {
      const keyLen = this.buf.readUInt16LE(pos);
      const offset = this.buf.readUInt32LE(pos + 2);
      pos += 6;
      const key = this.buf.toString('utf8', pos, pos + keyLen);
      pos += keyLen;
      this.sparseIndex.push({ key, offset });
    }
  }

  /* ---- Public API ----------------------------------------------- */

  /**
   * Look up a single key.
   * @param {string} key
   * @returns {string|null} The value, or null if not found.
   */
  get(key) {
    // 1. Bloom filter check – fast path rejection
    if (!this.bloom.mightContain(key)) return null;

    // 2. Use sparse index to find the data segment to scan
    let scanStart = this.dataOffset;
    let scanEnd = this.indexOffset;

    for (let i = 0; i < this.sparseIndex.length; i++) {
      if (this.sparseIndex[i].key > key) {
        scanEnd =
          i < this.sparseIndex.length
            ? this.sparseIndex[i].offset
            : this.indexOffset;
        break;
      }
      scanStart = this.sparseIndex[i].offset;
    }

    // 3. Linear scan of the data block segment
    let pos = scanStart;
    while (pos < scanEnd) {
      const keyLen = this.buf.readUInt16LE(pos);
      const valLen = this.buf.readUInt32LE(pos + 2);
      pos += 6;
      const k = this.buf.toString('utf8', pos, pos + keyLen);
      pos += keyLen;

      if (k === key) {
        return this.buf.toString('utf8', pos, pos + valLen);
      }

      // Keys are sorted — if we've passed the target, stop
      if (k > key) return null;

      pos += valLen;
    }

    return null;
  }

  /**
   * Range scan.
   * @param {string} startKey
   * @param {string} endKey
   * @returns {{ key: string, value: string }[]}
   */
  scan(startKey, endKey) {
    const result = [];
    let pos = this.dataOffset;
    const end = this.indexOffset;

    while (pos < end) {
      const keyLen = this.buf.readUInt16LE(pos);
      const valLen = this.buf.readUInt32LE(pos + 2);
      pos += 6;
      const key = this.buf.toString('utf8', pos, pos + keyLen);
      pos += keyLen;
      const value = this.buf.toString('utf8', pos, pos + valLen);
      pos += valLen;

      if (key > endKey) break;
      if (key >= startKey) {
        result.push({ key, value });
      }
    }

    return result;
  }

  /**
   * Iterate all entries (used during compaction).
   * @returns {{ key: string, value: string }[]}
   */
  entries() {
    const result = [];
    let pos = this.dataOffset;
    const end = this.indexOffset;

    while (pos < end) {
      const keyLen = this.buf.readUInt16LE(pos);
      const valLen = this.buf.readUInt32LE(pos + 2);
      pos += 6;
      const key = this.buf.toString('utf8', pos, pos + keyLen);
      pos += keyLen;
      const value = this.buf.toString('utf8', pos, pos + valLen);
      pos += valLen;

      result.push({ key, value });
    }

    return result;
  }
}
