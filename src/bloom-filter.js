/**
 * Bloom Filter — probabilistic membership test.
 *
 * A space-efficient data structure that can tell you with certainty
 * that a key is NOT in a set, or that it MIGHT be in the set (with
 * a configurable false-positive rate).
 *
 * Used to avoid unnecessary disk reads when looking up keys in SSTables.
 */

import { BLOOM_FILTER_SIZE, BLOOM_HASH_COUNT } from '../lsm.config.js';

export default class BloomFilter {
  /**
   * @param {number} [size]      — number of bits
   * @param {number} [hashCount] — number of hash functions
   */
  constructor(size = BLOOM_FILTER_SIZE, hashCount = BLOOM_HASH_COUNT) {
    this.size = size;
    this.hashCount = hashCount;
    /** Bit array stored as a Uint8Array (size / 8 bytes) */
    this.bits = new Uint8Array(Math.ceil(size / 8));
  }

  /* ---- Hash helpers --------------------------------------------- */

  /**
   * FNV-1a hash with a seed (used to create multiple independent hashes).
   * @param {string} key
   * @param {number} seed
   * @returns {number}
   */
  _hash(key, seed) {
    let hash = 2166136261 ^ seed;
    for (let i = 0; i < key.length; i++) {
      hash ^= key.charCodeAt(i);
      hash = (hash * 16777619) >>> 0; // multiply and keep 32-bit unsigned
    }
    return hash % this.size;
  }

  /* ---- Public API ----------------------------------------------- */

  /**
   * Add a key to the filter.
   * @param {string} key
   */
  add(key) {
    for (let i = 0; i < this.hashCount; i++) {
      const bit = this._hash(key, i);
      const byteIndex = bit >>> 3;       // bit / 8
      const bitIndex = bit & 7;          // bit % 8
      this.bits[byteIndex] |= (1 << bitIndex);
    }
  }

  /**
   * Test whether a key *might* be in the set.
   * - Returns `false`  → key is **definitely** not present.
   * - Returns `true`   → key is **probably** present (may be false positive).
   *
   * @param {string} key
   * @returns {boolean}
   */
  mightContain(key) {
    for (let i = 0; i < this.hashCount; i++) {
      const bit = this._hash(key, i);
      const byteIndex = bit >>> 3;
      const bitIndex = bit & 7;
      if ((this.bits[byteIndex] & (1 << bitIndex)) === 0) {
        return false;
      }
    }
    return true;
  }

  /* ---- Serialisation -------------------------------------------- */

  /**
   * Serialize the bloom filter to a Buffer for embedding in SSTable files.
   * Format: [size: 4B LE][hashCount: 1B][bits: remaining bytes]
   *
   * @returns {Buffer}
   */
  serialize() {
    const buf = Buffer.alloc(4 + 1 + this.bits.length);
    buf.writeUInt32LE(this.size, 0);
    buf.writeUInt8(this.hashCount, 4);
    Buffer.from(this.bits.buffer, this.bits.byteOffset, this.bits.byteLength)
      .copy(buf, 5);
    return buf;
  }

  /**
   * Deserialize a bloom filter from a Buffer.
   * @param {Buffer} buf
   * @returns {BloomFilter}
   */
  static deserialize(buf) {
    const size = buf.readUInt32LE(0);
    const hashCount = buf.readUInt8(4);
    const bf = new BloomFilter(size, hashCount);
    buf.copy(bf.bits, 0, 5);
    return bf;
  }
}
