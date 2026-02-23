/**
 * MemTable — In-memory sorted key-value store backed by a Skip List.
 *
 * A skip list is a probabilistic data structure that provides O(log n)
 * average time for insert, search, and delete operations while maintaining
 * sorted order — making it ideal for the MemTable layer of an LSM-tree.
 */

import { TOMBSTONE } from '../lsm.config.js';

/* ------------------------------------------------------------------ */
/*  Skip List Node                                                     */
/* ------------------------------------------------------------------ */

class SkipListNode {
  /**
   * @param {string} key
   * @param {string|null} value
   * @param {number} level  – number of forward pointers (1-based height)
   */
  constructor(key, value, level) {
    this.key = key;
    this.value = value;
    /** @type {SkipListNode[]} forward pointers, index 0 = bottom level */
    this.forward = new Array(level).fill(null);
  }
}

/* ------------------------------------------------------------------ */
/*  Skip List                                                          */
/* ------------------------------------------------------------------ */

const MAX_LEVEL = 16;
const P = 0.5; // probability for level promotion

class SkipList {
  constructor() {
    /** Sentinel head node – key is never matched */
    this.head = new SkipListNode(null, null, MAX_LEVEL);
    /** Current maximum level in the list (0-indexed) */
    this.level = 0;
    /** Number of logical entries (including tombstones) */
    this.count = 0;
    /** Approximate byte size of stored data */
    this.byteSize = 0;
  }

  /* ---- helpers --------------------------------------------------- */

  /** Generate a random level using geometric distribution */
  _randomLevel() {
    let lvl = 0;
    while (Math.random() < P && lvl < MAX_LEVEL - 1) lvl++;
    return lvl;
  }

  /* ---- public API ------------------------------------------------ */

  /**
   * Insert or update a key-value pair.
   * @param {string} key
   * @param {string} value
   */
  put(key, value) {
    const update = new Array(MAX_LEVEL).fill(null);
    let current = this.head;

    // Traverse from the highest level down to level 0
    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];

    if (current !== null && current.key === key) {
      // Key already exists — update value and adjust byte size
      this.byteSize -= Buffer.byteLength(current.value, 'utf8');
      current.value = value;
      this.byteSize += Buffer.byteLength(value, 'utf8');
      return;
    }

    // Insert new node
    const newLevel = this._randomLevel();

    if (newLevel > this.level) {
      for (let i = this.level + 1; i <= newLevel; i++) {
        update[i] = this.head;
      }
      this.level = newLevel;
    }

    const newNode = new SkipListNode(key, value, newLevel + 1);

    for (let i = 0; i <= newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }

    this.count++;
    this.byteSize +=
      Buffer.byteLength(key, 'utf8') + Buffer.byteLength(value, 'utf8');
  }

  /**
   * Look up a key.
   * @param {string} key
   * @returns {string|null} The value, TOMBSTONE, or null if not found.
   */
  get(key) {
    let current = this.head;

    for (let i = this.level; i >= 0; i--) {
      while (current.forward[i] !== null && current.forward[i].key < key) {
        current = current.forward[i];
      }
    }

    current = current.forward[0];

    if (current !== null && current.key === key) {
      return current.value;
    }
    return null;
  }

  /**
   * Mark a key as deleted by inserting a tombstone.
   * @param {string} key
   */
  delete(key) {
    this.put(key, TOMBSTONE);
  }

  /**
   * Return all entries sorted by key.
   * @returns {{ key: string, value: string }[]}
   */
  entries() {
    const result = [];
    let current = this.head.forward[0];

    while (current !== null) {
      result.push({ key: current.key, value: current.value });
      current = current.forward[0];
    }

    return result;
  }

  /**
   * Range scan — returns entries where startKey <= key <= endKey.
   * @param {string} startKey
   * @param {string} endKey
   * @returns {{ key: string, value: string }[]}
   */
  scan(startKey, endKey) {
    const result = [];
    let current = this.head;

    // Navigate to the first node >= startKey
    for (let i = this.level; i >= 0; i--) {
      while (
        current.forward[i] !== null &&
        current.forward[i].key < startKey
      ) {
        current = current.forward[i];
      }
    }

    current = current.forward[0];

    while (current !== null && current.key <= endKey) {
      result.push({ key: current.key, value: current.value });
      current = current.forward[0];
    }

    return result;
  }

  /** Reset the skip list to empty. */
  clear() {
    this.head = new SkipListNode(null, null, MAX_LEVEL);
    this.level = 0;
    this.count = 0;
    this.byteSize = 0;
  }
}

/* ------------------------------------------------------------------ */
/*  MemTable (public facade)                                          */
/* ------------------------------------------------------------------ */

export default class MemTable {
  constructor() {
    this._skipList = new SkipList();
  }

  /**
   * @param {string} key
   * @param {string} value
   */
  put(key, value) {
    this._skipList.put(key, value);
  }

  /**
   * @param {string} key
   * @returns {string|null}
   */
  get(key) {
    return this._skipList.get(key);
  }

  /**
   * @param {string} key
   */
  delete(key) {
    this._skipList.delete(key);
  }

  /**
   * @returns {{ key: string, value: string }[]}
   */
  entries() {
    return this._skipList.entries();
  }

  /**
   * @param {string} startKey
   * @param {string} endKey
   */
  scan(startKey, endKey) {
    return this._skipList.scan(startKey, endKey);
  }

  /**
   * Approximate byte size of data held in memory.
   */
  get size() {
    return this._skipList.byteSize;
  }

  /**
   * Number of entries (including tombstones).
   */
  get count() {
    return this._skipList.count;
  }

  /**
   * Discard all entries.
   */
  clear() {
    this._skipList.clear();
  }
}
