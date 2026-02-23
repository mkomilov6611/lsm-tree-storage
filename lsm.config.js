/**
 * LSM-Tree Storage Engine — Configuration
 */

import { join } from 'node:path';

/** Directory where all persistent data (WAL, SSTables) is stored */
export const DATA_DIR = join(process.cwd(), 'data');

/** Flush the MemTable to an SSTable when it exceeds this byte size */
export const MEMTABLE_SIZE_THRESHOLD = 64 * 1024; // 64 KB

/** Number of bits in the Bloom filter bit-array */
export const BLOOM_FILTER_SIZE = 1024;

/** Number of hash functions used by the Bloom filter */
export const BLOOM_HASH_COUNT = 7;

/** Maximum number of compaction levels */
export const MAX_LEVELS = 5;

/** When a level reaches this many SSTables, compact into the next level */
export const SIZE_RATIO = 4;

/** Build a sparse index entry every N keys inside an SSTable */
export const SPARSE_INDEX_INTERVAL = 16;

/** Tombstone marker for deleted keys */
export const TOMBSTONE = '__TOMBSTONE__';

/** SSTable file magic bytes (ASCII "LSMT") */
export const SSTABLE_MAGIC = 0x4c534d54;

/** SSTable format version */
export const SSTABLE_VERSION = 1;
