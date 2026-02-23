# LSM-Tree Storage Engine

A fully functional **Log-Structured Merge-Tree** key-value storage engine built in Node.js

```
                      ┌──────────────────────────────────┐
                      │           Client API             │
                      │   put · get · delete · scan      │
                      └───────────────┬──────────────────┘
                                      │
                ┌─────────────────────┼─────────────────────┐
                │                     ▼                     │
                │  ┌──────────┐  ┌──────────────────┐       │
    Write Path  │  │   WAL    │─▶│    MemTable      │       │  Read Path
                │  │ (append) │  │   (Skip List)    │◀──────┼──── ①
                │  └──────────┘  └────────┬─────────┘       │
                │                         │ flush           │
                │                         ▼                 │
                │              ┌─────────────────────┐      │
                │              │  Level 0 SSTables   │◀─────┼──── ②
                │              │  ┌─────┐ ┌─────┐    │      │
                │              │  │ SST │ │ SST │    │      │
                │              │  └─────┘ └─────┘    │      │
                │              └────────┬────────────┘      │
                │                       │ compact           │
                │                       ▼                   │
                │              ┌─────────────────────┐      │
                │              │  Level 1 SSTables   │◀─────┼──── ③
                │              │  ┌───────────────┐  │      │
                │              │  │  Merged SST   │  │      │
                │              │  └───────────────┘  │      │
                │              └────────┬────────────┘      │
                │                       │ compact           │
                │                       ▼                   │
                │              ┌─────────────────────┐      │
                │              │  Level N SSTables   │◀─────┼──── ④
                │              └─────────────────────┘      │
                └───────────────────────────────────────────┘

    Write: Client ──▶ WAL ──▶ MemTable ──(flush)──▶ SSTable L0
    Read:  Client ──▶ MemTable ──▶ Bloom Filter ──▶ SSTables (newest → oldest)
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Skip List MemTable** | O(log n) in-memory sorted writes and reads |
| **Write-Ahead Log** | Crash recovery with synchronous append |
| **SSTables** | Immutable binary on-disk files with sparse index |
| **Bloom Filter** | Fast key-miss detection (~1% false positive rate) |
| **Size-Tiered Compaction** | Automatic multi-level K-way merge |
| **Range Scans** | Efficient sorted iteration over key ranges |
| **CLI REPL** | Interactive terminal interface |
| **Web Dashboard** | Real-time stats, query console, and level visualiser |

---

## Project Structure

```
lsm-tree-storage/
├── lsm.config.js               # Central configuration constants
├── cli.js                      # Interactive CLI / REPL
├── src/
│   ├── memtable.js             # MemTable (Skip List)
│   ├── wal.js                  # Write-Ahead Log
│   ├── sstable.js              # SSTable Writer + Reader
│   ├── bloom-filter.js         # Bloom Filter
│   ├── compaction.js           # Size-Tiered Compaction
│   └── lsm-tree.js             # Main LSMTree orchestrator
├── ui/
│   ├── server.js               # Express REST API server
│   └── public/
│       ├── index.html          # Dashboard SPA
│       ├── styles.css          # Dark theme design system
│       └── app.js              # Client-side logic
├── tests/
│   ├── memtable.test.js
│   ├── wal.test.js
│   ├── sstable.test.js
│   ├── bloom-filter.test.js
│   ├── compaction.test.js
│   └── lsm-tree.test.js
└── bench/
    └── benchmark.js            # Performance benchmark
```

---

## Getting Started

### Prerequisites

- **Node.js v22+** (uses built-in `node:test` runner)
- **nvm** (recommended): `nvm use 22`

### Install

```bash
git clone <repo-url> && cd lsm-tree-storage
npm install
```

### Run the CLI REPL

```bash
npm start
```

```
╔══════════════════════════════════════╗
║   LSM-Tree Storage Engine v1.0       ║
║   Type 'help' for commands           ║
╚══════════════════════════════════════╝

lsm> put name Alice
  OK
lsm> get name
  Alice
lsm> put age 30
  OK
lsm> scan a z
  age = 30
  name = Alice
  (2 result(s))
lsm> flush
  Flushed MemTable to Level 0 SSTable
lsm> stats

Engine Statistics
  MemTable : 0 entries (0 B)
  SSTables : 1 total (233 B)
    Level 0 : 1 SSTable(s) (233 B)

lsm> delete name
  OK
lsm> get name
  (not found)
lsm> exit
  Goodbye!
```

### Run the Web Dashboard

```bash
npm run ui
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

---

## Running Tests

```bash
npm test
```

This runs all 6 test suites (40 tests total) using the built-in `node:test` runner:

```
▶ BloomFilter     — 4 tests ✔
▶ Compaction      — 7 tests ✔
▶ LSMTree         — 11 tests ✔
▶ MemTable        — 10 tests ✔
▶ SSTable         — 4 tests ✔
▶ WAL             — 4 tests ✔
────────────────────────────────
  40 pass, 0 fail (159ms)
```

You can also run individual test files:

```bash
# Run a single test suite
node --test tests/memtable.test.js
node --test tests/bloom-filter.test.js
node --test tests/sstable.test.js
node --test tests/wal.test.js
node --test tests/compaction.test.js
node --test tests/lsm-tree.test.js
```

---

## Benchmark

```bash
npm run bench
```

```
╔══════════════════════════════════════════╗
║      LSM-Tree Storage Engine Benchmark   ║
╚══════════════════════════════════════════╝

▶ Sequential Writes: 10,000 ops
  ✓ 338ms — 29,564 ops/sec
▶ Random Reads: 10,000 ops
  ✓ 68ms — 146,896 ops/sec
▶ Range Scans: 1,000 ops (50-key windows)
  ✓ 799ms — 1,252 ops/sec
```

---

## Configuration

All tunable parameters are in [`lsm.config.js`](lsm.config.js):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MEMTABLE_SIZE_THRESHOLD` | 64 KB | Flush MemTable when it exceeds this size |
| `BLOOM_FILTER_SIZE` | 1,024 bits | Bit array size for Bloom filters |
| `BLOOM_HASH_COUNT` | 7 | Number of hash functions |
| `MAX_LEVELS` | 5 | Maximum compaction levels |
| `SIZE_RATIO` | 4 | SSTables per level before compaction |
| `SPARSE_INDEX_INTERVAL` | 16 | Sparse index entry every N keys |

---

## REST API

When running the web dashboard (`npm run ui`), the following endpoints are available:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/put` | Insert key-value `{ "key": "...", "value": "..." }` |
| `GET` | `/api/get/:key` | Look up a key |
| `DELETE` | `/api/delete/:key` | Delete a key |
| `GET` | `/api/scan?start=...&end=...` | Range scan |
| `POST` | `/api/flush` | Force flush MemTable to SSTable |
| `GET` | `/api/stats` | Engine statistics |

---

## How It Works

### Write Path
1. **WAL Append** — The operation is first written to the Write-Ahead Log (crash-safe)
2. **MemTable Insert** — The key-value pair is inserted into the in-memory skip list
3. **Auto-Flush** — When the MemTable exceeds 64 KB, it is flushed to a new Level 0 SSTable
4. **Compaction** — When a level accumulates ≥ 4 SSTables, they are merged into the next level

### Read Path
1. **MemTable Check** — The in-memory skip list is checked first (newest data)
2. **Bloom Filter** — Each SSTable's bloom filter is checked to skip tables that definitely don't contain the key
3. **Sparse Index** — The SSTable's sparse index is used to narrow down the disk region to scan
4. **Data Lookup** — A linear scan of the narrowed region finds the exact key

### SSTable Binary Format

```
┌─────────────────────────────────────────────┐
│ Header: magic(4B) │ version(1B) │ count(4B) │
├─────────────────────────────────────────────┤
│ Data Block: repeated                        │
│   keyLen(2B) │ key │ valueLen(4B) │ value   │
├─────────────────────────────────────────────┤
│ Index Block: repeated (every 16th key)      │
│   keyLen(2B) │ key │ offset(4B)             │
├─────────────────────────────────────────────┤
│ Bloom Filter Block: serialized filter       │
├─────────────────────────────────────────────┤
│ Footer:                                     │
│   dataOff(4B) │ idxOff(4B) │ bloomOff(4B)   │
│   magic(4B)                                 │
└─────────────────────────────────────────────┘
```
