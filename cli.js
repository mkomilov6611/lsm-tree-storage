#!/usr/bin/env node

/**
 * LSM-Tree Storage Engine — Interactive CLI / REPL
 *
 * Commands:
 *   put <key> <value>      Insert or update a key-value pair
 *   get <key>              Retrieve the value for a key
 *   delete <key>           Delete a key
 *   scan <start> <end>     Range scan (inclusive)
 *   flush                  Force-flush MemTable to SSTable
 *   stats                  Show engine statistics
 *   help                   Show this help message
 *   exit / quit            Exit the REPL
 */

import { createInterface } from 'node:readline';
import LSMTree from './src/lsm-tree.js';

const db = new LSMTree();

const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: '\x1b[36mlsm>\x1b[0m ',
});

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function printHelp() {
  console.log(`
\x1b[1m LSM-Tree Storage Engine — Commands\x1b[0m
  \x1b[33mput\x1b[0m <key> <value>      Insert or update a key-value pair
  \x1b[33mget\x1b[0m <key>              Retrieve the value for a key
  \x1b[33mdelete\x1b[0m <key>           Delete a key
  \x1b[33mscan\x1b[0m <start> <end>     Range scan (inclusive)
  \x1b[33mflush\x1b[0m                  Force-flush MemTable to SSTable
  \x1b[33mstats\x1b[0m                  Show engine statistics
  \x1b[33mhelp\x1b[0m                   Show this help message
  \x1b[33mexit\x1b[0m / \x1b[33mquit\x1b[0m            Exit the REPL
`);
}

function printStats() {
  const s = db.stats();
  console.log(`\n\x1b[1m Engine Statistics\x1b[0m`);
  console.log(`  MemTable : ${s.memTable.entries} entries (${formatBytes(s.memTable.sizeBytes)})`);
  console.log(`  SSTables : ${s.totalSSTables} total (${formatBytes(s.totalSizeBytes)})`);

  for (const lvl of s.levels) {
    if (lvl.sstables > 0) {
      console.log(`    Level ${lvl.level} : ${lvl.sstables} SSTable(s) (${formatBytes(lvl.sizeBytes)})`);
    }
  }
  console.log();
}

console.log(`
\x1b[1m\x1b[35m╔══════════════════════════════════════╗
║      LSM-Tree Storage Engine v1.0    ║
║      Type 'help' for commands        ║
╚══════════════════════════════════════╝\x1b[0m
`);

rl.prompt();

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (!trimmed) {
    rl.prompt();
    return;
  }

  const parts = trimmed.split(/\s+/);
  const cmd = parts[0].toLowerCase();

  try {
    switch (cmd) {
      case 'put': {
        if (parts.length < 3) {
          console.log('\x1b[31m  Usage: put <key> <value>\x1b[0m');
          break;
        }
        const key = parts[1];
        const value = parts.slice(2).join(' ');
        db.put(key, value);
        console.log(`  \x1b[32mOK\x1b[0m`);
        break;
      }

      case 'get': {
        if (parts.length < 2) {
          console.log('\x1b[31m  Usage: get <key>\x1b[0m');
          break;
        }
        const val = db.get(parts[1]);
        if (val === null) {
          console.log(`  \x1b[90m(not found)\x1b[0m`);
        } else {
          console.log(`  \x1b[32m${val}\x1b[0m`);
        }
        break;
      }

      case 'delete': {
        if (parts.length < 2) {
          console.log('\x1b[31m  Usage: delete <key>\x1b[0m');
          break;
        }
        db.delete(parts[1]);
        console.log(`  \x1b[32mOK\x1b[0m`);
        break;
      }

      case 'scan': {
        if (parts.length < 3) {
          console.log('\x1b[31m  Usage: scan <startKey> <endKey>\x1b[0m');
          break;
        }
        const results = db.scan(parts[1], parts[2]);
        if (results.length === 0) {
          console.log(`  \x1b[90m(no results)\x1b[0m`);
        } else {
          for (const { key, value } of results) {
            console.log(`  \x1b[33m${key}\x1b[0m = \x1b[32m${value}\x1b[0m`);
          }
          console.log(`  \x1b[90m(${results.length} result(s))\x1b[0m`);
        }
        break;
      }

      case 'flush': {
        db.flush();
        console.log('  \x1b[32mFlushed MemTable to Level 0 SSTable\x1b[0m');
        break;
      }

      case 'stats': {
        printStats();
        break;
      }

      case 'help': {
        printHelp();
        break;
      }

      case 'exit':
      case 'quit': {
        db.close();
        console.log('  \x1b[90mGoodbye!\x1b[0m');
        process.exit(0);
      }

      default:
        console.log(`  \x1b[31mUnknown command: ${cmd}. Type 'help' for usage.\x1b[0m`);
    }
  } catch (err) {
    console.error(`  \x1b[31mError: ${err.message}\x1b[0m`);
  }

  rl.prompt();
});

rl.on('close', () => {
  db.close();
  process.exit(0);
});
