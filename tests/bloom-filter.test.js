/**
 * Tests — Bloom Filter
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import BloomFilter from '../src/bloom-filter.js';

describe('BloomFilter', () => {
  it('should report added keys as possibly present', () => {
    const bf = new BloomFilter();
    bf.add('hello');
    bf.add('world');
    assert.equal(bf.mightContain('hello'), true);
    assert.equal(bf.mightContain('world'), true);
  });

  it('should have no false negatives', () => {
    const bf = new BloomFilter(4096, 7);
    const keys = [];
    for (let i = 0; i < 200; i++) {
      const key = `key_${i}`;
      keys.push(key);
      bf.add(key);
    }
    // Every added key must be reported as possibly present
    for (const key of keys) {
      assert.equal(bf.mightContain(key), true, `false negative for ${key}`);
    }
  });

  it('should have a low false-positive rate', () => {
    const bf = new BloomFilter(4096, 7);
    for (let i = 0; i < 100; i++) {
      bf.add(`added_${i}`);
    }
    let fp = 0;
    const checks = 1000;
    for (let i = 0; i < checks; i++) {
      if (bf.mightContain(`not_added_${i}`)) fp++;
    }
    const rate = fp / checks;
    assert.ok(rate < 0.15, `False positive rate too high: ${(rate * 100).toFixed(1)}%`);
  });

  it('should serialize and deserialize correctly', () => {
    const bf = new BloomFilter(2048, 5);
    bf.add('alpha');
    bf.add('beta');
    bf.add('gamma');

    const buf = bf.serialize();
    const bf2 = BloomFilter.deserialize(buf);

    assert.equal(bf2.size, 2048);
    assert.equal(bf2.hashCount, 5);
    assert.equal(bf2.mightContain('alpha'), true);
    assert.equal(bf2.mightContain('beta'), true);
    assert.equal(bf2.mightContain('gamma'), true);
  });
});
