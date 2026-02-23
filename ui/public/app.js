/**
 * LSM-Tree Dashboard — Client-Side Application
 *
 * Handles API calls, DOM updates, auto-refresh, and the animated
 * LSM-tree level visualiser.
 */

/* ================================================================== */
/*  Helpers                                                            */
/* ================================================================== */

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

async function api(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(path, opts);
  return res.json();
}

/* ================================================================== */
/*  Stats refresh                                                      */
/* ================================================================== */

const MEMTABLE_THRESHOLD = 64 * 1024; // matches config

async function refreshStats() {
  const stats = await api('GET', '/api/stats');

  // ---- Stat cards ----
  document.getElementById('stat-memtable-entries').textContent =
    `${stats.memTable.entries} entries`;
  document.getElementById('stat-memtable-size').textContent =
    formatBytes(stats.memTable.sizeBytes);

  document.getElementById('stat-sstable-count').textContent =
    stats.totalSSTables;
  document.getElementById('stat-sstable-size').textContent =
    formatBytes(stats.totalSizeBytes);

  const activeLevels = stats.levels.filter(l => l.sstables > 0).length;
  document.getElementById('stat-active-levels').textContent = activeLevels;

  const totalSize = stats.memTable.sizeBytes + stats.totalSizeBytes;
  document.getElementById('stat-total-size').textContent =
    formatBytes(totalSize);

  // MemTable fill bar
  const fillPct = Math.min(
    (stats.memTable.sizeBytes / MEMTABLE_THRESHOLD) * 100,
    100
  );
  document.getElementById('memtable-bar').style.width = `${fillPct}%`;

  // ---- Visualiser ----
  const memBar = document.querySelector('.memtable-bar-vis');
  memBar.style.setProperty('--fill', `${fillPct}%`);

  const visLevels = document.getElementById('vis-levels');
  visLevels.innerHTML = '';

  const levelColors = [
    'linear-gradient(135deg, #06b6d4, #3b82f6)',
    'linear-gradient(135deg, #8b5cf6, #6366f1)',
    'linear-gradient(135deg, #f59e0b, #ef4444)',
    'linear-gradient(135deg, #10b981, #06b6d4)',
    'linear-gradient(135deg, #ec4899, #8b5cf6)',
  ];

  for (const lvl of stats.levels) {
    const div = document.createElement('div');
    div.className = 'vis-level';

    const nameSpan = document.createElement('span');
    nameSpan.className = 'vis-level-name';
    nameSpan.textContent = `Level ${lvl.level}`;
    div.appendChild(nameSpan);

    const blocksDiv = document.createElement('div');
    blocksDiv.className = 'vis-blocks';

    if (lvl.sstables === 0) {
      const empty = document.createElement('span');
      empty.className = 'vis-empty';
      empty.textContent = 'empty';
      blocksDiv.appendChild(empty);
    } else {
      for (let i = 0; i < lvl.sstables; i++) {
        const block = document.createElement('div');
        block.className = 'vis-block';
        block.style.background = levelColors[lvl.level % levelColors.length];
        block.style.width = `${Math.max(40, Math.min(120, lvl.sizeBytes / lvl.sstables / 500))}px`;
        block.style.animationDelay = `${i * 0.05}s`;
        block.textContent = `SST`;
        blocksDiv.appendChild(block);
      }
    }

    div.appendChild(blocksDiv);

    const sizeSpan = document.createElement('span');
    sizeSpan.className = 'vis-empty';
    sizeSpan.textContent = lvl.sstables > 0 ? formatBytes(lvl.sizeBytes) : '';
    div.appendChild(sizeSpan);

    visLevels.appendChild(div);

    // Arrow between levels (except the last one)
    if (lvl.level < stats.levels.length - 1) {
      const arrow = document.createElement('div');
      arrow.className = 'tree-arrow';
      arrow.textContent = '▼';
      visLevels.appendChild(arrow);
    }
  }
}

/* ================================================================== */
/*  Console output                                                     */
/* ================================================================== */

const output = document.getElementById('console-output');

function clearOutput() {
  output.innerHTML = '';
}

function addLine(html, cls = '') {
  // Remove placeholder if present
  const placeholder = output.querySelector('.output-placeholder');
  if (placeholder) placeholder.remove();

  const div = document.createElement('div');
  div.className = `output-line ${cls}`;
  div.innerHTML = html;
  output.appendChild(div);
  output.scrollTop = output.scrollHeight;
}

function timestamp() {
  return new Date().toLocaleTimeString();
}

/* ================================================================== */
/*  Tab switching                                                      */
/* ================================================================== */

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(`panel-${tab.dataset.tab}`).classList.add('active');
  });
});

/* ================================================================== */
/*  Button handlers                                                    */
/* ================================================================== */

// PUT
document.getElementById('btn-put').addEventListener('click', async () => {
  const key = document.getElementById('put-key').value.trim();
  const value = document.getElementById('put-value').value.trim();
  if (!key) return;

  await api('POST', '/api/put', { key, value });
  addLine(
    `<span class="meta">[${timestamp()}]</span> PUT <span class="key">${key}</span> = <span class="value">${value}</span>`,
    'success'
  );
  document.getElementById('put-key').value = '';
  document.getElementById('put-value').value = '';
  refreshStats();
});

// GET
document.getElementById('btn-get').addEventListener('click', async () => {
  const key = document.getElementById('get-key').value.trim();
  if (!key) return;

  const data = await api('GET', `/api/get/${encodeURIComponent(key)}`);
  if (data.value === null) {
    addLine(
      `<span class="meta">[${timestamp()}]</span> GET <span class="key">${key}</span> → <span class="null-val">(not found)</span>`
    );
  } else {
    addLine(
      `<span class="meta">[${timestamp()}]</span> GET <span class="key">${key}</span> → <span class="value">${data.value}</span>`,
      'success'
    );
  }
  document.getElementById('get-key').value = '';
});

// DELETE
document.getElementById('btn-delete').addEventListener('click', async () => {
  const key = document.getElementById('delete-key').value.trim();
  if (!key) return;

  await api('DELETE', `/api/delete/${encodeURIComponent(key)}`);
  addLine(
    `<span class="meta">[${timestamp()}]</span> DELETE <span class="key">${key}</span>`,
    'success'
  );
  document.getElementById('delete-key').value = '';
  refreshStats();
});

// SCAN
document.getElementById('btn-scan').addEventListener('click', async () => {
  const start = document.getElementById('scan-start').value.trim();
  const end = document.getElementById('scan-end').value.trim();
  if (!start || !end) return;

  const data = await api('GET', `/api/scan?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`);
  addLine(
    `<span class="meta">[${timestamp()}]</span> SCAN <span class="key">${start}</span> → <span class="key">${end}</span>  (${data.count} result${data.count !== 1 ? 's' : ''})`
  );
  for (const { key, value } of data.results) {
    addLine(`  <span class="key">${key}</span> = <span class="value">${value}</span>`);
  }
  if (data.count === 0) {
    addLine('  <span class="null-val">(no results)</span>');
  }
});

// FLUSH
document.getElementById('btn-flush').addEventListener('click', async () => {
  await api('POST', '/api/flush');
  addLine(
    `<span class="meta">[${timestamp()}]</span> <span class="value">⚡ MemTable flushed to Level 0 SSTable</span>`,
    'success'
  );
  refreshAll();
});

// GENERATE DATA
document.getElementById('btn-generate').addEventListener('click', async () => {
  const data = await api('POST', '/api/generate?count=200');
  addLine(
    `<span class="meta">[${timestamp()}]</span> <span class="value">Generated ${data.generated} users (${data.entries} entries)</span>`,
    'success'
  );
  refreshAll();
});

// REFRESH (stats + inspector)
async function refreshInspector() {
  const data = await api('GET', '/api/inspect?limit=200');

  // ---- MemTable ----
  const memEmpty = document.getElementById('memtable-empty');
  const memWrap = document.getElementById('memtable-table-wrap');
  const memMeta = document.getElementById('memtable-meta');

  if (data.memTable.length === 0) {
    memEmpty.innerHTML = 'MemTable is <strong>empty</strong> — all data has been flushed to SSTables';
    memEmpty.style.display = 'block';
    memWrap.style.display = 'none';
  } else {
    memEmpty.style.display = 'none';
    memWrap.style.display = 'block';

    const tombstones = data.memTable.filter(e => e.value === TOMBSTONE).length;
    memMeta.innerHTML = `
      <span class="insp-badge">${data.memTable.length} entries</span>
      <span class="insp-badge">Skip List (sorted)</span>
      ${tombstones > 0 ? `<span class="insp-badge">🪦 ${tombstones} tombstone${tombstones > 1 ? 's' : ''}</span>` : ''}
    `;

    buildEntryTable(data.memTable, 'memtable-data');
  }

  // ---- SSTables ----
  const sstEmpty = document.getElementById('sstables-empty');
  const sstList = document.getElementById('sstables-list');

  if (data.sstables.length === 0) {
    sstEmpty.innerHTML = 'No SSTables on disk — data is still in the MemTable';
    sstEmpty.style.display = 'block';
    sstList.innerHTML = '';
  } else {
    sstEmpty.style.display = 'none';

    // Save which cards are currently open
    const openCards = new Set();
    sstList.querySelectorAll('.sst-body.open').forEach(body => {
      const idx = body.id.replace('sst-body-', '');
      openCards.add(idx);
    });

    sstList.innerHTML = '';

    data.sstables.forEach((sst, idx) => {
      const card = document.createElement('div');
      card.className = 'sst-card';

      const levelColors = ['#06b6d4', '#8b5cf6', '#f59e0b', '#10b981', '#ec4899'];
      const color = levelColors[sst.level % levelColors.length];
      const isOpen = openCards.has(String(idx));

      card.innerHTML = `
        <div class="sst-header" data-sst-idx="${idx}">
          <div class="sst-title">
            <span style="color:${color}">●</span>
            ${sst.filename}
            <span class="insp-badge">Level ${sst.level}</span>
            <span class="insp-badge">${sst.entryCount} entries</span>
            <span class="insp-badge">${formatBytes(sst.sizeBytes)}</span>
          </div>
          <span class="sst-toggle${isOpen ? ' open' : ''}" id="sst-toggle-${idx}">▶</span>
        </div>
        <div class="sst-body${isOpen ? ' open' : ''}" id="sst-body-${idx}">
          <div class="insp-meta">
            <span class="insp-badge">Sparse Index: ${sst.sparseIndexSize} entries</span>
            <span class="insp-badge">Bloom Filter: ${sst.bloomFilterBits} bits</span>
            <span class="insp-badge">Binary format v1</span>
          </div>
          <div class="insp-table-wrap" style="max-height:300px">
            <table class="insp-table" id="sst-table-${idx}">
              <thead><tr><th>#</th><th>Key</th><th>Value</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      `;

      sstList.appendChild(card);

      // Build entry table
      const tbody = card.querySelector(`#sst-table-${idx} tbody`);
      sst.entries.forEach((entry, i) => {
        const tr = document.createElement('tr');
        const isTombstone = entry.value === TOMBSTONE;
        tr.innerHTML = `
          <td>${i + 1}</td>
          <td class="key-cell">${escapeHtml(entry.key)}</td>
          <td class="${isTombstone ? 'tombstone' : 'val-cell'}">${
            isTombstone ? '🪦 TOMBSTONE' : escapeHtml(entry.value)
          }</td>
        `;
        tbody.appendChild(tr);
      });

      if (sst.entryCount > sst.entries.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="3" style="text-align:center;color:var(--text-muted);font-style:italic">… ${sst.entryCount - sst.entries.length} more entries</td>`;
        tbody.appendChild(tr);
      }

      // Toggle handler
      card.querySelector('.sst-header').addEventListener('click', () => {
        const body = document.getElementById(`sst-body-${idx}`);
        const toggle = document.getElementById(`sst-toggle-${idx}`);
        body.classList.toggle('open');
        toggle.classList.toggle('open');
      });
    });
  }
}

async function refreshAll() {
  await Promise.all([refreshStats(), refreshInspector()]);
}

/* ================================================================== */
/*  Keyboard shortcuts (Enter to submit in active panel)               */
/* ================================================================== */

document.querySelectorAll('.form-group input').forEach(input => {
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      const panel = input.closest('.tab-panel');
      const btn = panel.querySelector('.btn');
      if (btn) btn.click();
    }
  });
});

/* ================================================================== */
/*  Inspector                                                          */
/* ================================================================== */

const TOMBSTONE = '__TOMBSTONE__';

// Tab switching for inspector
document.querySelectorAll('.insp-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.insp-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.insp-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(`insp-${tab.dataset.insp}`).classList.add('active');
  });
});

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function buildEntryTable(entries, tableId) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  tbody.innerHTML = '';

  entries.forEach((entry, idx) => {
    const tr = document.createElement('tr');
    const isTombstone = entry.value === TOMBSTONE;

    tr.innerHTML = `
      <td>${idx + 1}</td>
      <td class="key-cell">${escapeHtml(entry.key)}</td>
      <td class="${isTombstone ? 'tombstone' : 'val-cell'}">${
        isTombstone ? '🪦 TOMBSTONE' : escapeHtml(entry.value)
      }</td>
    `;
    tbody.appendChild(tr);
  });
}



/* ================================================================== */
/*  Refresh button (manual)                                            */
/* ================================================================== */

document.getElementById('btn-refresh').addEventListener('click', () => refreshAll());

// Initial load
refreshAll();
