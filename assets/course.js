/* Modern Data Engineering · course extension of the SDAIA Academy slide system (loaded after deck.js).
   The Qafila platform map: <div class="pmap" data-here="cdc,dagster"></div> marks tool / band keys as "you are here".
   The same data and packing as de_viz.platform_map (labs/de_viz.py): change both together. */
(function () {
  const STAGES = ['sources', 'ingest', 'store', 'transform', 'test', 'serve'];
  const SOURCES = [['App database', 'qafila_oltp.sqlite'], ['Payments API', 'pages, cursor, 429s'],
    ['CDC feed', 'inserts, updates, deletes'], ['Click events', 'a stream'], ['Supplier files', 'weekly CSVs'],
    ['Reviews', 'free text']];
  const TOOLS = [
    ['dlt', 'dlt', 'ingest', 'ingest', 'incremental loads, schema drift', 'D1_3'],
    ['duckdb', 'DuckDB', 'store', 'serve', 'the SQL engine in every lab', 'D1_1'],
    ['cdc', 'CDC + MERGE', 'ingest', 'ingest', 'changes, deletes, SCD2', 'D1_4'],
    ['parquet', 'Parquet', 'store', 'store', 'columnar files', 'D1_2'],
    ['polars', 'Polars', 'transform', 'test', 'DataFrames, lazy plans, checks', 'D2_1'],
    ['semantic', 'Semantic layer', 'serve', 'serve', 'metrics defined once', 'D4_4'],
    ['streaming', 'Streaming', 'ingest', 'ingest', 'events as they happen', 'D3_3 · D3_4'],
    ['iceberg', 'Iceberg', 'store', 'store', 'tables on files, time travel', 'D3_1'],
    ['dbt', 'dbt', 'transform', 'test', 'models, tests, docs, lineage', 'D2_2 · D2_3'],
    ['ai', 'Embeddings', 'serve', 'serve', 'search, text-to-SQL', 'D4_4'],
    ['spark', 'Spark', 'transform', 'transform', 'the same work on a cluster', 'D3_2'],
    ['pandera', 'Pandera', 'test', 'test', 'schemas for DataFrames', 'D2_4'],
    ['contracts', 'Contracts', 'test', 'test', 'ODCS: what consumers rely on', 'D4_2'],
  ];
  const BANDS = [['dagster', 'Orchestrate', 'Dagster: assets, schedules, checks', 'D4_1'],
    ['observe', 'Observe', 'freshness, volume, schema, distribution, lineage', 'D4_2'],
    ['govern', 'Govern', 'PDPL, masking, access', 'D4_3']];
  document.querySelectorAll('.pmap').forEach((el) => {
    const here = (el.dataset.here || '').split(',').map((k) => k.trim()).filter(Boolean);
    const met = (el.dataset.met || '').split(',').map((k) => k.trim()).filter(Boolean);  // optional: already covered, shown plain
    const marked = new Set();
    TOOLS.forEach(([k, , a, b]) => { if (here.includes(k)) STAGES.slice(STAGES.indexOf(a), STAGES.indexOf(b) + 1).forEach((s) => marked.add(s)); });
    const focus = here.length > 0;
    const rows = [], cells = [];
    TOOLS.forEach(([k, name, a, b, what, sessions]) => {
      const c0 = STAGES.indexOf(a) + 1, c1 = STAGES.indexOf(b) + 1, span = [];
      for (let c = c0; c <= c1; c++) span.push(c);
      let r = rows.findIndex((used) => span.every((c) => !used.has(c)));
      if (r < 0) { rows.push(new Set()); r = rows.length - 1; }
      span.forEach((c) => rows[r].add(c));
      const covers = STAGES.slice(c0 - 1, c1).some((s) => marked.has(s));
      const cls = here.includes(k) ? 'on' : (focus && !covers && !met.includes(k) ? 'dim' : '');
      cells.push(`<div class="pm-t ${cls}${c0 === c1 ? ' narrow' : ''}" style="grid-column:${c0}/${c1 + 1};grid-row:${r + 2}"><i>${sessions}</i><b>${name}</b><small>${what}</small></div>`);
    });
    const n = Math.max(rows.length, SOURCES.length);
    const srcDim = focus && !marked.has('sources') && !marked.has('ingest') ? ' dim' : '';
    SOURCES.forEach(([name, what], i) => cells.push(`<div class="pm-s narrow${srcDim}" style="grid-column:1;grid-row:${i + 2}"><b>${name}</b><small>${what}</small></div>`));
    BANDS.forEach(([k, name, what, sessions], i) => cells.push(`<div class="pm-b ${here.includes(k) ? 'on' : (focus && !met.includes(k) ? 'dim' : '')}" style="grid-column:2/-1;grid-row:${n + 2 + i}"><i>${sessions}</i><b>${name}</b><small>${what}</small></div>`));
    const heads = STAGES.map((s, i) => `<div class="pm-h${marked.has(s) ? ' on' : ''}" style="grid-column:${i + 1};grid-row:1">${s}</div>`).join('');
    const cols = STAGES.map((s, i) => (marked.has(s) ? `<div class="pm-col" style="grid-column:${i + 1};grid-row:1/${n + 2}"></div>` : '')).join('');
    el.innerHTML = cols + heads + cells.join('');
  });
  if (window.deck) window.deck.checkSelectors.push('.pmap > div', '.rec', '.evt');
})();
