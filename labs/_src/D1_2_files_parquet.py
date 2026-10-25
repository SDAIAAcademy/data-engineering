# %% [markdown]
# # Lab 1.2 · Parquet in practice
#
# Day 1 · Session 2 (Storage for analytics, part 2: columns in a file) · 25 min. What you'll do:
# - store the same few million Qafila order lines as CSV and as Parquet (Snappy, ZSTD) and measure size and speed;
# - open a Parquet footer and read its row groups, statistics and encodings;
# - see why **sorting** and **partitioning** let an engine skip most of a file, and how over-partitioning backfires.
#
# **Tasks**
# - [ ] 1 · CSV vs. Parquet (Snappy, ZSTD): size and scan time (6')
# - [ ] 2 · Read the footer: row groups, statistics, encodings (5')
# - [ ] 3 · Sorted vs. unsorted: count row groups skipped (5')
# - [ ] 4 · Partition by date; query one day (5')
# - [ ] 5 · Over-partition on purpose; measure the cost (4')
# - [ ] ✱ Stretch · Prove the partition caveat in the plan

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements-colab.txt"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # solutions/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import duckdb
from IPython.display import display

con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)
OUT = U.lab_dir("D1_2", reset=True)      # artifacts/D1_2: everything this lab writes
O = OUT.as_posix()

# %% [markdown]
# Qafila has ~173k order lines. Files behave differently at scale, so we simulate a bigger Qafila by replicating the lines
# `SCALE` times (with new order IDs) and **shuffling** them, as data often arrives in no useful order.
# One wide row per order line, the way analysts like it:

# %%
SCALE = U.pick(20, 3)
con.sql(f"""
    CREATE TABLE lines AS
    WITH base AS (
        SELECT o.order_id, i.line_no, o.created_at::TIMESTAMP AS created_at, o.store_id, s.city, o.status,
               o.payment_method, p.category, p.name_en AS product, i.quantity, i.unit_price, i.discount,
               round(i.quantity * i.unit_price - i.discount, 2) AS amount
        FROM app.orders o
        JOIN app.order_items i USING (order_id)
        JOIN app.products p USING (product_id)
        JOIN app.stores s USING (store_id)
    )
    SELECT * EXCLUDE (k) REPLACE (order_id + k * 10000000 AS order_id)
    FROM base, range({SCALE}) r(k)
    ORDER BY hash(order_id + k, line_no)        -- deterministic shuffle
""")
n_rows = con.sql("SELECT count(*) FROM lines").fetchall()[0][0]
print(f"{n_rows:,} rows x {len(con.table('lines').columns)} columns")

# %% [markdown]
# ## 1 · CSV vs. Parquet (Snappy, ZSTD): size and scan time (6')
#
# DuckDB's `COPY ... TO` writes any format. CSV first, then Parquet with the Snappy codec:

# %%
with U.timer("write CSV"):
    con.sql(f"COPY lines TO '{O}/lines.csv' (HEADER)")
with U.timer("write Parquet (snappy)"):
    con.sql(f"COPY lines TO '{O}/lines_snappy.parquet' (FORMAT parquet, COMPRESSION snappy)")

# %% [markdown]
# ✏️ **Exercise** · Write the same table to `lines_zstd.parquet` with **ZSTD** compression.

# %%
with U.timer("write Parquet (zstd)"):
    # BEGIN SOLUTION
    con.sql(f"COPY lines TO '{O}/lines_zstd.parquet' (FORMAT parquet, COMPRESSION zstd)")
    # END SOLUTION

# %% [markdown]
# Now the same analytical query on each file: revenue by city for delivered orders. It needs 3 of 13 columns.

# %%
QUERY = "SELECT city, round(sum(amount)) AS revenue FROM {src} WHERE status = 'delivered' GROUP BY city ORDER BY city"
files = {"CSV": f"read_csv('{O}/lines.csv')",
         "Parquet snappy": f"'{O}/lines_snappy.parquet'",
         "Parquet zstd": f"'{O}/lines_zstd.parquet'"}
results = []
for name, src in files.items():
    path = OUT / {"CSV": "lines.csv", "Parquet snappy": "lines_snappy.parquet", "Parquet zstd": "lines_zstd.parquet"}[name]
    secs = U.best_of(lambda: con.sql(QUERY.format(src=src)).fetchall())
    results.append({"format": name, "size_mb": round(U.size_mb(path), 1), "query_ms": round(secs * 1000)})
for r in results:
    print(f"{r['format']:15s} {r['size_mb']:8.1f} MB {r['query_ms']:7,d} ms")

# %%
assert con.sql(QUERY.format(src=files["CSV"])).fetchall() == con.sql(QUERY.format(src=files["Parquet zstd"])).fetchall()
csv_mb, zstd_mb = results[0]["size_mb"], results[2]["size_mb"]
U.check(zstd_mb < csv_mb / 3, f"ZSTD Parquet is {csv_mb / zstd_mb:.1f}x smaller than CSV")

# %% [markdown]
# 🔍 **Look at** both columns. Parquet is several times smaller *and* faster: the engine reads only the three columns it
# needs, already typed, while the CSV must be parsed in full, every time. ZSTD is usually a little smaller than Snappy and,
# on a laptop SSD, about as fast. On object storage (S3), where you pay per byte transferred, smaller files often win outright.
#
# ## 2 · Read the footer: row groups, statistics, encodings (5')
#
# Every Parquet file ends with a footer that describes the file. `parquet_metadata()` returns one row per
# **column chunk** (= column × row group).

# %%
ZSTD = f"{O}/lines_zstd.parquet"
con.sql(f"""
    SELECT row_group_id, row_group_num_rows, path_in_schema, type, stats_min, stats_max,
           compression, encodings, total_compressed_size, total_uncompressed_size
    FROM parquet_metadata('{ZSTD}')
    LIMIT 13""")

# %% [markdown]
# ✏️ **Exercise** · With SQL on `parquet_metadata(ZSTD)`, compute:
# - `n_row_groups`: how many row groups the file has (a number: end the query with `.fetchall()[0][0]`);
# - `col_bytes`: a query with one row per column, `column_name` (= `path_in_schema`) and `bytes` (the total
#   **compressed** size, `total_compressed_size`), largest first.

# %%
# BEGIN SOLUTION
n_row_groups = con.sql(f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{ZSTD}')").fetchall()[0][0]
col_bytes = con.sql(f"""
    SELECT path_in_schema AS column_name, sum(total_compressed_size) AS bytes
    FROM parquet_metadata('{ZSTD}')
    GROUP BY ALL
    ORDER BY bytes DESC""")
# END SOLUTION
print(f"{n_row_groups} row groups for {n_rows:,} rows")
bytes_per_row = con.sql(f"SELECT column_name, round(bytes / {n_rows}, 2) AS bytes_per_row FROM col_bytes")
display(bytes_per_row)
sizes = dict(col_bytes.fetchall())
U.check(n_row_groups >= 2, "the file has several row groups")
U.check(sizes["order_id"] > 5 * sizes["store_id"], "store_id costs a fraction of order_id")

# %% [markdown]
# 🔍 `store_id` has 12 distinct values in millions of rows. **Dictionary encoding** stores each value once and the column
# becomes a run of small integers that compresses to a fraction of a byte per row. Unique, shuffled IDs (`order_id`) and
# high-precision numbers (`amount`) cost several bytes per row. Narrow, repetitive columns are nearly free in Parquet.
#
# Now look at the statistics of `created_at` per row group:

# %%
con.sql(f"""
    SELECT row_group_id, row_group_num_rows, stats_min, stats_max
    FROM parquet_metadata('{ZSTD}')
    WHERE path_in_schema = 'created_at'
    LIMIT 8""")

# %% [markdown]
# Every row group spans (almost) the whole two months: the rows are shuffled. A filter on one day can't rule out any row group.
#
# ## 3 · Sorted vs. unsorted: count row groups skipped (5')
#
# Write two files with small row groups (100k rows) so the effect is visible: one in the shuffled order, one sorted by `created_at`.

# %%
con.sql(f"COPY lines TO '{O}/unsorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
con.sql(f"COPY (FROM lines ORDER BY created_at) TO '{O}/sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
DAY_FROM, DAY_TO = "2026-09-23 00:00:00", "2026-09-24 00:00:00"

# %% [markdown]
# ✏️ **Exercise** · Write `prunable(path)`: with one SQL query on `parquet_metadata`, return
# `(total_row_groups, row_groups_to_read)` for the filter `created_at >= DAY_FROM AND created_at < DAY_TO`. Keep the
# rows of the `created_at` column. A row group must be read if its `[stats_min, stats_max]` range overlaps the filter:
# `stats_max >= DAY_FROM AND stats_min < DAY_TO`. Hint: `count(*) FILTER (WHERE ...)` counts only some rows, and
# `.fetchall()[0]` is the one result row as a tuple. (`stats_min`/`stats_max` are strings; ISO timestamps compare
# correctly as strings.)

# %%
def prunable(path):
    # BEGIN SOLUTION
    return con.sql(f"""
        SELECT count(*), count(*) FILTER (WHERE stats_max >= '{DAY_FROM}' AND stats_min < '{DAY_TO}')
        FROM parquet_metadata('{path}')
        WHERE path_in_schema = 'created_at'""").fetchall()[0]
    # END SOLUTION

for name in ["unsorted", "sorted"]:
    total, read = prunable(f"{O}/{name}.parquet")
    q = f"SELECT count(*), sum(amount) FROM '{O}/{name}.parquet' WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'"
    ms = U.best_of(lambda: con.sql(q).fetchall()) * 1000
    print(f"{name:9s} row groups to read: {read:3d} / {total:3d}   query: {ms:6.1f} ms")

# %%
U.check(prunable(f"{O}/sorted.parquet")[1] < prunable(f"{O}/unsorted.parquet")[1] / 5, "sorting lets the engine skip most row groups")

# %% [markdown]
# 🔍 Same data, same query, same answer. Sorted by the filter column, the statistics are tight and nearly every row group
# is skipped without being read. This is why table formats (Day 3) offer **sort orders** and **clustering**.
#
# You can also see it in the plan. `EXPLAIN ANALYZE` runs the query and prints its **plan**, the steps the engine took,
# read from the bottom up. The bottom box is the scan of the file: the filter it pushed into the read and the rows it
# actually produced.

# %%
print(con.sql(f"EXPLAIN ANALYZE SELECT sum(amount) FROM '{O}/sorted.parquet' WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'").fetchall()[0][1][-1800:])

# %% [markdown]
# ## 4 · Partition by date; query one day (5')
#
# Partitioning splits the table into folders by a column value (`order_date=2026-09-23/`). A filter on that column
# opens one folder and ignores the rest, even before reading any footer.
#
# ✏️ **Exercise** · Write `lines` to the folder `OUT/by_date`, partitioned by `order_date` (= `created_at::DATE`).

# %%
# BEGIN SOLUTION
con.sql(f"""
    COPY (SELECT *, created_at::DATE AS order_date FROM lines)
    TO '{O}/by_date' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date))
""")
# END SOLUTION
print(U.count_files(OUT / "by_date"), "files, e.g.", sorted(p.name for p in (OUT / "by_date").iterdir())[:3])

# %%
by_date = f"read_parquet('{O}/by_date/*/*.parquet', hive_partitioning = true)"
one_day = f"SELECT count(*), sum(amount) FROM {by_date} WHERE order_date = '2026-09-23'"
all_days = f"SELECT count(*), sum(amount) FROM {by_date}"
print("one day :", con.sql(one_day).fetchall()[0], f"{U.best_of(lambda: con.sql(one_day).fetchall()) * 1000:.1f} ms")
print("all days:", con.sql(all_days).fetchall()[0], f"{U.best_of(lambda: con.sql(all_days).fetchall()) * 1000:.1f} ms")
U.check(U.count_files(OUT / "by_date") == 61, "one folder per day")

# %% [markdown]
# 🔍 The partition column isn't stored inside the files at all: it's in the path (`order_date=2026-09-23`). That's
# **Hive-style partitioning**. Two caveats: the folder date is UTC here (we didn't convert to Riyadh time), and engines
# only prune when you filter on the partition column itself, not on `created_at`.
#
# ## 5 · Over-partition on purpose; measure the cost (4')
#
# Someone suggests partitioning by date **and** store "for faster store reports". Let's measure.
#
# ✏️ **Exercise** · Write `OUT/by_date_store`, partitioned by `order_date` **and** `store_id`. The next cell measures
# each layout: the number of files, total MB, and the best time of a full-table `SELECT store_id, sum(amount) ...`.

# %%
# BEGIN SOLUTION
con.sql(f"""
    COPY (SELECT *, created_at::DATE AS order_date FROM lines)
    TO '{O}/by_date_store' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date, store_id))
""")
# END SOLUTION
print(U.count_files(OUT / "by_date_store"), "files")

# %%
layouts = {"single file": (OUT / "sorted.parquet", f"'{O}/sorted.parquet'"),
           "by_date": (OUT / "by_date", by_date),
           "by_date_store": (OUT / "by_date_store", f"read_parquet('{O}/by_date_store/*/*/*.parquet', hive_partitioning = true)")}
cost = []
for name, (path, src) in layouts.items():
    q = f"SELECT store_id, sum(amount) FROM {src} GROUP BY store_id"
    cost.append({"layout": name, "files": 1 if path.is_file() else U.count_files(path),
                 "size_mb": round(U.size_mb(path), 1), "full_scan_ms": round(U.best_of(lambda: con.sql(q).fetchall()) * 1000)})
for c in cost:
    print(f"{c['layout']:14s} {c['files']:5,d} files {c['size_mb']:7.1f} MB {c['full_scan_ms']:6,d} ms")
U.check(cost[2]["files"] > 500, f"{cost[2]['files']} files for {cost[2]['size_mb']} MB")
U.check(cost[2]["full_scan_ms"] > cost[0]["full_scan_ms"], "the over-partitioned layout is the slowest to scan")

# %% [markdown]
# 🔍 Hundreds of small files: each one costs an open, a footer read and scheduling work, and compresses worse. On object
# storage every file is also an HTTP request. Store filters were already fast in one file thanks to dictionary encoding
# and statistics. Partition by what you **load** and **filter** by most (usually a date), keep files large, and use
# sorting for the rest.
#
# ## ✱ Stretch · Prove the partition caveat in the plan
#
# Task 4 said engines only prune when you filter on the partition column itself. Check it: `EXPLAIN ANALYZE` prints
# `Total Files Read` in the scan box. `files_read(query)` returns that number.
#
# ✏️ **Exercise** · On `by_date`, ask for 23 September twice: once with `order_date = '2026-09-23'`, once with the
# `created_at` range of Task 3 (`DAY_FROM`, `DAY_TO`). Store the files read in `via_partition` and `via_timestamp`.

# %%
import re


def files_read(query):
    plan = con.sql("EXPLAIN ANALYZE " + query).fetchall()[0][1]
    return int(re.search(r"Total Files Read: (\d+)", plan).group(1))


# BEGIN SOLUTION
via_partition = files_read(one_day)
via_timestamp = files_read(f"SELECT count(*), sum(amount) FROM {by_date} "
                           f"WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'")
# END SOLUTION
print(f"files read · order_date filter: {via_partition} · created_at filter: {via_timestamp}")
U.check(via_partition == 1, "the partition filter opens one folder")
U.check(via_timestamp > via_partition, "the same day as a created_at filter opens more files")

# %% [markdown]
# 🔍 Same rows, same answer, but the engine can't know that `created_at` on 23 September lives in the folder
# `order_date=2026-09-23`: it opens the files and checks their statistics. Filter on the partition column (dbt models
# and dashboards often forget). Iceberg's **hidden partitioning** (Day 3) removes this trap.

# %% [markdown]
# ## Checkpoint
#
# 1. Why is Parquet faster than CSV even when both files are on the same SSD?
# 2. Your daily load appends one small Parquet file per run, every 5 minutes. What happens after a year, and what's the fix?
# 3. Which column would you sort Qafila's order lines by, and which would you partition by? Why not the same?
# 4. When would you still deliver a CSV?
#
# ## 🖼️ Visual summary
#
# The pictures below are drawn from the files this lab wrote.
#
# **1 · Statistics only help when the data is sorted.** Each line is one row group's `[min, max]` of `created_at`, read
# from the footer. The amber band is the one-day filter. The engine must read every row group whose line touches the
# band: in the shuffled file that's all of them, in the sorted file one or two.

# %%
import de_viz as V

ranges = {name: con.sql(f"""SELECT row_group_id, stats_min, stats_max FROM parquet_metadata('{O}/{name}.parquet')
                            WHERE path_in_schema = 'created_at'""").df()
          for name in ["unsorted", "sorted"]}
V.row_group_ranges(ranges, DAY_FROM, DAY_TO)

# %% [markdown]
# **2 · Where the bytes go.** Compressed bytes per row, per column: repetitive columns (dictionary-encoded) are almost
# free; unique IDs and precise numbers cost the most.

# %%
V.bars(bytes_per_row, "column_name", ["bytes_per_row"],
       titles=["compressed bytes per row (ZSTD Parquet)"], fmt="{:.2f}")

# %% [markdown]
# **3 · Format and layout.** Same rows, same answers. Left: CSV vs Parquet (Task 1). Right: one sorted file vs
# partitioned by day vs over-partitioned by day and store (Task 5), for a full scan.

# %%
V.bars(results, "format", ["size_mb", "query_ms"], titles=["size (MB)", "revenue-by-city query (ms)"])
V.bars(cost, "layout", ["files", "size_mb", "full_scan_ms"], titles=["files", "size (MB)", "full scan (ms)"])
