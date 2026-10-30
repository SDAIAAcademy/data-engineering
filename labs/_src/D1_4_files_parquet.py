# %% [markdown]
# # Lab 1.4 · Files for analytics
#
# Day 1 · Session 4 · 25 min. What you'll do:
# - store the same millions of order lines as CSV and as Parquet, and measure size and speed;
# - read each row group's min and max, and see why **sorting** lets an engine skip most of a file;
# - **partition** by date, then over-partition on purpose and measure the cost.
#
# **Tasks**
# - [ ] 1 · CSV vs. Parquet: size and scan time (6')
# - [ ] 2 · Read the min and max of each row group (5')
# - [ ] 3 · Sort the file and count row groups skipped (6')
# - [ ] 4 · Partition by date and query one day (5')
# - [ ] 5 · Over-partition on purpose and measure (3')
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
import re
import duckdb
import de_viz as V

con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)
OUT = U.lab_dir("D1_4", reset=True)      # artifacts/D1_4: everything this lab writes
O = OUT.as_posix()

# %% [markdown]
# Qafila has about 173k order lines. Files behave differently at scale, so we simulate a bigger Qafila by repeating the lines
# `SCALE` times (with new order IDs) and **shuffling** them, since data often arrives in no useful order. One wide row per order line:

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
    ORDER BY hash(order_id + k, line_no)        -- a repeatable shuffle
""")
n_rows = con.sql("SELECT count(*) FROM lines").fetchall()[0][0]
print(f"{n_rows:,} rows x {len(con.table('lines').columns)} columns")

# %% [markdown]
# ## 1 · CSV vs. Parquet: size and scan time (6')
#
# A **CSV** is rows written as text. **Parquet** is a column store in one file: each column together, with its type, compressed.
# `COPY ... TO` writes any table to a file. The CSV is written for you.
#
# ✏️ **Exercise 1** · Write `lines` to `lines.parquet`, compressed with ZSTD. Fill the blank.
# Hint: the options go in the brackets: `(FORMAT parquet, COMPRESSION zstd)`.

# %%
con.sql(f"COPY lines TO '{O}/lines.csv' (HEADER)")

def write_parquet():
    # con.sql(f"COPY lines TO '{O}/lines.parquet' (___)")
    # BEGIN SOLUTION
    con.sql(f"COPY lines TO '{O}/lines.parquet' (FORMAT parquet, COMPRESSION zstd)")
    # END SOLUTION

# %%
try:
    write_parquet()
    U.check((OUT / "lines.parquet").exists(), "lines.parquet written")
except NotImplementedError:
    print("⏭️ Exercise 1 not done yet. Expected: a Parquet file about 8 times smaller than the CSV. Using the reference result.")
    con.sql(f"COPY lines TO '{O}/lines.parquet' (FORMAT parquet, COMPRESSION zstd)")

# %% [markdown]
# Now Finance's question on each file: revenue by city for delivered orders. It needs 3 of the 13 columns.
#
# 🤔 **Predict** · How much smaller, and how much faster, is Parquet?

# %%
QUERY = "SELECT city, round(sum(amount)) AS revenue FROM {src} WHERE status = 'delivered' GROUP BY city ORDER BY city"
files = {"CSV": ("lines.csv", f"read_csv('{O}/lines.csv')"), "Parquet": ("lines.parquet", f"'{O}/lines.parquet'")}
results = []
for name, (file, src) in files.items():
    secs = U.best_of(lambda: con.sql(QUERY.format(src=src)).fetchall())
    results.append({"format": name, "size_mb": round(U.size_mb(OUT / file), 1), "query_ms": round(secs * 1000)})
    print(f"{name:8s} {results[-1]['size_mb']:8.1f} MB {results[-1]['query_ms']:7,d} ms")
U.check(con.sql(QUERY.format(src=files["CSV"][1])).fetchall() == con.sql(QUERY.format(src=files["Parquet"][1])).fetchall(), "same answer from both files")
print(f"Parquet is {results[0]['size_mb'] / results[1]['size_mb']:.1f}x smaller and {results[0]['query_ms'] / results[1]['query_ms']:.0f}x faster here")
V.bars(results, "format", ["size_mb", "query_ms"], titles=["size (MB)", "revenue-by-city query (ms)"])

# %% [markdown]
# 🔍 **Look at** both columns. The engine read three columns, already typed, while the CSV must be parsed in full on every query. Where did the bytes go?

# %%
bytes_per_row = con.sql(f"""
    SELECT path_in_schema AS column_name, round(sum(total_compressed_size) / {n_rows}, 2) AS bytes_per_row
    FROM parquet_metadata('{O}/lines.parquet')
    GROUP BY ALL ORDER BY bytes_per_row DESC""")
V.bars(bytes_per_row.df(), "column_name", ["bytes_per_row"], titles=["compressed bytes per row"], fmt="{:.2f}")

# %% [markdown]
# 🔍 `store_id` has 12 values in millions of rows, so it costs almost nothing. `order_id`, different on nearly every row, costs the most.
#
# ## 2 · Read the min and max of each row group (5')
#
# A big Parquet file is cut into **row groups**, slices of about 100,000 rows here. At its end, the file keeps each column's smallest and largest
# value per row group: its **statistics**. An engine reads those first and skips any row group whose range can't match the filter.
# `parquet_metadata()` shows them. This file is still shuffled.

# %%
con.sql(f"COPY lines TO '{O}/shuffled.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
DAY_FROM, DAY_TO = "2026-09-23 00:00:00", "2026-09-24 00:00:00"

def row_groups_to_read(path):
    """(all row groups, the ones whose created_at range touches the day)"""
    return con.sql(f"""
        SELECT count(*), count(*) FILTER (WHERE stats_max >= '{DAY_FROM}' AND stats_min < '{DAY_TO}')
        FROM parquet_metadata('{path}')
        WHERE path_in_schema = 'created_at'""").fetchall()[0]

con.sql(f"""
    SELECT row_group_id, row_group_num_rows, stats_min, stats_max
    FROM parquet_metadata('{O}/shuffled.parquet')
    WHERE path_in_schema = 'created_at' LIMIT 4""")

# %%
total, read = row_groups_to_read(f"{O}/shuffled.parquet")
print(f"a filter on 23 September must read {read} of {total} row groups")

# %% [markdown]
# 🔍 Each row group spans nearly the whole two months, because the rows are shuffled. The statistics are there, but they rule out nothing.
#
# ## 3 · Sort the file and count row groups skipped (6')
#
# ✏️ **Exercise 2** · Write `sorted.parquet` with the rows sorted by `created_at`. Fill the blank.
# Hint: `COPY` takes any query in brackets: `COPY (FROM lines ORDER BY ...) TO ...`.

# %%
def write_sorted():
    # con.sql(f"COPY (FROM lines ORDER BY ___) TO '{O}/sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
    # BEGIN SOLUTION
    con.sql(f"COPY (FROM lines ORDER BY created_at) TO '{O}/sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
    # END SOLUTION

# %%
try:
    write_sorted()
except NotImplementedError:
    print("⏭️ Exercise 2 not done yet. Expected: only a few row groups to read. Using the reference result.")
    con.sql(f"COPY (FROM lines ORDER BY created_at) TO '{O}/sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")

day_query = "SELECT count(*), sum(amount) FROM '{path}' WHERE created_at >= '" + DAY_FROM + "' AND created_at < '" + DAY_TO + "'"
for name in ["shuffled", "sorted"]:
    path = f"{O}/{name}.parquet"
    total, read = row_groups_to_read(path)
    ms = U.best_of(lambda: con.sql(day_query.format(path=path)).fetchall()) * 1000
    print(f"{name:9s} row groups to read: {read:3d} / {total:3d}  query: {ms:6.1f} ms  size: {U.size_mb(path):6.1f} MB")
U.check(row_groups_to_read(f"{O}/sorted.parquet")[1] < row_groups_to_read(f"{O}/shuffled.parquet")[1] / 5, "sorting lets the engine skip most row groups")

# %%
ranges = {name: con.sql(f"""SELECT row_group_id, stats_min, stats_max FROM parquet_metadata('{O}/{name}.parquet')
                            WHERE path_in_schema = 'created_at'""").df()
          for name in ["shuffled", "sorted"]}
V.row_group_ranges(ranges, DAY_FROM, DAY_TO)

# %% [markdown]
# 🔍 **Look at** the picture. Each line is one row group's min and max, and the band is the day we filter on. The engine reads every line that touches the band.
# Same data, same answer. Sorted values also compress better, so the file got smaller.
#
# ## 4 · Partition by date and query one day (5')
#
# **Partitioning** splits a table into one folder per value: `order_date=2026-09-23/`. A filter on that column opens one folder and ignores the rest.
#
# ✏️ **Exercise 3** · Write `lines` to the folder `by_date`, one folder per `order_date`. Fill the blank.
# Hint: the option is `PARTITION_BY (column)`.

# %%
WITH_DATE = "(SELECT *, created_at::DATE AS order_date FROM lines)"

def write_by_date():
    # con.sql(f"COPY {WITH_DATE} TO '{O}/by_date' (FORMAT parquet, COMPRESSION zstd, ___)")
    # BEGIN SOLUTION
    con.sql(f"COPY {WITH_DATE} TO '{O}/by_date' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date))")
    # END SOLUTION

# %%
try:
    write_by_date()
    U.check(U.count_files(OUT / "by_date") == 61, "one folder and one file per day: 61")
except NotImplementedError:
    print("⏭️ Exercise 3 not done yet. Expected: 61 files, one per day. Using the reference result.")
    con.sql(f"COPY {WITH_DATE} TO '{O}/by_date' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date))")
print(sorted(p.name for p in (OUT / "by_date").iterdir())[:3], "...")

by_date = f"read_parquet('{O}/by_date/*/*.parquet', hive_partitioning = true)"
one_day = f"SELECT count(*), round(sum(amount), 2) FROM {by_date} WHERE order_date = '2026-09-23'"
print("one day:", con.sql(one_day).fetchall()[0], f"in {U.best_of(lambda: con.sql(one_day).fetchall()) * 1000:.1f} ms")

# %% [markdown]
# 🔍 `order_date` isn't stored inside the files at all: it's in the folder name. This is **Hive-style partitioning**. Note that these folders use the UTC date, since we didn't convert to Riyadh time.
#
# ## 5 · Over-partition on purpose and measure (3')
#
# Someone suggests partitioning by date **and** store, "for faster store reports". Run it and read the table.

# %%
con.sql(f"COPY {WITH_DATE} TO '{O}/by_date_store' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date, store_id))")
layouts = {"one sorted file": (OUT / "sorted.parquet", f"'{O}/sorted.parquet'"),
           "by date": (OUT / "by_date", by_date),
           "by date and store": (OUT / "by_date_store", f"read_parquet('{O}/by_date_store/*/*/*.parquet', hive_partitioning = true)")}
cost = []
for name, (path, src) in layouts.items():
    q = f"SELECT store_id, sum(amount) FROM {src} GROUP BY store_id"
    cost.append({"layout": name, "files": 1 if path.is_file() else U.count_files(path),
                 "size_mb": round(U.size_mb(path), 1), "full_scan_ms": round(U.best_of(lambda: con.sql(q).fetchall()) * 1000)})
    print(f"{name:18s} {cost[-1]['files']:5,d} files {cost[-1]['size_mb']:7.1f} MB {cost[-1]['full_scan_ms']:6,d} ms")
U.check(cost[2]["full_scan_ms"] > cost[0]["full_scan_ms"], "the over-partitioned layout is slower to scan")
V.bars(cost, "layout", ["files", "size_mb", "full_scan_ms"], titles=["files", "size (MB)", "full scan (ms)"])

# %% [markdown]
# 🔍 Every small file costs an open, a read of its statistics and, on cloud storage, a paid request. Partition by what you **load** and **filter** by most, usually a date, keep files large, and sort for the rest.
#
# ## ✱ Stretch · Prove the partition caveat in the plan
#
# Engines only skip folders when you filter on the partition column itself. `EXPLAIN ANALYZE` runs a query and prints its **plan**, including how many files it read.

# %%
def files_read(query):
    plan = con.sql("EXPLAIN ANALYZE " + query).fetchall()[0][1]
    return int(re.search(r"Total Files Read: (\d+)", plan).group(1))

via_partition = files_read(one_day)
via_timestamp = files_read(f"SELECT count(*), sum(amount) FROM {by_date} WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'")
print(f"files read · order_date filter: {via_partition} · created_at filter: {via_timestamp}")

# %% [markdown]
# 🔍 The same day, but the engine can't know that `created_at` on 23 September lives in the folder `order_date=2026-09-23`.
#
# ## Checkpoint
#
# 1. Why is Parquet faster than CSV even on the same disk?
# 2. A 20 GB Parquet table is filtered by `created_at` in almost every query, yet each query reads nearly all of it. What's the most likely cause?
# 3. A job appends one small file every 5 minutes. What happens after a year?
#
# <details><summary>Answers</summary>
#
# 1. It reads only the columns the query names, already typed and compressed, while a CSV is parsed in full as text.
# 2. The rows aren't sorted by `created_at`, so every row group's min and max span nearly the whole range and nothing can be skipped.
# 3. About 100,000 small files. Each costs an open and a read of its statistics, so queries slow down. The cure is to rewrite them into fewer large files.
#
# </details>
#
# ## Carry forward
#
# Parquet is the file format under the rest of the week: Lab 2.4 writes it, and Lab 4.1 builds reliable tables on it. Each starts from its own data.
#
# Want more? `tools/duckdb.ipynb` shows more ways to read and write files.
