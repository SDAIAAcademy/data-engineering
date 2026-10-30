# %% [markdown]
# # Lab 4.2 · Spark: when you need it, and how to read it
#
# Day 4 · Session 2 · 25 min. **Run this lab on Google Colab**: the setup installs Java and Spark for you in about a minute.
# What you'll do:
# - answer "do you need a cluster?" for Qafila's data, with a measurement;
# - run the same query in Spark with the DataFrame API and with SQL;
# - find the shuffles in a plan and count the files a write leaves behind.
#
# **Tasks**
# - [ ] 1 · Do you need a cluster? Time it on one machine (3')
# - [ ] 2 · Start Spark; see that nothing runs until an action (6')
# - [ ] 3 · Daily sales per city: DataFrame API and SQL (6')
# - [ ] 4 · Read the plan and find the shuffles (5')
# - [ ] 5 · Write partitioned output; count the files (5')
# - [ ] ✱ Stretch · Open the Spark UI and find your shuffle

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

# %% [markdown]
# First, a small "data lake" to work on. DuckDB exports the raw tables to Parquet, with `orders` split into one folder per
# day (`order_date=2026-09-23/`), the way a nightly job would land it.

# %%
import duckdb

LAKE = U.lab_dir("D4_2", reset=True)
con = U.connect(U.build_warehouse(), read_only=True)
con.sql(f"""COPY (SELECT * EXCLUDE (_loaded_at), created_at[:10]::DATE AS order_date FROM raw.orders)
            TO '{(LAKE / "orders").as_posix()}' (FORMAT parquet, PARTITION_BY (order_date))""")
for t in ["order_items", "stores"]:
    con.sql(f"COPY (SELECT * EXCLUDE (_loaded_at) FROM raw.{t}) TO '{(LAKE / t).as_posix()}.parquet' (FORMAT parquet)")
print(f"order folders: {len(list((LAKE / 'orders').iterdir()))} | files: {U.count_files(LAKE)} | size: {U.size_mb(LAKE):.1f} MB")

# %% [markdown]
# ## 1 · Do you need a cluster? Time it on one machine (3')
#
# **Spark** runs one job across many machines. Before you reach for it, ask how long one machine takes. The metric: for
# **delivered** orders, net sales = Σ(`quantity × unit_price − discount`) over lines with `quantity > 0`, per city and day.

# %%
DAILY_SQL = f"""
    SELECT s.city, o.order_date, count(DISTINCT o.order_id) AS orders,
           round(sum(i.quantity * i.unit_price - i.discount), 2) AS net_sales
    FROM '{(LAKE / "order_items.parquet").as_posix()}' i
    JOIN read_parquet('{(LAKE / "orders").as_posix()}/*/*.parquet', hive_partitioning = true) o USING (order_id)
    JOIN '{(LAKE / "stores.parquet").as_posix()}' s USING (store_id)
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY ALL ORDER BY o.order_date, s.city
"""
n_lines = con.sql(f"SELECT count(*) FROM '{(LAKE / 'order_items.parquet').as_posix()}'").fetchall()[0][0]
t_duck = U.best_of(lambda: con.sql(DAILY_SQL).fetchall(), 3)
duck = con.sql(DAILY_SQL).df()
print(f"{n_lines:,} order lines | {U.size_mb(LAKE):.1f} MB | DuckDB, one machine: {t_duck:.2f} s")
duck.head(3)

# %% [markdown]
# 🔍 This fits on a laptop, and so do most jobs you will meet. **Spark is the answer when the data, or the time you have,
# no longer fits one machine**, or when your company already runs everything on it. We'll use Spark here because you will
# meet it at work, not because Qafila's data needs it.
#
# ## 2 · Start Spark; see that nothing runs until an action (6')
#
# `U.get_spark()` starts Spark inside this one notebook: 2 cores, no real cluster. On a cluster your code stays the same.
# The first run on Colab installs Java and takes about a minute. (On your own machine you'd need Java 17+: use Colab.)

# %%
from pyspark.sql import functions as F

spark = U.get_spark()
print("Spark", spark.version, "| cores:", spark.sparkContext.defaultParallelism)

orders = spark.read.parquet((LAKE / "orders").as_posix())
items = spark.read.parquet((LAKE / "order_items.parquet").as_posix())
stores = spark.read.parquet((LAKE / "stores.parquet").as_posix())
orders.printSchema()

# %% [markdown]
# Two kinds of Spark calls:
# - A **transformation** (`filter`, `select`, `join`, `groupBy`) only adds a step to a plan. Nothing runs.
# - An **action** (`count`, `show`, `collect`, `write`) runs the plan.
#
# ✏️ **Exercise 1** · Keep the delivered orders of National Day (2026-09-23), then count them.
#
# *Hint:* `on_the_day` is prefilled. Add `.filter(F.col("status") == "delivered")`, then `.count()`. One line, starting with `return`.

# %%
def national_day_count():
    on_the_day = orders.filter(F.col("order_date") == "2026-09-23")
    # BEGIN SOLUTION
    return on_the_day.filter(F.col("status") == "delivered").count()
    # END SOLUTION

# %%
expected = con.sql("SELECT count(*) FROM raw.orders WHERE created_at[:10] = '2026-09-23' AND status = 'delivered'").fetchall()[0][0]
try:
    n_national = national_day_count()
except NotImplementedError:
    print(f"⏭️ Exercise 1 not done yet. Expected: {expected:,}. Using the reference.")
    n_national = orders.filter((F.col("order_date") == "2026-09-23") & (F.col("status") == "delivered")).count()
print(f"delivered orders on National Day: {n_national:,}")
U.check(n_national == expected, "same answer as DuckDB")

# a transformation alone starts no job: count the jobs before and after
tracker = spark.sparkContext.statusTracker()
jobs_before = len(tracker.getJobIdsForGroup())
delivered = orders.filter(F.col("status") == "delivered").select("order_id", "store_id")
jobs_after_plan = len(tracker.getJobIdsForGroup())
delivered.count()
jobs_after_action = len(tracker.getJobIdsForGroup())
print(f"jobs started by the transformations: {jobs_after_plan - jobs_before} | by count(): {jobs_after_action - jobs_after_plan}")

# %% [markdown]
# 🔍 Waiting lets Spark look at the whole plan before it reads anything. Run the next cell and find `PartitionFilters` in the
# output: of the 61 day folders, Spark opens **one**.

# %%
orders.filter((F.col("order_date") == "2026-09-23") & (F.col("status") == "delivered")).explain()

# %% [markdown]
# ## 3 · Daily sales per city: DataFrame API and SQL (6')
#
# Spark has two front doors to the same engine. The SQL version is done for you; it's the query from step 1.
#
# ✏️ **Exercise 2** · Finish the DataFrame version: from `grouped`, compute `orders` and `net_sales`, sorted by `order_date`, `city`.
#
# *Hint:*
# ```python
# return (grouped.agg(F.countDistinct("order_id").alias("orders"),
#                     F.round(F.sum(<quantity × unit_price − discount>), 2).alias("net_sales"))
#         .orderBy("order_date", "city"))
# ```
# A column is `F.col("quantity")`, and columns multiply and subtract like numbers.

# %%
for name, df in {"orders": orders, "items": items, "stores": stores}.items():
    df.createOrReplaceTempView(name)

daily_sql = spark.sql("""
    SELECT s.city, o.order_date, count(DISTINCT o.order_id) AS orders,
           round(sum(i.quantity * i.unit_price - i.discount), 2) AS net_sales
    FROM items i JOIN orders o USING (order_id) JOIN stores s USING (store_id)
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY s.city, o.order_date
    ORDER BY o.order_date, s.city
""")

joined = (items.filter(F.col("quantity") > 0)
          .join(orders.filter(F.col("status") == "delivered"), "order_id")
          .join(stores, "store_id"))

def daily_sales():
    grouped = joined.groupBy("city", "order_date")
    # BEGIN SOLUTION
    return (grouped
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.round(F.sum(F.col("quantity") * F.col("unit_price") - F.col("discount")), 2).alias("net_sales"))
            .orderBy("order_date", "city"))
    # END SOLUTION

# %%
try:
    daily_df = daily_sales()
except NotImplementedError:
    print("⏭️ Exercise 2 not done yet. Expected: the same rows as the SQL version. Using the SQL version from here on.")
    daily_df = daily_sql
daily_df.show(6)

a, b = daily_df.toPandas(), daily_sql.toPandas()
U.check(len(a) == len(b) == len(duck) and (a.orders.values == b.orders.values).all(), "DataFrame API == Spark SQL")
U.check(abs(a.net_sales.values - duck.net_sales.values).max() < 0.05, "Spark == DuckDB on the same files")

# %% [markdown]
# 🔍 Same rows, same plan underneath. Use whichever your team reads best.
#
# ## 4 · Read the plan and find the shuffles (5')
#
# Read a plan **from the bottom up**: the scan is at the bottom, the result at the top. Each `Exchange` line is a **shuffle**:
# rows move between machines so that equal keys meet. Shuffles are where jobs get slow, so they're what you look for first.
# Run the cells.

# %%
daily_df.explain()

# %%
plan_text = daily_df.select("*")._jdf.queryExecution().executedPlan().toString()
n_shuffles = sum("Exchange hashpartitioning" in line or "Exchange rangepartitioning" in line for line in plan_text.splitlines())
n_small = sum("BroadcastHashJoin" in line for line in plan_text.splitlines())
print("shuffles (Exchange):", n_shuffles, "| joins that copied a small table instead of shuffling:", n_small)
U.check(n_shuffles >= 1, "groupBy and orderBy shuffle")

# %% [markdown]
# 🔍 Find each `Exchange` in the printout. Which line of your code caused it: the `groupBy` or the `orderBy`?
# Why did the joins *not* shuffle? (`stores` and the filtered `orders` are small, so Spark copied them to every worker.)
#
# ## 5 · Write partitioned output; count the files (5')
#
# Each task writes its own file into every folder it has rows for. `items_wide` is spread over 8 tasks and 3 cities, so a
# plain write leaves up to 8 × 3 = 24 files. Too many small files make every later read slower.
#
# ✏️ **Exercise 3** · Write `items_wide` again to `out_tidy`, this time with `.repartition("city")` first, so all of a city's
# rows sit in one task.
#
# *Hint:* copy the line in the cell and add `.repartition("city")` after `items_wide`. One line.

# %%
lines = items.join(orders.select("order_id", "order_date", "store_id"), "order_id")
items_wide = lines.join(stores, "store_id").repartition(8)
items_wide.write.mode("overwrite").partitionBy("city").parquet((LAKE / "out_naive").as_posix())

def write_tidy():
    # BEGIN SOLUTION
    items_wide.repartition("city").write.mode("overwrite").partitionBy("city").parquet((LAKE / "out_tidy").as_posix())
    # END SOLUTION

# %%
try:
    write_tidy()
except NotImplementedError:
    print("⏭️ Exercise 3 not done yet. Expected: 3 files, one per city. Using the reference.")
    items_wide.repartition("city").write.mode("overwrite").partitionBy("city").parquet((LAKE / "out_tidy").as_posix())

n_naive, n_tidy = U.count_files(LAKE / "out_naive"), U.count_files(LAKE / "out_tidy")
print(f"files: plain write {n_naive} | after repartition {n_tidy}")
U.check(n_tidy == 3 and n_naive > n_tidy, "one file per city folder")

# %% [markdown]
# ## ✱ Stretch · Open the Spark UI and find your shuffle
#
# Spark serves a live web page with every job. Open it, click the last job and find the stage boundary where the `Exchange` is.

# %%
print("Spark UI:", spark.sparkContext.uiWebUrl)
if IN_COLAB:
    from google.colab import output
    output.serve_kernel_port_as_window(4040, path="/jobs/")

# %%
spark.stop()

# %% [markdown]
# ## Checkpoint
#
# 1. A job processes 3 GB a day and DuckDB finishes it in a minute on one machine. Do you need Spark?
#
# <details><summary>Answer</summary>No. A cluster adds cost and things that can fail, and you'd get nothing back. Reach for Spark when one machine can't finish in the time you have, or when your platform already runs on it.</details>
#
# 2. What's the difference between a transformation and an action?
#
# <details><summary>Answer</summary>A transformation adds a step to the plan and runs nothing. An action runs the plan. Waiting lets Spark skip what it doesn't need to read.</details>
#
# 3. How do you find a shuffle in a plan, and why do you care?
#
# <details><summary>Answer</summary>Look for `Exchange`. Rows move between machines there, which is where jobs usually get slow.</details>
#
# ## Carry forward
#
# You can now open a Spark job at work and read its plan. The capstone doesn't require Spark. Want more? `tools/spark.ipynb`.
