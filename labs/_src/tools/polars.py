# %% [markdown]
# # Polars, one idea at a time
#
# Optional, self-paced: goes deeper than Session 2.4.
#
# CPU only. Operations asks how many couriers delivered in each city. You answer with a
# Python loop and watch it slow down, then build Polars, the DataFrame library Lab 2.4 cleans bronze into silver
# with, one idea at a time.
#
# **The data:** Qafila's own: a 12-row stores table you can check by eye, then the 45,766 orders of the app database
# and the courier feed. Lab 2.4 cleans the messy parts (timestamps, duplicates, time zones).
#
# 🤔 asks you to predict before you run a cell · 🔍 says what the output means · ➕ sections are for your own pace.
#
# **Contents**
# - [ ] 1 · The problem: a question answered by a loop
# - [ ] 2 · The mental model: you write the rules, the engine runs them
# - [ ] 3 · A DataFrame is a table of typed columns
# - [ ] 4 · Expressions and `select`
# - [ ] 5 · `filter`: keep the rows you want
# - [ ] 6 · `with_columns`: add and fix columns
# - [ ] 7 · `group_by` + `agg`: one row per group
# - [ ] 8 · Joins: combine tables
# - [ ] 9 · `over`: group answers without collapsing rows ➕
# - [ ] 10 · Lazy mode: describe the query, let Polars plan it
# - [ ] 11 · Put it together: one question, one chain ➕
# - [ ] 12 · Summary
#
# Know pandas? Polars has no index, changes nothing in place (every method returns a new DataFrame), and describes
# columns with **expressions** instead of `df[...]`. §4 is the one to read slowly.

# %% [markdown] 🎓
# 🎓 **Instructor notes** (instructor copy only)
#
# - Core ≈ 27': §1 3' · §2 3' · §3 2' · §4 4' · §5 2' · §6 2' · §7 3' · §8 2' (one join; orphans ➕) · §10 5' ·
#   §12 1'. §4 (expressions and contexts) is the one to slow down on. §9 and §11 are ➕.
# - In class, run the cells of a ➕ part without stopping on them: later cells may use what they build (§8 and §11
#   use `lines` from §4).
# - §1 reads the courier feed 40 times (≈900k events): the loop takes several seconds, Polars well under one. The
#   ratio is the point, not the numbers; quick mode reads it 10 times.
# - Lab 2.4 parses three timestamp formats, picks one copy per `event_id` and converts to Riyadh time: don't teach those here.
#   §6's time zones and §10's plans are what it builds on.

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
        os.chdir("..")  # tools/ and solutions/tools/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import sqlite3
import polars as pl

pl.Config.set_tbl_rows(8)                                  # print at most 8 rows of a table
app = sqlite3.connect(U.RAW / "qafila_oltp.sqlite")       # Qafila's app database (SQLite)
print("polars", pl.__version__)

# %% [markdown]
# Polars **transforms** bronze into silver (Lab 2.4) and **tests** DataFrames on the way (Lab 3.3). It does in Python,
# with a DataFrame API, what DuckDB does with SQL.

# %%
import de_viz as V
V.platform_map(tool="polars")

# %% [markdown]
# ## 1 · The problem: a question answered by a loop
#
# Operations asks: **how many couriers delivered at least one order in each city?** The answer is in the courier
# feed, one JSON object per line. To see how a plain Python loop grows with the data, it reads the feed 40 times.

# %%
import json
from collections import defaultdict

FEED = U.RAW / "events" / "delivery_events.jsonl"
COPIES = U.pick(40, 10)                      # read the feed this many times (fewer in quick mode)
times = {}

with U.timer("a Python loop", times):
    couriers = defaultdict(set)               # city → the couriers seen delivering there
    events_read = 0
    for _ in range(COPIES):
        with open(FEED, encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)      # parse all 10 fields of every event...
                events_read += 1
                if event["event_type"] == "delivered":   # ...to keep 2 of them, from 1 event in 4
                    couriers[event["city"]].add(event["courier_id"])

by_loop = {city: len(ids) for city, ids in sorted(couriers.items())}
print(f"{events_read:,} events read →", by_loop)

# %% [markdown]
# 🤔 **Predict** · The same question in Polars, on the same 40 copies. How much faster: twice, ten times?

# %%
with U.timer("Polars", times):
    by_polars = (
        pl.scan_ndjson([FEED] * COPIES)
          .filter(pl.col("event_type") == "delivered")
          .group_by("city")
          .agg(pl.col("courier_id").n_unique().alias("couriers"))
          .sort("city")
          .collect()
    )
print(by_polars.rows())
U.check(dict(by_polars.rows()) == by_loop, "the same answer as the loop")
print(f"Polars was {times['a Python loop'] / times['Polars']:.0f}× faster")

# %% [markdown]
# 🔍 The loop is correct, but it runs on one core, parses every field of every line, and welds **how** it runs to
# **what** it computes: the rule ("delivered, distinct couriers, per city") is spread over a `for`, an `if`, a `dict`
# and a `set`. In Polars the rule reads as written, and a new question means changing one expression.
#
# ## 2 · The mental model: you write the rules, the engine runs them
#
# ```
#  a DataFrame (eager)        expressions          a plan             execution
#  or a LazyFrame (lazy)  →   in a context    →    optimized     →    on columns, in batches,  →  a DataFrame
#  typed columns              select, filter, …    (pushdowns)        on every core
#  └──────────────── you write this ────────────────┘  └───────────── Polars does this ─────────────┘
# ```
#
# It is the same split as SQL in a database: you say **what**, the engine decides **how**. What no engine decides for
# you is the **meaning**: which timestamp is the real one, which copy of a duplicate survives, what counts as revenue.
# Those rules are yours to write.
#
# ## 3 · A DataFrame is a table of typed columns
#
# Qafila has 12 dark stores. `pl.read_database` runs a SQL query on a database connection and returns a **DataFrame**.

# %%
stores = pl.read_database("SELECT * FROM stores", app)
stores

# %% [markdown]
# Read the header before the rows: under each column name is its **data type** (`str` text, `f64` decimal, `i64`
# integer...), and every value of the column has that type. `opened_at` is `str`: SQLite has no date type, so the app
# stores dates as text. Turning text into real types is a big part of cleaning (§6).

# %%
print(stores.shape)
print(stores.columns)
stores.schema

# %% [markdown]
# One column on its own is a **Series**: a name, a type and the values.

# %%
city = stores["city"]
print(type(city).__name__, "|", city.name, "|", city.dtype, "|", city.len(), "values")
city.unique().sort().to_list()

# %% [markdown]
# Now the real tables: 45,766 orders and 172,545 order lines, in well under a second. `glimpse()` prints one line per
# column, easier to read for wide tables.

# %%
orders = pl.read_database("SELECT * FROM orders", app)
items = pl.read_database("SELECT * FROM order_items", app)
customers = pl.read_database("SELECT * FROM customers", app)
print(f"orders {orders.shape}  items {items.shape}  customers {customers.shape}")
orders.glimpse()

# %% [markdown]
# 🔍 `created_at` is `str` again, and `promo_code` is `null` on most rows: **null** is Polars' "no value" (SQL's
# `NULL`), and every type can hold it.
#
# Files give the same DataFrame, whatever the format:

# %%
reviews = pl.read_csv(U.RAW / "reviews.csv")
payments = pl.read_ndjson(U.RAW / "api" / "payments.jsonl")
print(f"reviews {reviews.shape}  payments {payments.shape}")
payments.head(3)

# %% [markdown]
# ## 4 · Expressions and `select`
#
# The rest of Polars is built on this idea. `pl.col("delivery_fee")` does **not** fetch a column. It is an
# **expression**: a recipe that says "take the column called `delivery_fee`". Extend it, and still nothing is computed:

# %%
fee_with_vat = pl.col("delivery_fee") * 1.15
print(fee_with_vat)

# %% [markdown]
# An expression is computed only when you hand it to a DataFrame method that runs it, a **context**. There are four,
# one per section below:
#
# | context | what it does with your expressions | rows out |
# |---|---|---|
# | `df.select(...)` | computes them; keeps **only** them | same, or 1 if they aggregate |
# | `df.with_columns(...)` | computes them; adds them to (or replaces) the existing columns | same |
# | `df.filter(...)` | keeps the rows where the expression is `true` | fewer |
# | `df.group_by(...).agg(...)` | computes them once per group | one per group |
#
# `select` with column names picks columns. With expressions, it computes new ones.

# %%
orders.select("order_id", "store_id", fee_with_vat)

# %% [markdown]
# 🤔 **Predict** · The last column was computed as `delivery_fee * 1.15`. What is it **called** in the output? And what
# happens if you also ask for the original `delivery_fee`?

# %%
try:
    orders.select("order_id", "store_id", "delivery_fee", fee_with_vat)
except pl.exceptions.PolarsError as e:
    print(type(e).__name__, "→", str(e).splitlines()[0])

# %% [markdown]
# 🔍 A computed column takes the name of the first column in its expression: two columns called `delivery_fee`, and
# Polars refuses. Name every computed column yourself with `.alias(...)`:

# %%
orders.select("order_id", "store_id", "delivery_fee", fee_with_vat.alias("fee_with_vat"))

# %% [markdown]
# Some expressions **aggregate** a column into one value: `sum`, `mean`, `n_unique`... `pl.len()` counts rows.
#
# 🤔 **Predict** · How many rows does the first `select` below return? And the second one, which mixes a plain column
# with an aggregate?

# %%
orders.select(pl.len().alias("orders"), pl.col("delivery_fee").sum().alias("fees"), pl.col("customer_id").n_unique())

# %%
orders.select("order_id", "delivery_fee", pl.col("delivery_fee").mean().alias("avg_fee"))

# %% [markdown]
# 🔍 One row when everything aggregates. Otherwise Polars repeats (**broadcasts**) the single value on every row, like
# SQL's `avg(delivery_fee) OVER ()`.
#
# Finance counts revenue from order lines. The amount of a line is `quantity * unit_price - discount`; §8 and §11
# build on these `lines`.

# %%
lines = items.select(
    "order_id", "line_no",
    (pl.col("quantity") * pl.col("unit_price") - pl.col("discount")).round(2).alias("line_total"),
)
U.check(lines["line_total"][0] == 98.4, "order 5000001, line 1: 2 × 49.20 = 98.4 SAR")
lines.head(3)

# %% [markdown]
# ## 5 · `filter`: keep the rows you want
#
# `filter` keeps the rows where an expression is `true` (a **predicate**).

# %%
orders.filter(pl.col("status") == "cancelled")

# %% [markdown]
# Combine predicates with `&` (and), `|` (or) and `~` (not), **not** with Python's `and`/`or`/`not`.
#
# 🤔 **Predict** · The next cell wants cancelled orders with a free delivery. Will it work?

# %%
try:
    orders.filter(pl.col("status") == "cancelled" & pl.col("delivery_fee") == 0)
except TypeError as e:
    print("TypeError →", str(e).splitlines()[0])

# %% [markdown]
# 🔍 In Python, `&` binds **tighter** than `==`, so this reads `pl.col("status") == ("cancelled" & pl.col(...)) == 0`.
# Put each comparison in parentheses:

# %%
orders.filter((pl.col("status") == "cancelled") & (pl.col("delivery_fee") == 0))

# %% [markdown]
# Predicates separated by commas are combined with `&` for you:

# %%
orders.filter(pl.col("status").is_in(["cancelled", "failed_delivery"]), pl.col("promo_code").is_not_null())

# %% [markdown]
# The app database has a planted problem: order lines with a quantity of zero or less, which no real basket has.

# %%
bad_lines = items.filter(pl.col("quantity") <= 0)
U.check(bad_lines.height == 142, "142 lines with a quantity of zero or less")
bad_lines.head(3)

# %% [markdown]
# 🔍 Finding bad rows is easy; deciding what to do with them (drop, fix, quarantine, ask the source team) is a data
# engineering decision. Lab 3.3 builds a quarantine for exactly this.
#
# ## 6 · `with_columns`: add and fix columns
#
# `with_columns` keeps **every** column and adds yours; an expression whose name already exists **replaces** that
# column. That makes it the tool for fixing types. `.str.to_datetime()` parses text into a datetime:

# %%
orders.with_columns(pl.col("created_at").str.to_datetime()).select("order_id", "created_at").head(3)

# %% [markdown]
# A datetime has its own methods under `.dt` (`.dt.date()`, `.dt.hour()`...), as text has `.str`. Only the right type
# has them.
#
# 🤔 **Predict** · The next cell parses `created_at` and, in the **same** `with_columns`, takes its date. Will it work?

# %%
try:
    orders.with_columns(
        pl.col("created_at").str.to_datetime(),
        pl.col("created_at").dt.date().alias("order_date"),
    )
except pl.exceptions.PolarsError as e:
    print(type(e).__name__, "→", str(e).splitlines()[0])

# %% [markdown]
# 🔍 The expressions of one context run **in parallel**, on the DataFrame as it was **before** the context: the second
# one still sees text. When a column depends on another new column, chain two contexts:

# %%
orders.with_columns(pl.col("created_at").str.to_datetime()).with_columns(
    pl.col("created_at").dt.date().alias("order_date")
).select("order_id", "created_at", "order_date").head(3)

# %% [markdown]
# Operations wants to know at what hour people order. The app stores `created_at` in **UTC**, but customers live on
# Riyadh time, three hours ahead. Say the text is UTC, then convert. The rest of the notebook uses this `orders`:

# %%
orders = orders.with_columns(
    pl.col("created_at").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Riyadh"),
    pl.col("updated_at").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Riyadh"),
)
orders.select("order_id", "created_at", "updated_at").head(3)

# %% [markdown]
# Order 5000001 was placed at 12:14 in Riyadh: the text said `09:14:07`, in UTC. Now the hour, and the weekend:

# %%
orders_t = orders.with_columns(
    pl.col("created_at").dt.hour().alias("hour"),
    pl.col("created_at").dt.weekday().is_in([5, 6]).alias("is_weekend"),   # Monday = 1 … Sunday = 7: Fri, Sat
)
U.check(orders_t["hour"].mode().item() == 20, "the busiest hour is 20:00 to 21:00, Riyadh time")
orders_t.select("order_id", "created_at", "hour", "is_weekend").head(3)

# %% [markdown]
# **Conditions** with `pl.when(...).then(...).otherwise(...)`, Polars' `CASE WHEN`:

# %%
orders.with_columns(
    pl.when(pl.col("delivery_fee") == 0).then(pl.lit("free")).otherwise(pl.lit("paid")).alias("fee_type")
).select("order_id", "delivery_fee", "fee_type").head(3)

# %% [markdown]
# 🔍 A plain string in an expression means a **column name**: `.then("free")` would look for a column called `free`.
# `pl.lit` makes it a value.
#
# ## 7 · `group_by` + `agg`: one row per group
#
# `group_by(keys).agg(expressions)` computes each expression once per group, like SQL's `GROUP BY`.

# %%
orders.group_by("status").agg(pl.len().alias("orders"), pl.col("delivery_fee").mean().round(2).alias("avg_fee"))

# %% [markdown]
# 🔍 Run it again: the rows may come back in another order. Polars computes the groups in parallel and returns them as
# they finish. When the order matters, `.sort(...)` after the `agg`.
#
# Operations asks whether some stores cancel more than others. The **mean of a `true`/`false` column** is the share of
# `true`s, so `(pl.col("status") == "cancelled").mean()` is a cancellation rate:

# %%
by_store = (
    orders.group_by("store_id")
          .agg(pl.len().alias("orders"),
               (pl.col("status") == "cancelled").mean().round(3).alias("cancel_rate"))
          .sort("cancel_rate", descending=True)
)
U.check(by_store["cancel_rate"].is_between(0.06, 0.08).all(), "cancellation rates between 6% and 8%")
by_store

# %% [markdown]
# 🔍 No store stands out: a cancellation problem would be a platform problem, not a store problem.
#
# ## 8 · Joins: combine tables
#
# Orders know their `store_id`; stores know their `city`. `join` matches rows on a key column:

# %%
orders.join(stores.select("store_id", "city"), on="store_id").select("order_id", "store_id", "city", "status").head(3)

# %% [markdown]
# 🤔 **Predict** · `orders` has 45,766 rows and `items` 172,545. How many rows does `orders.join(items, on="order_id")`
# have?

# %%
orders.join(items, on="order_id").shape

# %% [markdown]
# 🔍 172,545: one row per **line**, so each order's columns repeat once per line. Always ask "one row per what?" after
# a join: summing `delivery_fee` here would count each fee once per line.
#
# ➕ Compliance: under Saudi Arabia's PDPL, a customer can ask to be erased. The app deletes their row from
# `customers`, but their orders stay, pointing at a `customer_id` that no longer exists: **orphan orders**. A join with
# `how="anti"` finds them:

# %%
orphans = orders.join(customers, on="customer_id", how="anti")
U.check(orphans.height == 1040, "1,040 orders point at an erased customer")
print(orphans.height, "orphan orders from", orphans["customer_id"].n_unique(), "erased customers")

# %% [markdown]
# The `how` argument says what to do with rows that don't match:
#
# | `how=` | keeps |
# |---|---|
# | `"inner"` (default) | only the rows that match on both sides |
# | `"left"` | every row of the left table; nulls where the right has no match |
# | `"anti"` | only the left rows that have **no** match on the right (and no right columns) |
# | `"semi"` | only the left rows that **have** a match (and no right columns) |
#
# 🔍 An inner join of orders with customers would **silently** drop these 1,040 orders, and revenue would shrink by a
# couple of percent in every report built on it.
#
# Joins chain like everything else. Revenue per store: lines → their order (status, store) → their store (city):

# %%
store_revenue = (
    lines.join(orders.select("order_id", "store_id", "status"), on="order_id")
         .filter(pl.col("status") == "delivered")
         .join(stores.select("store_id", "city"), on="store_id")
         .group_by("city", "store_id")
         .agg(pl.col("line_total").sum().round(0).alias("revenue"))
         .sort("city", "store_id")
)
store_revenue

# %% [markdown]
# ## 9 · `over`: group answers without collapsing rows ➕
#
# "What share of its city's revenue does each store make?" needs the city's total **next to each row**. That's
# `.over(...)`: `pl.col("revenue").sum().over("city")` is SQL's `sum(revenue) OVER (PARTITION BY city)`.
#
# 🤔 **Predict** · How many rows will this return: 3 (one per city) or 12?

# %%
store_revenue.with_columns(
    pl.col("revenue").sum().over("city").alias("city_revenue"),
    (pl.col("revenue") / pl.col("revenue").sum().over("city")).round(3).alias("share"),
)

# %% [markdown]
# 🔍 Twelve. The sum is computed per city, then written on every store of that city.
#
# ## 10 · Lazy mode: describe the query, let Polars plan it
#
# So far every method ran at once: **eager** mode, good for exploring. In **lazy** mode you describe the whole query,
# and nothing is read or computed until `.collect()`. In between, Polars **optimizes** it, as a database does with SQL.
#
# `pl.scan_ndjson` is the lazy twin of `pl.read_ndjson`. Here is §1's query again, on one copy of the feed:

# %%
events = pl.scan_ndjson(os.path.relpath(FEED))   # a relative path keeps the plan short
query = (
    events.filter(pl.col("event_type") == "delivered")
          .select("city", "courier_id")
          .group_by("city")
          .agg(pl.col("courier_id").n_unique().alias("couriers"))
          .sort("city")
)
type(query).__name__

# %% [markdown]
# Nothing has been read yet: a `LazyFrame` is a **plan**. `.explain()` prints the plan Polars will run, after
# optimizing it. Read it from the bottom up: the bottom is the file scan.

# %%
print(query.explain())

# %% [markdown]
# Two things happened to the scan at the bottom:
# - **projection pushdown**: `PROJECT 3/10 COLUMNS`. Polars parses only the 3 fields the query uses.
# - **predicate pushdown**: `SELECTION: ...` The filter moved **into** the scan, so other events are dropped as the
#   file is read, never stored in memory.
#
# You wrote the steps in the order that was clear to you; the optimizer reordered them. On files of many gigabytes
# that's the difference between seconds and out-of-memory.

# %%
query.collect()

# %% [markdown]
# On **Parquet** the same pushdowns go further: each column is stored apart, so the other 7 are never read from disk,
# and min/max statistics per block of rows let the filter skip whole blocks.
#
# 🤔 **Predict** · We save the feed as Parquet and run the same query with `pl.scan_parquet`. What changes in the plan?

# %%
events_parquet = U.lab_dir("tools_polars") / "delivery_events.parquet"
events.collect().write_parquet(events_parquet)
query_parquet = (
    pl.scan_parquet(os.path.relpath(events_parquet))
      .filter(pl.col("event_type") == "delivered")
      .select("city", "courier_id")
      .group_by("city")
      .agg(pl.col("courier_id").n_unique().alias("couriers"))
      .sort("city")
)
print(query_parquet.explain())
U.check(query_parquet.collect().equals(query.collect()), "same answer from the Parquet copy")

# %% [markdown]
# 🔍 Only the scan line changed (`Parquet SCAN`): the plan is the same whatever the format. What differs is what the
# scan can then **avoid reading**.
#
# A good habit: **eager to explore** a sample, **lazy for the pipeline** you'll run again and again. Start from a
# `scan_` when you can: `pl.read_ndjson(...).lazy()` optimizes what comes after, but the file was already read whole.
#
# ## 11 · Put it together: one question, one chain ➕
#
# Finance asks: *what is the average delivered basket per city, in September?* One lazy chain: a filter, two joins,
# an aggregation and a derived column.

# %%
basket_by_city = (
    orders.lazy()
          .filter(pl.col("status") == "delivered", pl.col("created_at").dt.month() == 9)
          .join(lines.lazy(), on="order_id")
          .join(stores.lazy().select("store_id", "city"), on="store_id")
          .group_by("city")
          .agg(pl.col("line_total").sum().alias("revenue"),
               pl.col("order_id").n_unique().alias("orders"))
          .with_columns((pl.col("revenue") / pl.col("orders")).round(2).alias("avg_basket"),
                        pl.col("revenue").round(2))
          .sort("city")
          .collect()
)
basket_by_city

# %% [markdown]
# The same question in **SQL**, with DuckDB, straight on the app database. Lab 2.4 makes this comparison too.

# %%
import duckdb

con = duckdb.connect()
U.attach_app(con)
sql_answer = con.sql("""
    SELECT s.city,
           round(sum(i.quantity * i.unit_price - i.discount), 2)                          AS revenue,
           count(DISTINCT o.order_id)                                                     AS orders,
           round(sum(i.quantity * i.unit_price - i.discount) / count(DISTINCT o.order_id), 2) AS avg_basket
    FROM app.orders o
    JOIN app.order_items i USING (order_id)
    JOIN app.stores s USING (store_id)
    WHERE o.status = 'delivered'
      AND month(timezone('Asia/Riyadh', o.created_at::TIMESTAMP AT TIME ZONE 'UTC')) = 9
    GROUP BY s.city ORDER BY s.city
""").pl()
U.check(basket_by_city["orders"].to_list() == sql_answer["orders"].to_list(), "same orders per city as SQL")
U.check((basket_by_city["avg_basket"] - sql_answer["avg_basket"]).abs().max() < 0.02, "same average basket as SQL")

# %% [markdown]
# 🔍 Two details of this question are easy to get wrong:
# - **September in which time zone?** An order placed at 01:30 on 1 September in Riyadh is 22:30 on 31 August in UTC.
#   Because §6 converted `created_at` to Riyadh time, `.dt.month()` uses the customer's calendar.
# - **One row per what?** After the join with `lines` there is one row per line, so `pl.len()` would count lines.
#   `n_unique` on `order_id` counts each order once.
#
# ## 12 · Summary
#
# | | |
# |---|---|
# | **It does** | DataFrame transformations in Python, fast, on every core · lazy queries with a plan you can read · files in place (CSV, JSON lines, Parquet, Iceberg, databases) · larger-than-memory queries with the streaming engine (`.collect(engine="streaming")`, `sink_parquet`) |
# | **It doesn't** | store tables or run SQL for other users (that's a database: DuckDB, a warehouse) · scale past one machine (Spark, Session 4.2) · decide your cleaning rules · schedule, ingest or test by itself (Dagster, dlt, Pandera) |
# | **In this course** | bronze → silver cleaning (Lab 2.4) · Pandera checks on DataFrames (Lab 3.3) · reading an Iceberg table (`pl.scan_iceberg`, Lab 4.1) |
# | **As a service** | none as such: Polars runs inside your job, and Polars Cloud runs Polars queries remotely. The same DataFrame code at cluster scale is Spark's DataFrame API (Databricks, EMR, Dataproc) |
#
# **Which engine?** It follows the workload, not a gigabyte threshold. On one machine, Polars and DuckDB give the same
# answer (Lab 2.4 checks it), so the choice is mostly the language your team writes. A larger dataset doesn't by
# itself need a cluster: measure memory, disk spill and the plan first.
#
# **Cheat sheet**
#
# | Polars | SQL, or what it does |
# |---|---|
# | `pl.read_database(sql, con)`, `pl.read_csv`, `pl.read_ndjson`, `pl.read_parquet` | a DataFrame from a database or a file |
# | `pl.scan_ndjson`, `pl.scan_csv`, `pl.scan_parquet`, `df.lazy()` | a LazyFrame: nothing is read until `.collect()` |
# | `df.schema`, `df.glimpse()`, `df.null_count()` | column types, one line per column, nulls per column |
# | `df.select("a", (pl.col("b") * 2).alias("c"))` | `SELECT a, b * 2 AS c`: only these columns; 1 row if all aggregate |
# | `df.with_columns((pl.col("b") * 2).alias("c"))` | `SELECT *, b * 2 AS c`: every column; a same name replaces it |
# | `df.filter((pl.col("x") == 1) & (pl.col("y") > 2))` | `WHERE x = 1 AND y > 2`: fewer rows |
# | `.is_in([...])`, `.is_between(a, b)`, `.is_null()`, `.is_not_null()`, `.str.contains(...)` | more predicates |
# | `pl.when(pl.col("x") == 0).then(pl.lit("a")).otherwise(pl.lit("b"))` | `CASE WHEN x = 0 THEN 'a' ELSE 'b' END` |
# | `.str.to_datetime()`, `.dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Riyadh")` | parse text; set, then convert a time zone |
# | `.dt.date()`, `.dt.hour()`, `.dt.weekday()`, `.dt.month()` | parts of a datetime (weekday: Monday = 1) |
# | `df.group_by("k").agg(pl.len(), pl.col("v").sum())` | `GROUP BY k` + `count(*)`, `sum(v)`: one row per group, in any order |
# | `pl.col("v").n_unique()` | `count(DISTINCT v)` |
# | `(pl.col("s") == "x").mean()` | the share of rows where `s = 'x'` |
# | `df.sort("v", descending=True)` | `ORDER BY v DESC` |
# | `df.join(t, on="k")` / `how="left"` | `JOIN t USING (k)` / `LEFT JOIN` |
# | `df.join(t, on="k", how="anti")` / `how="semi"` | `WHERE k NOT IN (SELECT k FROM t)` / `WHERE k IN (...)` |
# | `pl.col("v").sum().over("k")` | `sum(v) OVER (PARTITION BY k)` |
# | `lazy_frame.explain()` | `EXPLAIN`: the optimized plan, with its pushdowns |
#
# ## Checkpoint
#
# 1. The loop in §1 was correct. Why does it get slower, and harder to change, as the data and the questions grow?
# 2. What is the difference between `pl.col("fee") * 2` and `orders["fee"] * 2`?
# 3. Name the four contexts. Why can't one `with_columns` both create a column and use it?
# 4. Which join finds the orders whose customer was erased, and why would an inner join hide them?
# 5. What does `.explain()` show that `.collect()` doesn't, and what are projection and predicate pushdown?
#
# Next: [Lab 2.4 · Bronze to silver](../D2_4_bronze_to_silver.ipynb). It parses three timestamp formats with `.str` and
# `.dt`, keeps one copy per event with `unique`, and writes silver Parquet. Docs:
# [user guide](https://docs.pola.rs/user-guide/) ·
# [expressions and contexts](https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/) ·
# [lazy API](https://docs.pola.rs/user-guide/lazy/) ·
# [coming from pandas](https://docs.pola.rs/user-guide/migration/pandas/).
