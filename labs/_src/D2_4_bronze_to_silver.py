# %% [markdown]
# # Lab 2.4 · Bronze to silver with Polars
#
# Day 2 · Session 4 · 25 min
#
# Bronze keeps the couriers' delivery events as they arrived: three timestamp formats, and repeated rows. Silver is the
# same data made trustworthy. You'll clean it with **Polars**, a fast DataFrame library for Python. What you'll do:
# - use Polars expressions, and check the answer against SQL;
# - parse three timestamp formats into one UTC type, remove duplicates, add Riyadh time;
# - write silver as Parquet, and read it back with another engine.
#
# **Tasks**
# - [ ] 1 · Meet Polars: expressions on the bronze events (5')
# - [ ] 2 · Parse three timestamp formats into UTC (6')
# - [ ] 3 · Deduplicate and convert to Riyadh time (6')
# - [ ] 4 · Measure how late events arrive (3')
# - [ ] 5 · Write silver Parquet and read it back (5')
# - [ ] ✱ Stretch · Clean the customers' mobile numbers to one format

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
import sqlite3

import duckdb
import polars as pl

pl.Config.set_tbl_rows(8)
pl.Config.set_fmt_str_lengths(40)
OUT = U.lab_dir("D2_4_silver", reset=True)
EVENTS = (U.RAW / "events" / "delivery_events.jsonl").as_posix()

# %% [markdown]
# ## 1 · Meet Polars: expressions on the bronze events (5')
#
# `scan_ndjson` doesn't read anything yet: it returns a **LazyFrame**, a plan. You stack steps on it, and `.collect()`
# runs them all at once, so Polars can skip what you don't need.

# %%
bronze = pl.scan_ndjson(EVENTS)
bronze.select("event_id", "order_id", "event_type", "event_time", "ingest_time").head(4).collect()

# %% [markdown]
# `pl.col("city")` is an **expression**: a recipe for a column, computed only when a method runs it. Four methods run
# them, and they cover most days of work:
#
# | Method | Does | SQL you know |
# |---|---|---|
# | `select` | keep or compute columns | `SELECT` |
# | `filter` | keep rows | `WHERE` |
# | `with_columns` | add or replace columns, keep the rest | `SELECT *, ... AS x` |
# | `group_by(...).agg(...)` | one row per group | `GROUP BY` |
#
# One question, in both languages: how many deliveries per city?

# %%
in_polars = (
    bronze.filter(pl.col("event_type") == "delivered")
          .group_by("city")
          .agg(pl.len().alias("rows"), pl.col("order_id").n_unique().alias("orders"))
          .sort("city")
          .collect()
)
in_sql = duckdb.sql(f"""
    SELECT city, count(*) AS rows, count(DISTINCT order_id) AS orders
    FROM read_json_auto('{EVENTS}')
    WHERE event_type = 'delivered'
    GROUP BY city ORDER BY city
""").pl()
U.check(in_polars.equals(in_sql.cast({"rows": pl.UInt32, "orders": pl.UInt32})), "Polars and DuckDB agree")
in_polars

# %% [markdown]
# 🔍 Same answer, two interfaces. Compare `rows` and `orders`: a delivered order should have one delivered event. Why are
# they different? The next two tasks answer that.
#
# And look at the plan Polars made for this query. Which columns does it actually read from the file?

# %%
print(bronze.filter(pl.col("event_type") == "delivered").group_by("city").agg(pl.len()).explain())

# %% [markdown]
# ## 2 · Parse three timestamp formats into UTC (6')
#
# `event_time` is text, and the couriers' phones send it three ways. Count them by shape:

# %%
shapes = bronze.with_columns(
    pl.col("event_time").str.replace_all(r"\d", "9").alias("shape")
).group_by("shape").agg(pl.len().alias("events"), pl.col("event_time").first().alias("example")).sort("events", descending=True).collect()
shapes

# %% [markdown]
# A **time zone** matters here: `...T01:54:55+03:00` and `...T22:54:55Z` are the same moment. **UTC** (the `Z`) is the
# world's reference clock: store one moment one way, and convert only for display.
#
# The three recipes are given below as expressions: `is_epoch` (13 digits, milliseconds since 1970), `from_epoch` and `from_iso`.
#
# ✏️ **Exercise** · Complete `utc_expression`: if `is_epoch`, use `from_epoch`, otherwise `from_iso`.
#
# Hint: `pl.when(condition).then(a).otherwise(b)`, one line.

# %%
et = pl.col("event_time")
is_epoch = et.str.contains(r"^\d{13}$")
from_epoch = pl.from_epoch(et.cast(pl.Int64, strict=False), time_unit="ms").dt.replace_time_zone("UTC")
from_iso = et.str.to_datetime(time_zone="UTC", strict=False)      # converts any offset to UTC


def utc_expression():
    # BEGIN SOLUTION
    return pl.when(is_epoch).then(from_epoch).otherwise(from_iso)
    # END SOLUTION

# %%
try:
    event_at_utc = utc_expression()
except NotImplementedError:
    print("✏️ Not done yet: using the reference expression so the rest of the lab runs.")
    event_at_utc = pl.when(is_epoch).then(from_epoch).otherwise(from_iso)
parsed = bronze.with_columns(
    event_at_utc.alias("event_at_utc"),
    pl.col("ingest_time").str.to_datetime(time_zone="UTC").alias("ingested_at_utc"),
)
sample = parsed.select("event_time", "event_at_utc").collect()
print("nulls:", sample["event_at_utc"].null_count(), "(expected: 0) | type:", sample["event_at_utc"].dtype)
U.check(sample["event_at_utc"].null_count() == 0, "every event_time parsed")
sample.with_columns(et.str.replace_all(r"\d", "9").alias("shape")).group_by("shape").agg(et.first(), pl.col("event_at_utc").first())

# %% [markdown]
# ## 3 · Deduplicate and convert to Riyadh time (6')
#
# Phones retry when the network drops, so some events arrived twice, with the same `event_id`. Keep the **first-ingested**
# copy. Then add the time Riyadh's operations team reads.
#
# ✏️ **Exercise** · Complete `clean`: sort by `ingested_at_utc`, keep the first row per `event_id`, and add `event_at_riyadh`.
#
# Hint: `.sort(...)`, `.unique(subset="event_id", keep="first", maintain_order=True)`,
# `.with_columns(pl.col("event_at_utc").dt.convert_time_zone("Asia/Riyadh").alias("event_at_riyadh"))`.

# %%
def clean(frame):
    # BEGIN SOLUTION
    return (frame.sort("ingested_at_utc")
                 .unique(subset="event_id", keep="first", maintain_order=True)
                 .with_columns(pl.col("event_at_utc").dt.convert_time_zone("Asia/Riyadh").alias("event_at_riyadh")))
    # END SOLUTION

# %%
try:
    cleaned = clean(parsed)
except NotImplementedError:
    print("✏️ Not done yet: using the reference chain so the rest of the lab runs.")
    cleaned = (parsed.sort("ingested_at_utc")
                     .unique(subset="event_id", keep="first", maintain_order=True)
                     .with_columns(pl.col("event_at_utc").dt.convert_time_zone("Asia/Riyadh").alias("event_at_riyadh")))
ev = cleaned.collect()
n_bronze = bronze.select(pl.len()).collect().item()
print(f"bronze {n_bronze:,} rows -> silver {ev.height:,} rows ({n_bronze - ev.height} duplicates dropped; expected: 22,518 -> 22,206)")
U.check(ev["event_id"].n_unique() == ev.height == 22206, "event_id is unique in silver")
ev.filter(pl.col("order_id") == 5043883).select("event_type", "event_at_utc", "event_at_riyadh").sort("event_at_utc")

# %% [markdown]
# 🔍 Khalid's order 5043883: the rider delivered it at 01:54 on 28 September in Riyadh, which is 22:54 on the **27th** in
# UTC. Which date would a daily report use? Decide on purpose. We keep both columns, and bronze keeps the original text.
#
# ## 4 · Measure how late events arrive (3')
#
# `ingest_time` is when the event reached us. Run this and read it: most arrive within seconds, and a few much later,
# when a courier's phone was offline. Nothing to write.

# %%
ev = ev.with_columns((pl.col("ingested_at_utc") - pl.col("event_at_utc")).dt.total_seconds().alias("ingest_delay_s"))
ev.select(
    (pl.col("ingest_delay_s") <= 10).mean().round(3).alias("share_within_10s"),
    (pl.col("ingest_delay_s") > 300).sum().alias("later_than_5min"),
    pl.col("ingest_delay_s").max().alias("max_delay_s"),
)

# %% [markdown]
# 🔍 Those late arrivals are why a report on "yesterday" can change tomorrow. Streaming on Day 4 deals with them head on.
#
# ## 5 · Write silver Parquet and read it back (5')
#
# Silver has clean names and only the columns people need.
#
# ✏️ **Exercise** · Complete `write_silver`: write `silver` to `path` as Parquet with `zstd` compression.
#
# Hint: `frame.write_parquet(path, compression="zstd")`, one line.

# %%
silver = ev.select("event_id", "order_id", "store_id", "city", "courier_id", "event_type",
                   "event_at_utc", "event_at_riyadh", "ingested_at_utc", "ingest_delay_s")
(OUT / "silver").mkdir(exist_ok=True)
SILVER = OUT / "silver" / "delivery_events.parquet"


def write_silver(frame, path):
    # BEGIN SOLUTION
    frame.write_parquet(path, compression="zstd")
    # END SOLUTION

# %%
try:
    write_silver(silver, SILVER)
except NotImplementedError:
    print("✏️ Not done yet: using the reference write so the rest of the lab runs.")
    silver.write_parquet(SILVER, compression="zstd")
bronze_mb, silver_mb = U.size_mb(EVENTS), U.size_mb(SILVER)
print(f"bronze file {bronze_mb:.2f} MB | silver file {silver_mb:.2f} MB, {silver_mb / bronze_mb:.0%} of the size")
rows, ids = duckdb.sql(f"SELECT count(*), count(DISTINCT event_id) FROM '{SILVER.as_posix()}'").fetchall()[0]
U.check(rows == ids == ev.height, "silver is readable by another engine, and still unique")
duckdb.sql(f"""
    SELECT city, count(*) AS deliveries, count(DISTINCT order_id) AS orders
    FROM '{SILVER.as_posix()}' WHERE event_type = 'delivered' GROUP BY city ORDER BY city
""").df()

# %% [markdown]
# 🔍 Now `deliveries` equals `orders`: the question from Task 1 is answered. The duplicates were inflating the count.
# Written by Polars, read by DuckDB: Parquet is the handshake between engines.
#
# ## ✱ Stretch · Clean the customers' mobile numbers to one format
#
# The app holds mobiles in several shapes: `0596390578`, `966596390578`, `+966 59 639 0578`. Complete `mobile_e164`: an
# expression that keeps only the digits, and if they match `(966|0)?5` + 8 digits, gives `+966` + the last 9 digits (the
# international E.164 format), otherwise `null`.

# %%
app = sqlite3.connect(U.RAW / "qafila_oltp.sqlite")
customers = pl.read_database("SELECT customer_id, mobile FROM customers", app)


def mobile_e164():
    # BEGIN SOLUTION
    digits = pl.col("mobile").str.replace_all(r"[^0-9]", "")
    return (pl.when(digits.str.contains(r"^(966|0)?5\d{8}$"))
              .then(pl.lit("+966") + digits.str.slice(-9))
              .otherwise(None))
    # END SOLUTION

# %%
try:
    clean_mobiles = customers.with_columns(mobile_e164().alias("mobile_e164"))
    print("not recognised:", clean_mobiles["mobile_e164"].null_count(), "of", clean_mobiles.height)
    U.check(clean_mobiles["mobile_e164"].drop_nulls().str.contains(r"^\+9665\d{8}$").all(), "all mobiles in E.164")
    display(clean_mobiles.group_by(pl.col("mobile").str.replace_all(r"\d", "9").alias("shape")).agg(pl.len(), pl.col("mobile_e164").first()))
except NotImplementedError:
    print("✱ Stretch not attempted: that's fine.")

# %% [markdown]
# ## Checkpoint
#
# **1.** What does `scan_ndjson` do that `read_ndjson` doesn't?
#
# <details><summary>Answer</summary>It reads nothing yet. It returns a plan (a LazyFrame), and <code>.collect()</code> runs
# it, so Polars can read only the columns and rows the whole query needs.</details>
#
# **2.** Riyadh has no daylight saving time. Why store UTC and convert, instead of just storing Riyadh time?
#
# <details><summary>Answer</summary>Other systems, cities and tools work in UTC, and one moment should have one stored value.
# Convert at the edge, for people: and keep the local column when a report needs local dates.</details>
#
# **3.** A cleaning rule turns out to be wrong a month later. What saves you?
#
# <details><summary>Answer</summary>Bronze still has the raw text. Fix the rule and rebuild silver from bronze. That's why
# you never clean in place.</details>
#
# ## Carry forward
#
# Bronze is clean into silver, and written as Parquet. Tomorrow you model it for the business, test it and put it on a
# schedule. Want more? `tools/polars.ipynb`.
