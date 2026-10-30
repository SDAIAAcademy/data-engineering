# %% [markdown]
# # Dagster, assets on a schedule
#
# Optional, self-paced: goes deeper than Session 3.4.
#
# CPU only. Every lab so far ran its steps by hand. In production a nightly script runs them,
# until it fails halfway, runs twice or lets a bad file through. You'll write that script, watch it break, then
# declare the same pipeline as **Dagster assets**: the tables you want, what each is built from, and the checks it
# must pass.
#
# **The data:** three stores and one orders file per day from the stores app, invented and small enough to check by
# eye. Lab 3.4 orchestrates the real warehouse.
#
# 🤔 asks you to predict before you run a cell · 🔍 says what the output means · ➕ sections are for your own pace.
#
# **Contents**
# - [ ] 1 · The problem: a nightly script
# - [ ] 2 · The mental model: assets, not tasks
# - [ ] 3 · Hello world
# - [ ] 4 · A graph of tables, and running part of it
# - [ ] 5 · Every run is recorded
# - [ ] 6 · Resources: the same code in dev and prod ➕
# - [ ] 7 · Partitions: one day at a time, and backfills
# - [ ] 8 · Asset checks: stop bad data spreading
# - [ ] 9 · When runs start: schedules, sensors, retries
# - [ ] 10 · dbt in the graph
# - [ ] 11 · Summary

# %% [markdown] 🎓
# 🎓 **Instructor notes** (instructor copy only)
#
# - Core ≈ 27': §1 3' · §2 2' · §3 2' · §4 4' · §5 3' (the hand edit ➕) · §7 4' (the append re-run ➕) · §8 4' ·
#   §9 1' (the schedule; the sensor and retries ➕) · §10 3' · §11 1'. §6 is ➕.
# - Sections 1–4 and 8 carry the session: the script breaking, the graph, selection, and a blocking check. §10
#   bridges to Lab 3.4, whose task 2 puts the real dbt project in the graph.
# - In class, run the cells of a ➕ part without stopping on them: later cells may use what they build (the
#   summary's `Definitions` uses §6's resource and §9's sensor).
# - Don't write Lab 3.4's code here: its exercises are the ingestion assets, a blocking check and a volume check. This
#   notebook uses invented data, a negative-amount check, partitions and a sensor.
# - Expect "Airflow?": the most deployed orchestrator. Airflow 3 added assets too, but its unit is still the task;
#   in Dagster the unit is the asset. The ideas here (dependencies, partitions, idempotency, checks) transfer.
# - Everything runs in-process with `dg.materialize`: no server. If there's time, show `dagster dev` on the capstone
#   pipeline (`labs/capstone/`) for the UI: the same graph, runs and checks.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "dagster>=1.10", "dagster-dbt>=0.26",
                    "duckdb>=1.4", "dbt-duckdb>=1.11"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # tools/ and solutions/tools/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import warnings
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")             # Dagster flags some APIs as beta or preview
import dagster as dg
import duckdb
import pandas as pd
from IPython.display import display
import de_utils as U
import de_viz as V

# Rerunning THIS cell starts again from an empty folder (it only deletes this notebook's own outputs).
OUT = U.lab_dir("tools_dagster", reset=True)
DB = OUT / "qafila_toy.duckdb"
INBOX = OUT / "inbox"                         # where the stores app drops one orders file per day
INBOX.mkdir()
print("Output folder:", OUT)


def sql(query, params=None):
    """Run SQL on the toy warehouse. Each call opens and closes its own connection, so dbt can use the file too."""
    with duckdb.connect(str(DB)) as con:
        return con.execute(query, params or []).df()

# %% [markdown]
# Dagster **orchestrates**. It doesn't move or transform data itself: it runs the tools of every stage in the right
# order, at the right time, and records what each one produced.

# %%
V.platform_map(tool="dagster")

# %% [markdown]
# Three stores, and three days of orders, one CSV per day as the stores app sends them. Wednesday's file has a
# problem.

# %%
STORES = pd.DataFrame({"store_id": ["R1", "R2", "J1"], "city": ["Riyadh", "Riyadh", "Jeddah"]})
ORDERS = {
    "2026-09-14": [(101, "R1", 80.50), (102, "R2", 45.00), (103, "J1", 120.00)],
    "2026-09-15": [(104, "R1", 60.00), (105, "J1", 35.25), (106, "J1", 90.00)],
    "2026-09-16": [(107, "R2", -12.00), (108, "R1", 55.00), (109, "J1", 40.00)],
}


def land(day, rows):
    """The stores app drops a day's orders into the inbox."""
    df = pd.DataFrame(rows, columns=["order_id", "store_id", "amount"]).assign(day=day)
    df.to_csv(INBOX / f"orders_{day}.csv", index=False)


for day, rows in ORDERS.items():
    land(day, rows)
print(sorted(p.name for p in INBOX.iterdir()))
pd.read_csv(INBOX / "orders_2026-09-16.csv")

# %% [markdown]
# ## 1 · The problem: a nightly script
#
# Finance reads a revenue-per-city report every morning. The usual first version: three functions, called in order
# by one `nightly(day)` that cron starts at 01:30. Load the day's file, total each store, total each city.

# %%
SCRIPT_DB = OUT / "by_hand.duckdb"
REPORT_TIMES_OUT = False      # set below to simulate a step failing


def run_sql(query):
    with duckdb.connect(str(SCRIPT_DB)) as con:
        return con.execute(query).df()


def load_orders(day):
    run_sql("CREATE TABLE IF NOT EXISTS orders (order_id INT, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")
    run_sql(f"INSERT INTO orders SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")


def build_revenue():
    run_sql("CREATE OR REPLACE TABLE store_revenue AS "
            "SELECT day, store_id, sum(amount) AS revenue FROM orders GROUP BY ALL")


def build_report():
    if REPORT_TIMES_OUT:
        raise TimeoutError("the report step timed out")
    run_sql("CREATE OR REPLACE TABLE city_report AS SELECT day, city, sum(revenue) AS revenue "
            f"FROM store_revenue JOIN read_csv('{(OUT / 'stores.csv').as_posix()}') USING (store_id) "
            "GROUP BY ALL ORDER BY day, city")


def nightly(day):
    load_orders(day)
    build_revenue()
    build_report()


STORES.to_csv(OUT / "stores.csv", index=False)
nightly("2026-09-14")
run_sql("SELECT * FROM city_report")

# %% [markdown]
# Monday works. On Tuesday night the report step times out after the load and the revenue step have run. In the
# morning the on-call engineer runs Tuesday again.
#
# 🤔 **Predict** · What does Tuesday's report say for Jeddah? Its orders were 35.25 and 90.00.

# %%
REPORT_TIMES_OUT = True
try:
    nightly("2026-09-15")
except TimeoutError as e:
    print("❌ 01:30 run:", e)
REPORT_TIMES_OUT = False
nightly("2026-09-15")                         # the morning re-run
run_sql("SELECT * FROM city_report")

# %% [markdown]
# 🔍 Jeddah shows 250.50 on Tuesday, twice the truth: the re-run loaded Tuesday's file a second time. Nothing failed
# loudly; the report is just wrong.
#
# On Wednesday the file has a refund written as an order of -12.00, and it goes straight into the totals:

# %%
nightly("2026-09-16")
run_sql("SELECT * FROM city_report WHERE day = '2026-09-16'")

# %% [markdown]
# 🔍 Riyadh's Wednesday is 43.00, not 55.00, and no step looked. And when was `city_report` last built? From which
# files? Is it older than `store_revenue`? The script doesn't know; the answers live in cron's log and in people's
# heads.

# %% [markdown]
# ## 2 · The mental model: assets, not tasks
#
# ```
#   task-based (cron, Airflow 2)                      asset-based (Dagster)
#
#   load_orders ──► build_revenue ──► build_report    raw/orders ──► store_revenue ──► city_report
#                                                                                     ▲
#   "these steps ran, in this order"                  raw/stores ──────────────────────┘
#                                                     "these tables exist, each built from those,
#                                                      at this time, and their checks passed"
# ```
#
# An **asset** is something you want to exist and keep up to date: a table, a file, a model. You write the function
# that produces it and say what it's built from; running it is a **materialization**. You don't write "run A, then
# B". You write "B is built from A", and Dagster works out the order from the whole graph before running anything.

# %% [markdown]
# ## 3 · Hello world
#
# Two assets. `cities` takes an argument named `stores`, so it is built from the asset `stores`.

# %%
@dg.asset
def stores():
    return dict(zip(STORES["store_id"], STORES["city"]))


@dg.asset
def cities(stores):
    return sorted(set(stores.values()))


QUIET = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}   # keep the notebook output readable
result = dg.materialize([stores, cities], run_config=QUIET)
print(result.success, result.output_for_node("cities"))

# %% [markdown]
# 🔍 `dg.materialize` runs assets in this process: no server, no UI. Dagster ran `stores` first, saved what it
# returned (the **IO manager**'s job, by default a pickle file), and loaded it again to call `cities`. Replay the run
# from its event log:

# %%
V.asset_run([stores, cities], result)

# %% [markdown]
# ## 4 · A graph of tables, and running part of it
#
# Passing values suits small things. In a data pipeline each asset writes a **table** and the next one reads it, so
# Dagster only needs the order. Each asset below runs SQL and returns a `MaterializeResult` with its row count as
# **metadata**. `deps=` says what it is built from. The key `["raw", "orders"]` names the asset `raw/orders`, like the
# table `raw.orders` it writes. `group_name` and `kinds` are labels.

# %%
def rows(table):
    return dg.MaterializeResult(metadata={"rows": int(sql(f"SELECT count(*) AS n FROM {table}")["n"][0])})


@dg.asset(key=["raw", "orders"], group_name="ingest", kinds={"duckdb"})
def raw_orders():
    """Every orders file in the inbox, as sent."""
    sql("CREATE SCHEMA IF NOT EXISTS raw")
    sql(f"""CREATE OR REPLACE TABLE raw.orders AS
            SELECT * FROM read_csv('{(INBOX / 'orders_*.csv').as_posix()}',
                                   columns = {{'order_id': 'INT', 'store_id': 'VARCHAR',
                                               'amount': 'DECIMAL(9,2)', 'day': 'DATE'}})""")
    return rows("raw.orders")


@dg.asset(key=["raw", "stores"], group_name="ingest", kinds={"duckdb"})
def raw_stores():
    """The store list, from the app database."""
    sql("CREATE SCHEMA IF NOT EXISTS raw")
    sql("CREATE OR REPLACE TABLE raw.stores AS SELECT * FROM STORES")
    return rows("raw.stores")


@dg.asset(deps=[raw_orders], group_name="marts", kinds={"duckdb"})
def store_revenue():
    """Revenue per store and day."""
    sql("CREATE OR REPLACE TABLE store_revenue AS "
        "SELECT day, store_id, sum(amount) AS revenue FROM raw.orders GROUP BY ALL")
    return rows("store_revenue")


@dg.asset(deps=[store_revenue, raw_stores], group_name="marts", kinds={"duckdb"})
def city_report():
    """Revenue per city and day."""
    sql("CREATE OR REPLACE TABLE city_report AS SELECT day, city, sum(revenue) AS revenue "
        "FROM store_revenue JOIN raw.stores USING (store_id) GROUP BY ALL ORDER BY day, city")
    return rows("city_report")


@dg.asset(deps=[store_revenue], group_name="marts", kinds={"duckdb"})
def store_ranking():
    """Stores by total revenue."""
    sql("CREATE OR REPLACE TABLE store_ranking AS SELECT store_id, sum(revenue) AS revenue, "
        "rank() OVER (ORDER BY sum(revenue) DESC) AS rank FROM store_revenue GROUP BY ALL")
    return rows("store_ranking")


PIPELINE = [raw_orders, raw_stores, store_revenue, city_report, store_ranking]
V.asset_run(PIPELINE)

# %% [markdown]
# 🔍 Five assets, nothing run yet. The graph came from `deps=` alone.
#
# From here on every run uses one Dagster **instance**: the folder where Dagster keeps its run history (§5).

# %%
HOME = OUT / "dagster_home"
HOME.mkdir()
(HOME / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")   # the instance's settings file
INSTANCE = dg.DagsterInstance.local_temp(tempdir=str(HOME))
RUN = dict(instance=INSTANCE, run_config=QUIET, raise_on_error=False)   # the options every run below shares

result = dg.materialize(PIPELINE, **RUN)
V.asset_run(PIPELINE, result)

# %%
sql("SELECT * FROM city_report")

# %% [markdown]
# 🔍 The script's Monday numbers, and Tuesday is right this time: `raw/orders` is rebuilt from the files on every run
# (`CREATE OR REPLACE`), so running it twice can't load a file twice. Wednesday's -12.00 is still in; that's §8's job.
#
# Operations opens a new store, D1 in Dammam. Only the assets built from `raw/stores` need to run again.
#
# 🤔 **Predict** · Which assets does `AssetSelection.assets(raw_stores).downstream()` select?

# %%
STORES.loc[len(STORES)] = ["D1", "Dammam"]
selection = dg.AssetSelection.assets(raw_stores).downstream()
result = dg.materialize(PIPELINE, selection=selection, **RUN)
V.asset_run(PIPELINE, result, selection=selection)

# %% [markdown]
# 🔍 Two: `raw/stores` and `city_report`. `store_revenue` doesn't read the store list, so it wasn't touched. You ask
# for data to be up to date, and Dagster works out the steps.
#
# | Selection | Selects |
# |---|---|
# | `dg.AssetSelection.assets(a, b)` | those assets |
# | `.downstream()` / `.upstream()` | plus everything built from them / everything they're built from |
# | `dg.AssetSelection.groups("marts")`, `.key_prefixes("raw")` | by label or name |
# | `a \| b`, `a - b` | union, difference |
#
# The other direction: Finance wants `city_report` fresh before a meeting, so we select it and everything it is built
# from.

# %%
selection = dg.AssetSelection.assets(city_report).upstream()
result = dg.materialize(PIPELINE, selection=selection, **RUN)
V.asset_run(PIPELINE, result, selection=selection)

# %% [markdown]
# ## 5 · Every run is recorded
#
# The instance keeps every run and every materialization, with its metadata: what the script couldn't tell us.

# %%
def last_built(keys):
    out = []
    for key in keys:
        ev = INSTANCE.get_latest_materialization_event(key)
        meta = ev.asset_materialization.metadata if ev else {}
        out.append({"asset": key.to_user_string(),
                    "last built": datetime.fromtimestamp(ev.timestamp).strftime("%H:%M:%S.%f")[:-3] if ev else None,
                    "run": ev.run_id[:8] if ev else None, "rows": meta["rows"].value if "rows" in meta else None})
    return pd.DataFrame(out)


KEYS = [k for a in PIPELINE for k in a.keys]
last_built(KEYS)

# %% [markdown]
# 🤔 **Predict** · Finance's refresh rebuilt `raw/orders` and `store_revenue` but not `store_ranking`. Is
# `store_ranking` still up to date?

# %%
graph = dg.Definitions(assets=PIPELINE).resolve_asset_graph()
built_at = {k: INSTANCE.get_latest_materialization_event(k).timestamp for k in KEYS}
for k in KEYS:
    newer = [p.to_user_string() for p in graph.get(k).parent_keys if built_at[p] > built_at[k]]
    print(f"{k.to_user_string():14}", "stale: built before " + ", ".join(newer) if newer else "up to date")

# %% [markdown]
# 🔍 `store_ranking` is older than the `store_revenue` it reads, so it may be out of date. The Dagster UI marks such
# assets, and an automation condition (`dg.AutomationCondition.eager()`) can rebuild an asset whenever its parents
# change. A script can't: it doesn't know what it built.
#
# ➕ **Break it on purpose.** The record is kept in a folder, so it survives a restart. But it only knows about runs
# that went through Dagster. An analyst "fixes" a number by hand:

# %%
sql("UPDATE store_ranking SET revenue = 0 WHERE store_id = 'J1'")
restarted = dg.DagsterInstance.local_temp(tempdir=str(HOME))   # as if Dagster restarted: the same folder
ev = restarted.get_latest_materialization_event(dg.AssetKey("store_ranking"))
print("runs in the record after a restart:", len(restarted.get_run_records()))
print("store_ranking per the record:", ev.asset_materialization.metadata["rows"].value, "rows, built by run", ev.run_id[:8])
sql("SELECT * FROM store_ranking WHERE store_id = 'J1'")

# %% [markdown]
# 🔍 The record says nothing about the hand edit. Dagster's history is only complete if everything that writes these
# tables runs through Dagster. Checks (§8) look at the data itself. Put the table right before moving on:

# %%
result = dg.materialize(PIPELINE, selection=dg.AssetSelection.assets(store_ranking), **RUN)
sql("SELECT * FROM store_ranking")

# %% [markdown]
# ## 6 · Resources: the same code in dev and prod ➕
#
# The assets above write to `DB`, a path fixed in the notebook. In production they write to another warehouse, with
# other credentials. A **resource** is an object Dagster hands to every asset that asks for it by name; which one it
# gets is chosen when you run.

# %%
class Warehouse(dg.ConfigurableResource):
    """A DuckDB file. In production this could be a Snowflake or BigQuery client with the same methods."""
    path: str

    def sql(self, query, params=None):
        with duckdb.connect(self.path) as con:
            return con.execute(query, params or []).df()


@dg.asset(group_name="marts", kinds={"duckdb"})
def order_count(warehouse: Warehouse):
    """Orders per day from the inbox files, written to whichever warehouse the run is given."""
    warehouse.sql("CREATE OR REPLACE TABLE order_count AS SELECT day, count(*) AS orders "
                  f"FROM read_csv('{(INBOX / 'orders_*.csv').as_posix()}') GROUP BY day")
    return dg.MaterializeResult(metadata={"warehouse": Path(warehouse.path).name})


for name in ["dev.duckdb", "prod.duckdb"]:
    r = dg.materialize([order_count], resources={"warehouse": Warehouse(path=str(OUT / name))}, **RUN)
    print(name, "→", r.get_asset_materialization_events()[0].materialization.metadata["warehouse"].value)

# %% [markdown]
# 🔍 One asset, two warehouses, no `if env == "prod"` in its code. A resource is also where secrets belong
# (`dg.EnvVar("WAREHOUSE_TOKEN")`), and it makes tests easy: give the asset a resource pointing at a scratch file.

# %% [markdown]
# ## 7 · Partitions: one day at a time, and backfills
#
# `raw/orders` rebuilds every file on every run: fine for three files, not for three years. A **partitioned** asset
# processes one partition (here one day) per run. A bad day is re-run alone, and history is filled in by a
# **backfill**, one run per partition.

# %%
DAYS = dg.DailyPartitionsDefinition(start_date="2026-09-14", end_date="2026-09-17", timezone="Asia/Riyadh")
print(DAYS.get_partition_keys())
sql("CREATE OR REPLACE TABLE orders_daily (order_id INT, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")


@dg.asset(partitions_def=DAYS, group_name="ingest", kinds={"duckdb"})
def orders_daily(context: dg.AssetExecutionContext):
    """One day's orders file, appended."""
    day = context.partition_key
    sql(f"INSERT INTO orders_daily SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")
    return dg.MaterializeResult(metadata={"rows": int(sql("SELECT count(*) AS n FROM orders_daily WHERE day = ?", [day])["n"][0])})


def backfill(asset, days):
    """One run per partition, one after another: a backfill with concurrency 1."""
    return {day: dg.materialize([asset], partition_key=day, **RUN).success for day in days}


print(backfill(orders_daily, DAYS.get_partition_keys()))
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Three runs, each with `context.partition_key` set to its day. The instance records which partitions are
# materialized:

# %%
INSTANCE.get_materialized_partitions(dg.AssetKey("orders_daily"))

# %% [markdown]
# ➕ **Break it on purpose.** Retries and backfills are normal: a run times out, someone re-runs Tuesday.
#
# 🤔 **Predict** · We re-run the partition `2026-09-15`. How many orders does Tuesday have afterwards?

# %%
backfill(orders_daily, ["2026-09-15"])
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Six: §1's bug, now inside Dagster. Dagster runs what you ask; it can't know the asset appends. A partitioned
# asset must be **idempotent**: running a partition twice leaves the same table as running it once.
#
# Three common ways: delete the partition, then insert it; `MERGE` / upsert on a key; overwrite the partition's file.
# Here, an upsert: `order_id` becomes the primary key and `INSERT OR REPLACE` replaces a row that is already there.

# %%
sql("CREATE OR REPLACE TABLE orders_daily (order_id INT PRIMARY KEY, store_id VARCHAR, amount DECIMAL(9,2), day DATE)")


@dg.asset(partitions_def=DAYS, group_name="ingest", kinds={"duckdb"})
def orders_daily(context: dg.AssetExecutionContext):
    """One day's orders file, upserted by order_id."""
    day = context.partition_key
    sql(f"INSERT OR REPLACE INTO orders_daily SELECT * FROM read_csv('{(INBOX / f'orders_{day}.csv').as_posix()}')")
    return dg.MaterializeResult(metadata={"rows": int(sql("SELECT count(*) AS n FROM orders_daily WHERE day = ?", [day])["n"][0])})


print(backfill(orders_daily, DAYS.get_partition_keys()))
print(backfill(orders_daily, ["2026-09-15", "2026-09-15"]))
sql("SELECT day, count(*) AS orders FROM orders_daily GROUP BY day ORDER BY day")

# %% [markdown]
# 🔍 Three a day, however often Tuesday runs. One catch: if the stores app re-sends Tuesday **without** order 106
# (cancelled), the upsert keeps the old 106, because nothing deletes it. Delete-then-insert of the whole partition
# doesn't have that problem. Which one fits depends on what a re-sent file means.
#
# Partition loads by the day the data **arrived** (the file's day), so a re-run reads the same file; partition
# analytics by the day the event **happened**.

# %% [markdown] 🎓
# 🎓 Ask: "a backfill of 365 days with concurrency 10: what else must be true of the asset?" (Partitions mustn't step
# on each other: each run touches only its day. A `CREATE OR REPLACE` of the whole table in a partitioned asset is the
# classic bug.)

# %% [markdown]
# ## 8 · Asset checks: stop bad data spreading
#
# An **asset check** looks at an asset's data after it is built and returns pass or fail, with metadata. Here: no
# order may have a negative amount.

# %%
@dg.asset_check(asset=raw_orders, description="An order's amount is never negative")
def no_negative_amounts():
    bad = sql("SELECT order_id, amount FROM raw.orders WHERE amount < 0")
    return dg.AssetCheckResult(passed=bad.empty, metadata={"bad_orders": bad["order_id"].tolist()})


result = dg.materialize(PIPELINE + [no_negative_amounts], **RUN)
V.asset_run(PIPELINE + [no_negative_amounts], result)

# %% [markdown]
# 🔍 The check found order 107 and the failure is on record. But the run **succeeded**, and the three marts were built
# with the -12.00 in them. These numbers go to Finance, so make the check **blocking**: if it fails, nothing
# downstream of its asset runs.

# %%
@dg.asset_check(asset=raw_orders, blocking=True, description="An order's amount is never negative")
def no_negative_amounts_blocking():
    bad = sql("SELECT order_id, amount FROM raw.orders WHERE amount < 0")
    return dg.AssetCheckResult(passed=bad.empty, metadata={"bad_orders": bad["order_id"].tolist()})


result = dg.materialize(PIPELINE + [no_negative_amounts_blocking], **RUN)
built = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
U.check(not result.success, "the run failed: a blocking check did not pass")
U.check(built == {"raw/orders", "raw/stores"}, "only the two raw assets were materialized")
V.asset_run(PIPELINE + [no_negative_amounts_blocking], result)

# %% [markdown]
# 🔍 The marts were **skipped**, so they keep what they held before: the -12.00 from the non-blocking run above. A
# blocking check stops bad data from spreading **from now on**; it doesn't undo what an earlier run built.
#
# When a check is blocking, and when it only warns:
#
# | Blocking | Warn only |
# |---|---|
# | the numbers go to finance, customers or a model | a human should look, but the data is still useful |
# | wrong data downstream costs more than late data | late data costs more than slightly wrong data |
# | e.g. negative amounts, duplicate keys, an empty load | e.g. a null rate a bit above normal, a new category |
#
# The fix belongs at the source: the stores app re-sends Wednesday with the refund taken out (refunds come from the
# payments feed, not the orders file). Run again:

# %%
land("2026-09-16", [r for r in ORDERS["2026-09-16"] if r[2] > 0])
result = dg.materialize(PIPELINE + [no_negative_amounts_blocking], **RUN)
print("run succeeded:", result.success, "· checks:",
      [(c.check_name, c.passed) for c in result.get_asset_check_evaluations()])
sql("SELECT * FROM city_report WHERE day = '2026-09-16'")

# %% [markdown]
# 🔍 Riyadh's Wednesday is 55.00 again.

# %% [markdown] 🎓
# 🎓 Ask: "the check is blocking, the data is wrong, the CEO's dashboard is due at 08:00. Who gets woken, and what do
# they see?" The check alone doesn't answer that: someone must own the alert and decide what the dashboard says when its data is late.

# %% [markdown]
# ## 9 · When runs start: schedules, sensors, retries
#
# Until now we called `materialize` by hand. A deployed Dagster has a **daemon** that starts runs on a **schedule**
# (a clock) or from a **sensor** (something happened), while a **retry policy** decides what happens when a step
# fails. Each can be tested by evaluating one "tick" yourself.
#
# First a schedule for the partitioned load: every night at 01:30, Riyadh time (the timezone of `DAYS`), load
# **yesterday's** partition.

# %%
daily_job = dg.define_asset_job("daily_orders", selection=[orders_daily], partitions_def=DAYS)
nightly_schedule = dg.build_schedule_from_partitioned_job(daily_job, hour_of_day=1, minute_of_hour=30)
print("cron:", nightly_schedule.cron_schedule, nightly_schedule.execution_timezone)

tick_time = datetime(2026, 9, 16, 1, 30, tzinfo=ZoneInfo("Asia/Riyadh"))
tick = nightly_schedule.evaluate_tick(dg.build_schedule_context(scheduled_execution_time=tick_time))
print("tick at", tick_time, "→ runs partition", [r.partition_key for r in tick.run_requests])

# %% [markdown]
# 🔍 The 01:30 tick on Wednesday asks for Tuesday's partition, the day that just ended. A missed night can be re-run
# as that partition, not as "today".
#
# ➕ A file doesn't always arrive at 01:30. A **sensor** is a function the daemon calls every 30 seconds or so; it
# returns the runs to start. Between ticks it keeps a **cursor**, a string it writes and gets back next time. This one
# requests one run per file whose name sorts after the cursor (file names sort by date).

# %%
pipeline_job = dg.define_asset_job("pipeline", selection=dg.AssetSelection.assets(*PIPELINE))


@dg.sensor(job=pipeline_job, minimum_interval_seconds=30)
def new_orders_files(context: dg.SensorEvaluationContext):
    seen = context.cursor or ""                               # the last file name already handled
    new = sorted(p.name for p in INBOX.glob("orders_*.csv") if p.name > seen)
    for name in new:
        yield dg.RunRequest(run_key=name)
    if new:
        context.update_cursor(new[-1])


def sensor_tick(cursor):
    out = new_orders_files.evaluate_tick(dg.build_sensor_context(instance=INSTANCE, cursor=cursor))
    print(f"cursor {cursor!r:28} → runs {[r.run_key for r in out.run_requests]}, new cursor {out.cursor!r}")
    return out


tick1 = sensor_tick(None)
tick2 = sensor_tick(tick1.cursor)
land("2026-09-17", [(110, "R1", 70.00), (111, "J1", 25.50)])
tick3 = sensor_tick(tick2.cursor)

# %% [markdown]
# 🔍 Without the cursor every tick would see three files and ask for three runs again. The `run_key` is a second
# safety net: the daemon never starts two runs with the same key, so even a sensor that forgets its cursor can't
# process a file twice.
#
# ➕ Some failures are worth retrying (an API's 429 "too many requests", a timeout), most are not (a bug, a bad file).
# A **retry policy** on an asset re-runs its step, with a delay:

# %%
attempts = []


@dg.asset(retry_policy=dg.RetryPolicy(max_retries=3, delay=0.5))
def payments_page():
    """Calls a payments API that answers the first two requests with 429."""
    attempts.append(datetime.now().strftime("%H:%M:%S"))
    if len(attempts) < 3:
        raise ConnectionError("HTTP 429 Too Many Requests")
    return dg.MaterializeResult(metadata={"attempts": len(attempts)})


result = dg.materialize([payments_page], **RUN)
V.asset_run([payments_page], result)

# %% [markdown]
# 🔍 Two failures, two retries, then success, all inside one run. Retries hide flaky failures, so keep them for errors
# that are flaky by nature.

# %% [markdown]
# ## 10 · dbt in the graph
#
# Session 3.2's transformations are dbt models, and dbt has its own graph. `dagster-dbt` reads dbt's `manifest.json` and
# makes **every model an asset** and **every dbt test an asset check**. A dbt source becomes a dependency on the
# asset with the same key, so the Python assets above and the SQL models join into one graph.
#
# A two-model dbt project on the toy warehouse:

# %%
DBT_TOY = OUT / "dbt_toy"
(DBT_TOY / "models").mkdir(parents=True)
(DBT_TOY / "dbt_project.yml").write_text(
    "name: toy\nprofile: toy\nmodels:\n  toy:\n    +materialized: table\n", encoding="utf-8")
(DBT_TOY / "profiles.yml").write_text(
    "toy:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
    "      path: \"{{ env_var('QAFILA_DUCKDB') }}\"\n      schema: marts\n", encoding="utf-8")
(DBT_TOY / "models" / "schema.yml").write_text("""
sources:
  - name: raw
    tables: [{name: orders}, {name: stores}]
models:
  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
  - name: fct_city_sales
    columns:
      - name: city
        data_tests: [not_null]
""", encoding="utf-8")
(DBT_TOY / "models" / "stg_orders.sql").write_text(
    "select order_id, store_id, amount, day from {{ source('raw', 'orders') }} where amount > 0\n", encoding="utf-8")
(DBT_TOY / "models" / "fct_city_sales.sql").write_text(
    "select o.day, s.city, sum(o.amount) as revenue, count(*) as orders\n"
    "from {{ ref('stg_orders') }} o join {{ source('raw', 'stores') }} s using (store_id)\n"
    "group by all\n", encoding="utf-8")
from dagster_dbt import DbtCliResource, dbt_assets

os.environ["QAFILA_DUCKDB"] = DB.resolve().as_posix()          # profiles.yml reads the warehouse path from here
dbt = DbtCliResource(project_dir=str(DBT_TOY), profiles_dir=str(DBT_TOY))
dbt.cli(["parse"], target_path=Path("target")).wait()           # dbt reads the project, writes target/manifest.json
print(sorted(p.name for p in (DBT_TOY / "target").iterdir() if p.suffix == ".json"))

# %%
@dbt_assets(manifest=DBT_TOY / "target" / "manifest.json")
def toy_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


print("assets:", sorted(k.to_user_string() for k in toy_dbt.keys))
print("checks:", sorted(c.name for c in toy_dbt.check_keys))

# %% [markdown]
# 🤔 **Predict** · The dbt sources are `raw.orders` and `raw.stores`. In the combined graph, what do the dbt models
# hang off?

# %%
WITH_DBT = [raw_orders, raw_stores, toy_dbt]
result = dg.materialize(WITH_DBT, resources={"dbt": dbt}, **RUN)
V.asset_run(WITH_DBT, result)

# %% [markdown]
# 🔍 The Python assets `raw/orders` and `raw/stores`: dbt's `source('raw', 'orders')` became the key `raw/orders`. One
# graph, from the file in the inbox to the dbt mart. dbt's three tests ran as checks right after their models. They
# are **blocking** (thick outline): a dbt test with the default severity `error` becomes a blocking check, one with
# `severity: warn` doesn't. Selecting `raw_orders` and its downstream would run the loader, then `dbt build --select`
# only the models that depend on it.

# %%
sql("SELECT * FROM marts.fct_city_sales ORDER BY day, city")

# %% [markdown]
# The real Qafila dbt project from Lab 3.2 is read the same way. Lab 3.4 puts it in a graph with the courier events
# loader.

# %%
qafila = DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))
qafila.cli(["parse"], target_path=Path("target")).wait()        # parse reads the SQL only: no database is opened


@dbt_assets(manifest=U.DBT_PROJECT / "target" / "manifest.json")
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


print(len(qafila_dbt.keys), "assets and", len(qafila_dbt.check_keys), "checks from the Qafila dbt project")
V.asset_run([qafila_dbt])

# %% [markdown]
# 🔍 Every model is an asset and every test a check, with no Dagster code per model. The `raw/...` boxes on the left
# are the dbt sources: external here, because no asset in this graph builds them. The checks are grouped one pill per
# model; a thin outline marks a pill holding one of the project's three `severity: warn` tests, the only non-blocking
# ones.

# %% [markdown]
# ## 11 · Summary
#
# | | |
# |---|---|
# | **It does** | a graph of assets from the code · runs a selection in order · records every materialization with metadata · resources for dev and prod · partitions and backfills · asset checks, blocking or not · schedules, sensors, retries · dbt models and tests in the same graph · a UI for all of it |
# | **It doesn't** | move or transform data itself (your SQL, dlt, dbt, Polars do) · make an asset idempotent (you do, §7) · know about changes made outside it (§5) |
# | **In this course** | Lab 3.4 orchestrates the real warehouse: a loader and the Qafila dbt project in one graph, a blocking check and a schedule · the capstone's `pipeline.py`, one code location from the sources to the gold table |
# | **As a service** | Dagster+ · Airflow as a service: Astronomer, Amazon MWAA, Google Cloud Composer · built into platforms: Databricks Workflows, Snowflake tasks, dbt platform jobs, Fabric pipelines |
#
# **Cheat sheet**
#
# | Code | Does |
# |---|---|
# | `@dg.asset(key=[...], deps=[...], group_name=..., kinds={...})` | an asset; `deps` are what it's built from |
# | `def b(a): ...` | `b` is built from `a`, and gets `a`'s value (IO manager) |
# | `return dg.MaterializeResult(metadata={...})` | record facts about this materialization |
# | `dg.materialize(assets, selection=..., partition_key=..., resources=...)` | run in this process |
# | `dg.AssetSelection.assets(a).downstream()` / `.upstream()` | a and what's built from it / what it's built from |
# | `instance.get_latest_materialization_event(key)` | when, by which run, with what metadata |
# | `class R(dg.ConfigurableResource)` + an argument `r: R` | a resource, chosen per run; secrets via `dg.EnvVar(...)` |
# | `dg.DailyPartitionsDefinition(start_date=...)` + `context.partition_key` | one run per day; make it idempotent |
# | `@dg.asset_check(asset=a, blocking=True)` → `dg.AssetCheckResult(passed=...)` | a check; blocking stops downstream |
# | `dg.define_asset_job(name, selection=...)` | a named selection, for schedules and sensors |
# | `dg.build_schedule_from_partitioned_job(job, hour_of_day=1)` | nightly run of yesterday's partition |
# | `@dg.sensor(job=...)`, `context.cursor`, `dg.RunRequest(run_key=...)` | runs when something happens, at most once per key |
# | `dg.RetryPolicy(max_retries=3, delay=1, backoff=dg.Backoff.EXPONENTIAL)` | retry a failed step, waiting longer each time |
# | `@dbt_assets(manifest=...)` + `dbt.cli(["build"], context=context).stream()` | dbt models as assets, tests as checks |
# | `dg.Definitions(assets=..., asset_checks=..., jobs=..., schedules=..., sensors=..., resources=...)` | one code location; `dagster dev` serves the UI on it |
#
# ## Checkpoint
#
# 1. The script's morning re-run doubled Tuesday. Which two things in §4 and §7 stop that from happening?
# 2. `store_ranking` was stale in §5. How did Dagster know, and why couldn't the script?
# 3. A blocking check failed. What do the skipped assets contain now?
# 4. The supplier files arrive "sometime on Monday". Schedule or sensor? And what makes the sensor safe to tick every
#    30 seconds?
#
# Next: [Lab 3.4 · Orchestrating Qafila with Dagster](../D3_4_orchestration_dagster.ipynb). Docs:
# [assets](https://docs.dagster.io/guides/build/assets) ·
# [partitions](https://docs.dagster.io/guides/build/partitions-and-backfills) ·
# [asset checks](https://docs.dagster.io/guides/test/asset-checks) ·
# [schedules](https://docs.dagster.io/guides/automate/schedules) ·
# [sensors](https://docs.dagster.io/guides/automate/sensors) ·
# [dbt](https://docs.dagster.io/integrations/libraries/dbt).
#
# **What you built:** everything above in one `dg.Definitions`, the code location that `dagster dev` serves (the
# capstone has one: `uv run dagster dev -f capstone/pipeline.py`). Then the pipeline with dbt in it: the inbox loader
# and the store list in Python, the models and their tests from dbt, one graph and one run.

# %%
defs = dg.Definitions(
    assets=PIPELINE + [orders_daily, toy_dbt],
    asset_checks=[no_negative_amounts_blocking],
    jobs=[daily_job, pipeline_job],
    schedules=[nightly_schedule],
    sensors=[new_orders_files],
    resources={"dbt": dbt, "warehouse": Warehouse(path=str(OUT / "dev.duckdb"))},
)
print(len(defs.resolve_asset_graph().get_all_asset_keys()), "assets in this code location")

# %%
V.asset_run(WITH_DBT + [no_negative_amounts_blocking],
            dg.materialize(WITH_DBT + [no_negative_amounts_blocking], resources={"dbt": dbt}, **RUN))
