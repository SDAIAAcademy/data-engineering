# %% [markdown]
# # Lab 3.4 · Orchestrating Qafila with Dagster
#
# Day 3 · Session 4 · 25 min. So far you ran each step by hand, in the right order. An **orchestrator** does that for you: on
# a schedule, in dependency order, with a record of every run. **Dagster** models a pipeline as **assets**: the tables you
# want to exist. Everything runs inside this notebook, with no server. What you'll do:
# - write assets and put the dbt project in the same graph;
# - schedule the graph to run every night;
# - add a blocking check that stops bad data, and a volume check that warns about a quiet day.
#
# **Tasks**
# - [ ] 1 · Write ingestion assets (5')
# - [ ] 2 · Put dbt in the graph (6')
# - [ ] 3 · Schedule the graph (3')
# - [ ] 4 · Stop bad data with a blocking check (6')
# - [ ] 5 · Add a volume check (5')
# - [ ] ✱ Stretch · Retry a flaky API

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
import contextlib
import io
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")             # Dagster flags some APIs as beta/preview
import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
import de_viz as V

LAB = U.lab_dir("D3_4", reset=True)
PROJ, DB = U.dbt_lab_copy(LAB)                # a private copy of the dbt project (the Lab 3.2 star) and of the raw tables
os.environ["QAFILA_DUCKDB"] = DB.resolve().as_posix()   # profiles.yml reads the warehouse path from here
dbt = DbtCliResource(project_dir=str(PROJ), profiles_dir=str(PROJ))
dbt.cli(["build", "--quiet"], target_path=Path("target")).wait()   # builds the marts and writes target/manifest.json
QUIET = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}   # keep notebook output readable


def sql(query):
    """Run SQL on the lab warehouse with a short-lived connection (dbt needs the file too)."""
    con = U.connect(DB)
    try:
        return con.execute(query).fetchall()
    finally:
        con.close()


sql("CREATE SCHEMA IF NOT EXISTS ops")
print("ready:", sql("SELECT count(*) FROM marts.fct_orders")[0][0], "orders in the star")

# %% [markdown]
# ## 1 · Write ingestion assets (5')
#
# An **asset** is a table (or file) you want to exist, plus the function that builds it. `key` is its name, and `deps` says
# what it's built from. `dg.materialize` runs it and records the run; **materialize** just means "build it now". Here's the
# loader for the courier events:

# %%
EVENTS = (U.RAW / "events" / "delivery_events.jsonl").as_posix()


@dg.asset(key=["raw", "delivery_events"], group_name="ingest")
def raw_delivery_events():
    """Courier app events, loaded as they are."""
    sql(f"""CREATE OR REPLACE TABLE raw.delivery_events AS
            SELECT * FROM read_json('{EVENTS}', format = 'newline_delimited', columns = {{
              event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR', courier_id: 'VARCHAR',
              event_type: 'VARCHAR', event_time: 'VARCHAR', lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})""")
    return dg.MaterializeResult(metadata={"rows": sql("SELECT count(*) FROM raw.delivery_events")[0][0]})

# %% [markdown]
# ✏️ **Exercise** · Write the SQL for a second loader: replace `raw.reviews` with everything in `U.RAW / "reviews.csv"`.
#
# *Hint:* `CREATE OR REPLACE TABLE raw.reviews AS SELECT * FROM read_csv('...')`, with the `src` path below.

# %%
def load_reviews_sql():
    src = (U.RAW / "reviews.csv").as_posix()
    # BEGIN SOLUTION
    return f"CREATE OR REPLACE TABLE raw.reviews AS SELECT * FROM read_csv('{src}')"
    # END SOLUTION

# %%
try:
    REVIEWS_SQL = load_reviews_sql()
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    REVIEWS_SQL = f"CREATE OR REPLACE TABLE raw.reviews AS SELECT * FROM read_csv('{(U.RAW / 'reviews.csv').as_posix()}')"


@dg.asset(key=["raw", "reviews"], group_name="ingest")
def raw_reviews():
    """Customer reviews export."""
    sql(REVIEWS_SQL)
    return dg.MaterializeResult(metadata={"rows": sql("SELECT count(*) FROM raw.reviews")[0][0]})


result = dg.materialize([raw_delivery_events, raw_reviews], run_config=QUIET)
print("expected: 22,518 events and 3,000 reviews")
for ev in result.get_asset_materialization_events():
    m = ev.event_specific_data.materialization
    print(m.asset_key.to_user_string(), {k: v.value for k, v in m.metadata.items()})
U.check(result.success and sql("SELECT count(*) FROM raw.reviews")[0][0] == 3_000, "both assets materialized")

# %% [markdown]
# ## 2 · Put dbt in the graph (6')
#
# `dagster-dbt` reads dbt's `target/manifest.json` and turns **every model into an asset** and **every test into a check**, with
# no Dagster code per model. A dbt source becomes a dependency on the asset with the same key, so `raw/delivery_events` above
# connects to the models that read it. One graph, from file to mart.

# %%
@dbt_assets(manifest=PROJ / "target" / "manifest.json")
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


ALL = [raw_delivery_events, raw_reviews, qafila_dbt]
print(len(qafila_dbt.keys), "assets and", len(qafila_dbt.check_keys), "checks from the dbt project")
V.asset_run(ALL)

# %% [markdown]
# New courier events just landed. You don't want to rerun everything: you want `raw/delivery_events` and whatever reads it.
#
# 🤔 **Predict** · Does this rebuild `marts/dim_products`? Does it rebuild `marts/fct_orders`?

# %%
selection = dg.AssetSelection.keys(["raw", "delivery_events"]).downstream()
with contextlib.redirect_stdout(io.StringIO()):       # dbt's own log would fill the screen; the events below are enough
    run = dg.materialize(ALL, selection=selection, resources={"dbt": dbt}, run_config=QUIET)
rebuilt = {ev.event_specific_data.materialization.asset_key.to_user_string() for ev in run.get_asset_materialization_events()}
checks = run.get_asset_check_evaluations()
print(sorted(rebuilt))
print(f"{len(checks)} dbt tests ran as checks; not passed: {[c.check_name for c in checks if not c.passed]}")
U.check(run.success and "marts/fct_orders" in rebuilt, "events and everything that reads them were rebuilt")
U.check("marts/dim_products" not in rebuilt and "raw/reviews" not in rebuilt, "unrelated assets were left alone")

# %% [markdown]
# 🔍 You asked for data to be fresh, and Dagster worked out which steps to run. In a script you'd keep that list by hand.
#
# ## 3 · Schedule the graph (3')
#
# A **schedule** starts a run on a clock. The clock is written as **cron**, five fields: minute, hour, day, month, weekday.
# Test it without waiting for the night:

# %%
nightly = dg.ScheduleDefinition(name="nightly", target=dg.AssetSelection.all(),
                                cron_schedule="30 1 * * *", execution_timezone="Asia/Riyadh")
ctx = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 9, 28, 1, 30))
tick = nightly.evaluate_tick(ctx)
print("cron:", nightly.cron_schedule, "| timezone:", nightly.execution_timezone, "| runs requested:", len(tick.run_requests))
U.check(len(tick.run_requests) == 1, "the schedule asks for one run per tick")

# %% [markdown]
# 🔍 What would cron for "every Monday at 06:00" look like? (Answer: `0 6 * * 1`.) In production a small service,
# the Dagster daemon, watches the clock and starts the runs; here you only tested the rule.
#
# ## 4 · Stop bad data with a blocking check (6')
#
# Phones retry when the signal drops, so the raw events hold duplicates. A loader that copies them straight through puts wrong
# numbers on the operations board. Here's that loader, and a KPI asset that reads it:

# %%
LOAD_SQL = """SELECT event_id, order_id, store_id, event_type, ingest_time FROM raw.delivery_events"""


@dg.asset(key=["ops", "delivery_events"], group_name="ops", deps=[raw_delivery_events])
def ops_delivery_events():
    sql(f"CREATE OR REPLACE TABLE ops.delivery_events AS {LOAD_SQL}")


@dg.asset(key=["ops", "delivered_per_store"], group_name="ops", deps=[ops_delivery_events])
def ops_delivered_per_store():
    sql("""CREATE OR REPLACE TABLE ops.delivered_per_store AS
           SELECT store_id, count(*) AS delivered FROM ops.delivery_events WHERE event_type = 'delivered' GROUP BY ALL""")

# %% [markdown]
# An **asset check** looks at an asset right after it's built and returns pass or fail. If it's **blocking** and fails, nothing
# downstream runs.
#
# ✏️ **Exercise** · Write the query for the check: rows minus distinct `event_id`s in `ops.delivery_events`. Zero means no duplicates.
#
# *Hint:* `SELECT count(*) - count(DISTINCT event_id) FROM ...`

# %%
def duplicates_sql():
    # BEGIN SOLUTION
    return "SELECT count(*) - count(DISTINCT event_id) FROM ops.delivery_events"
    # END SOLUTION

# %%
try:
    DUPES_SQL = duplicates_sql()
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    DUPES_SQL = "SELECT count(*) - count(DISTINCT event_id) FROM ops.delivery_events"


@dg.asset_check(asset=ops_delivery_events, blocking=True, description="Phones retry: each event_id must appear once")
def no_duplicate_events():
    dupes = sql(DUPES_SQL)[0][0]
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicates": dupes})


OPS = [raw_delivery_events, ops_delivery_events, ops_delivered_per_store, no_duplicate_events]
first = dg.materialize(OPS, run_config=QUIET, raise_on_error=False)
built = sorted(ev.event_specific_data.materialization.asset_key.to_user_string() for ev in first.get_asset_materialization_events())
meta = first.get_asset_check_evaluations()[0].metadata["duplicates"].value
print(f"run succeeded: {first.success} | duplicates found: {meta} (expected 312) | built: {built}")
U.check(not first.success and "ops/delivered_per_store" not in built, "the blocking check stopped the KPI from being built")
V.asset_run(OPS, first)

# %% [markdown]
# 🔍 The KPI asset was **skipped**, so the board keeps yesterday's number instead of a wrong one. Now fix the loader, keeping one
# row per `event_id`, and run again.

# %%
LOAD_SQL = """SELECT event_id, order_id, store_id, event_type, ingest_time FROM raw.delivery_events
              QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1"""
second = dg.materialize(OPS, run_config=QUIET, raise_on_error=False)
print("after the fix, run succeeded:", second.success, "|", sql("SELECT sum(delivered) FROM ops.delivered_per_store")[0][0], "delivered events")
U.check(second.success, "the check passes and the KPI is built")

# %% [markdown]
# ## 5 · Add a volume check (5')
#
# Tests catch what you can describe in advance. A **volume check** catches what you can't: today has far fewer rows than usual.
# (A **freshness** check is its twin: the newest row is older than it should be.) Orders swing by weekday, so compare each day
# with the same weekday in the weeks before it, not with yesterday.
#
# We simulate an incident: on 29 September a load job silently keeps only 40% of the day's orders, and still reports success.

# %%
@dg.asset(key=["ops", "orders_per_day"], group_name="ops", deps=[dg.AssetKey(["marts", "fct_orders"])])
def ops_orders_per_day():
    sql("""CREATE OR REPLACE TABLE ops.orders_per_day AS
           WITH d AS (SELECT created_at[:10]::DATE AS order_date, count(*)::DOUBLE AS orders FROM raw.orders
                      WHERE NOT (created_at[:10] = '2026-09-29' AND order_id % 10 >= 4)      -- the simulated bad load
                      GROUP BY 1)
           SELECT order_date, orders,
                  (SELECT median(orders) FROM d b WHERE b.order_date < d.order_date AND b.order_date >= d.order_date - 28
                     AND dayofweek(b.order_date) = dayofweek(d.order_date)) AS usual
           FROM d ORDER BY order_date""")


# %% [markdown]
# ✏️ **Exercise** · Complete the rule: a day is **odd** if it has a `usual` value and its `orders` are more than 30% away from it,
# in either direction. Return the `WHERE` condition.
#
# *Hint:* `usual IS NOT NULL AND abs(orders - usual) > ...`

# %%
def odd_day_condition():
    # BEGIN SOLUTION
    return "usual IS NOT NULL AND abs(orders - usual) > 0.3 * usual"
    # END SOLUTION

# %%
try:
    ODD_WHERE = odd_day_condition()
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    ODD_WHERE = "usual IS NOT NULL AND abs(orders - usual) > 0.3 * usual"


@dg.asset_check(asset=ops_orders_per_day, description="Volume: no day more than 30% away from its usual weekday")
def orders_volume():
    odd = sql(f"SELECT order_date::VARCHAR, orders, usual FROM ops.orders_per_day WHERE {ODD_WHERE} ORDER BY 1")
    return dg.AssetCheckResult(passed=not odd, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"odd_days": ", ".join(f"{d}: {n:.0f} vs. {u:.1f} usual" for d, n, u in odd) or "none"})


vol = dg.materialize([ops_orders_per_day, orders_volume], run_config=QUIET, raise_on_error=False)
c = vol.get_asset_check_evaluations()[0]
odd_days = c.metadata["odd_days"].value
print(f"run succeeded: {vol.success} | check passed: {c.passed} | odd days: {odd_days}")
print("expected: two days, 23 Sep and 29 Sep")
U.check(vol.success and not c.passed and "2026-09-29" in odd_days, "a warning, not a stop: the run succeeded and the alert is on record")

# %% [markdown]
# 🔍 Two alerts in two months. 23 September is National Day: a real spike, on the calendar. 29 September is the incident. That's
# why this check only **warns**: an odd day may be real, and someone should look. The duplicate check **blocks** because the number
# would be wrong. Now the whole thing, in one object: what a production
# deployment would load.

# %%
everything = [raw_delivery_events, raw_reviews, qafila_dbt, ops_delivery_events, ops_delivered_per_store, ops_orders_per_day]
defs = dg.Definitions(assets=everything, asset_checks=[no_duplicate_events, orders_volume], schedules=[nightly], resources={"dbt": dbt})
print(len(defs.resolve_asset_graph().get_all_asset_keys()), "assets, 1 schedule, 2 checks of your own plus the dbt tests")
V.asset_run(everything + [no_duplicate_events, orders_volume])

# %% [markdown]
# ## ✱ Stretch · Retry a flaky API
#
# The payments API answers some requests with HTTP 429, "too many requests". Give an asset `dg.RetryPolicy(max_retries=3, delay=1)`
# and watch Dagster retry it. The API below answers every 2nd request with 429 and is primed so your **first** call fails.

# %%
def stretch():
    # BEGIN SOLUTION
    import json
    import urllib.request

    api = U.PaymentsAPI(page_size=500, rate_limit_every=2)
    api.start()
    api.requests = 1          # primed: the next request is the 2nd, so it gets a 429
    attempts = []


    @dg.asset(retry_policy=dg.RetryPolicy(max_retries=3, delay=1))
    def payments_page():
        attempts.append(datetime.now().strftime("%H:%M:%S"))
        with urllib.request.urlopen(f"{api.base_url}/payments?updated_since=2026-09-01T00:00:00Z") as r:   # 429 raises HTTPError
            page = json.load(r)
        return dg.MaterializeResult(metadata={"rows": len(page["data"])})


    r = dg.materialize([payments_page], run_config=QUIET, raise_on_error=False)
    api.stop()
    print("success:", r.success, "| attempts:", len(attempts))
    # END SOLUTION


try:
    stretch()
except NotImplementedError:
    print("✱ Optional: write stretch() above if you have time.")

# %% [markdown]
# ## Checkpoint
#
# 1. A task-based script and an asset graph both run steps in order. What does the asset graph know that the script doesn't?
# 2. When should a check block, and when should it only warn?
# 3. Why compare today's volume with the same weekday rather than with yesterday?
#
# <details><summary>Answers</summary>
#
# 1. What each step produces and what it's built from. So it can rebuild only what's affected, and show when each table was last built and whether its checks passed.
# 2. Block when a wrong number downstream costs more than a late one (duplicates, an empty load). Warn when a person should look but the data is still useful.
# 3. Orders follow a weekly rhythm, so every weekend would look like an incident compared with the day before.
#
# </details>
#
# ## Carry forward
#
# You now have what the capstone's pipeline needs: assets, dbt in the graph, a schedule and checks. `labs/capstone/pipeline.py` is the
# same pattern at full size. It also runs `dagster dev`, the web interface for this graph.
#
# Want more? `tools/dagster.ipynb`.
