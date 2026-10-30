# %% [markdown]
# # Lab 2.3 · From a change feed to history
#
# Day 2 · Session 3 · 25 min
#
# The app's `customers` table changes all the time. Our copy goes stale the moment it does. You'll replay a month of
# changes from a **change feed** (one event for each insert, update or delete) and end exactly where the app is. What you'll do:
# - find the feed's duplicates and out-of-order events, and keep the latest change per customer with a window function;
# - apply those changes with `MERGE`, and prove the copy matches the app;
# - build a simple history table (SCD Type 2) from the same events.
#
# **Tasks**
# - [ ] 1 · Read the feed; find duplicates and late events (4')
# - [ ] 2 · Keep the latest change per customer (6')
# - [ ] 3 · MERGE the changes into our copy (6')
# - [ ] 4 · Reconcile with the app (0 differences) (4')
# - [ ] 5 · Build the SCD2 history (5')
# - [ ] ✱ Stretch · As-of join: revenue by customer city at order time

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
import pandas as pd

pd.set_option("display.max_colwidth", 60)
con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)                 # the app database, as it looks on 30 September, is `app.*`
RAW = U.RAW.as_posix()

# %% [markdown]
# ## 1 · Read the feed; find duplicates and late events (4')
#
# Two inputs: a snapshot of `customers` taken on **1 September** (our stale copy), and every change made during
# September, in the order they reached us. We keep that order as `arrival`, and lift the event's position in the
# database's log, `lsn` (log sequence number: higher means later), into its own column.
#
# Each event has `op` (`c` create, `u` update, `d` delete), `before` and `after` (the whole row), `lsn` and `ts_ms`
# (the commit time).

# %%
con.sql(f"""
    CREATE TABLE changes AS
    SELECT row_number() OVER () AS arrival, source.lsn AS lsn, op, ts_ms, before, after
    FROM read_json_auto('{RAW}/cdc/customers_changes.jsonl')
""")
con.sql("SELECT op, count(*) AS events FROM changes GROUP BY op ORDER BY op").df()

# %% [markdown]
# Follow one customer. On 2 September, customer 100898 changed email, then moved from Dammam to Jeddah.

# %%
con.sql("""
    SELECT arrival, lsn, op, after.city, after.district, make_timestamp(ts_ms * 1000) AS committed_utc
    FROM changes WHERE after.customer_id = 100898 ORDER BY arrival
""").df()

# %% [markdown]
# 🔍 Compare the first and last rows. Now count both problems across the whole feed. A **duplicate** is an `lsn` delivered
# more than once. A **late** event has a smaller `lsn` than some event that arrived before it (a window function that
# looks at all earlier rows finds them).

# %%
n_total, n_duplicates = con.sql("SELECT count(*), count(*) - count(DISTINCT lsn) FROM changes").fetchall()[0]
n_late = con.sql("""
    SELECT count(*) FROM (
        SELECT lsn < max(lsn) OVER (ORDER BY arrival ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS late
        FROM changes
    ) WHERE late
""").fetchall()[0][0]
print(f"{n_total:,} events | {n_duplicates} duplicates | {n_late:,} late")
U.check(n_duplicates == 188, "188 events were delivered twice")

# %% [markdown]
# 🔍 Both are normal for a change feed. Delivery is **at-least-once**: after a restart the connector would rather send an
# event twice than lose it, and events can overtake each other on the way. The fix for both is the same: **trust the log
# position, not the arrival order.**
#
# ## 2 · Keep the latest change per customer (6')
#
# Our copy only needs the **last** change per customer. Two steps: drop the repeats, then number each customer's events
# newest first and keep number 1. That's the `row_number` dedupe from Day 1. `QUALIFY` is `WHERE` for window functions.
# (A delete has no `after`, so the key comes from `before`.)
#
# The view `events` (given) has one row per `lsn` and a `customer_id` column.
#
# ✏️ **Exercise** · Complete `latest_filter`: return the `QUALIFY` line that keeps each customer's newest event.
#
# Hint: `QUALIFY row_number() OVER (PARTITION BY ___ ORDER BY ___ DESC) = 1`

# %%
con.sql("""
    CREATE VIEW events AS
    SELECT DISTINCT ON (lsn) coalesce(after.customer_id, before.customer_id) AS customer_id, lsn, op, ts_ms, after
    FROM changes
""")


def latest_filter():
    # BEGIN SOLUTION
    return "QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) = 1"
    # END SOLUTION

# %%
try:
    qualify = latest_filter()
except NotImplementedError:
    print("✏️ Not done yet: using the reference line so the rest of the lab runs.")
    qualify = "QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY lsn DESC) = 1"
con.sql(f"""
    CREATE OR REPLACE VIEW latest AS
    SELECT customer_id, op, lsn, after.* EXCLUDE (customer_id) FROM events {qualify}
""")
n_events, n_latest = con.sql("SELECT (SELECT count(*) FROM events), (SELECT count(*) FROM latest)").fetchall()[0]
print(f"{n_total:,} events -> {n_events:,} distinct lsn -> {n_latest:,} customers (expected: 6,346 -> 6,158 -> 4,542)")
U.check(n_latest == 4542, "one row per customer who changed")
con.sql("SELECT customer_id, op, lsn, city, district FROM latest WHERE customer_id = 100898").df()

# %% [markdown]
# ## 3 · MERGE the changes into our copy (6')
#
# `MERGE` compares a source with a target by key, and for each source row runs the first branch that matches. An
# **upsert** is update-or-insert by key; with deletes added, one statement does all three.
#
# Careful with types when you read the snapshot: identifiers like the mobile `0596390578` are text, not numbers.

# %%
SNAPSHOT = f"""read_csv('{RAW}/cdc/customers_snapshot_2026-09-01.csv',
    types = {{'mobile': 'VARCHAR', 'national_id': 'VARCHAR', 'iban': 'VARCHAR',
              'created_at': 'VARCHAR', 'updated_at': 'VARCHAR'}})"""
con.sql(f"CREATE TABLE customers AS SELECT * FROM {SNAPSHOT}")
cols = con.table("customers").columns
print("snapshot rows:", con.sql("SELECT count(*) FROM customers").fetchall()[0][0], "| columns:", cols)

# Before we change anything: what will each latest change do?
con.sql("""
    SELECT CASE WHEN c.customer_id IS NULL AND l.op = 'd' THEN 'not found, a delete: nothing to do'
                WHEN c.customer_id IS NULL                THEN 'not found: INSERT'
                WHEN l.op = 'd'                           THEN 'found, a delete: DELETE'
                ELSE                                           'found: UPDATE' END AS outcome,
           count(*) AS customers
    FROM latest l LEFT JOIN customers c USING (customer_id)
    GROUP BY 1 ORDER BY 2 DESC
""").df()

# %% [markdown]
# ✏️ **Exercise** · Complete `merge_branches`: return the three `WHEN` branches, in this order. A found row whose `op` is
# `d` is deleted; any other found row is updated; a row not found is inserted, unless it's a delete.
# `set_all`, `col_list` and `val_list` (given) spell out the columns so you don't have to.
#
# Hint: `WHEN MATCHED AND s.op = 'd' THEN DELETE`, `WHEN MATCHED THEN UPDATE SET {set_all}`,
# `WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT ({col_list}) VALUES ({val_list})`.

# %%
set_all = ", ".join(f"{c} = s.{c}" for c in cols if c != "customer_id")
col_list = ", ".join(cols)
val_list = ", ".join(f"s.{c}" for c in cols)


def merge_branches():
    # BEGIN SOLUTION
    return f"""
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_all}
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT ({col_list}) VALUES ({val_list})
    """
    # END SOLUTION

# %%
try:
    branches = merge_branches()
except NotImplementedError:
    print("✏️ Not done yet: using the reference branches so the rest of the lab runs.")
    branches = f"""
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {set_all}
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT ({col_list}) VALUES ({val_list})
    """
con.sql(f"MERGE INTO customers USING latest s USING (customer_id) {branches}")
n_copy = con.sql("SELECT count(*) FROM customers").fetchall()[0][0]
print(f"our copy: {n_copy:,} rows (expected: 6,060 - 139 + 1,521 = 7,442)")
U.check(n_copy == 7442, "the row count adds up")

con.sql(f"MERGE INTO customers USING latest s USING (customer_id) {branches}")     # run it again
U.check(con.sql("SELECT count(*) FROM customers").fetchall()[0][0] == 7442, "a second MERGE changes nothing: it's idempotent")
con.sql("SELECT customer_id, city, district FROM customers WHERE customer_id IN (100898, 107371)").df()

# %% [markdown]
# 🔍 Customer 100898 lives in Jeddah now. Customer 107371 is Khalid, who signed up on 26 September, after the snapshot:
# the insert branch. Which branch would run for a customer who signed up **and** was erased during September?
#
# ## 4 · Reconcile with the app (0 differences) (4')
#
# `app.customers` holds the truth on 30 September. A **reconciliation** compares the copy with its source. `EXCEPT` compares whole
# rows, so a single wrong column in a single row shows up. Nothing to write: run it, then see what it would have caught.

# %%
def differences(table):
    missing = con.sql(f"SELECT count(*) FROM (SELECT * FROM app.customers EXCEPT SELECT * FROM {table})").fetchall()[0][0]
    extra = con.sql(f"SELECT count(*) FROM (SELECT * FROM {table} EXCEPT SELECT * FROM app.customers)").fetchall()[0][0]
    return missing, extra

n_app = con.sql("SELECT count(*) FROM app.customers").fetchall()[0][0]
missing, extra = differences("customers")
print(f"app {n_app:,} rows | copy {n_copy:,} rows | missing {missing} | extra {extra}")
U.check(missing == 0 and extra == 0, "the copy matches the app, every column of every row")

# %% [markdown]
# Now the same MERGE, but trusting the arrival order instead of `lsn`:

# %%
con.sql(f"CREATE TABLE customers_by_arrival AS SELECT * FROM {SNAPSHOT}")
con.sql("""
    CREATE VIEW latest_by_arrival AS
    SELECT coalesce(after.customer_id, before.customer_id) AS customer_id, op, lsn, after.* EXCLUDE (customer_id)
    FROM changes
    QUALIFY row_number() OVER (PARTITION BY coalesce(after.customer_id, before.customer_id) ORDER BY arrival DESC) = 1
""")
con.sql(f"MERGE INTO customers_by_arrival USING latest_by_arrival s USING (customer_id) {branches}")
n_arrival = con.sql("SELECT count(*) FROM customers_by_arrival").fetchall()[0][0]
print(f"by arrival: {n_arrival:,} rows | missing, extra = {differences('customers_by_arrival')}")
con.sql("SELECT customer_id, city, district FROM customers_by_arrival WHERE customer_id = 100898").df()

# %% [markdown]
# 🔍 The row count is the same, and nothing failed, yet 100898 is back in Dammam: the re-sent 17:15 event arrived last. Only
# the row-by-row comparison catches it. In production, run this check on a schedule and alert when it isn't zero.
#
# ## 5 · Build the SCD2 history (5')
#
# Our copy answers "where does the customer live **now**?". Marketing also asks "where did they live **when they
# ordered**?", and MERGE overwrote that. A **slowly changing dimension, Type 2** (SCD2) keeps one row per **version**, with a validity interval:
# each version starts at its change and ends where the next one starts.
#
# `versions` (given) lists every version start: the snapshot rows first (`lsn` 0), then one per event. A delete has
# `is_delete` true: it ends the last version and starts nothing.
#
# ✏️ **Exercise** · Complete `valid_to_expr`: return the expression for the next version's `valid_from` of the same
# customer. It's `lead`, from Day 1, and ordering by `lsn` handles out-of-order events for free.
#
# Hint: `lead(valid_from) OVER (PARTITION BY ___ ORDER BY ___)`.

# %%
con.sql(f"""
    CREATE VIEW versions AS
    SELECT customer_id, 0 AS lsn, false AS is_delete, city, district, updated_at::TIMESTAMP AS valid_from
    FROM {SNAPSHOT}
    UNION ALL
    SELECT customer_id, lsn, op = 'd', after.city, after.district, make_timestamp(ts_ms * 1000)
    FROM events
""")


def valid_to_expr():
    # BEGIN SOLUTION
    return "lead(valid_from) OVER (PARTITION BY customer_id ORDER BY lsn)"
    # END SOLUTION

# %%
try:
    valid_to = valid_to_expr()
except NotImplementedError:
    print("✏️ Not done yet: using the reference expression so the rest of the lab runs.")
    valid_to = "lead(valid_from) OVER (PARTITION BY customer_id ORDER BY lsn)"
con.sql(f"""
    CREATE TABLE dim_customer_scd2 AS
    SELECT customer_id, city, district, valid_from, {valid_to} AS valid_to, {valid_to} IS NULL AS is_current
    FROM versions
    QUALIFY NOT is_delete        -- filtered after the window ran, so a delete still ends the version before it
""")
n_versions, n_customers, n_current = con.sql("""
    SELECT count(*), count(DISTINCT customer_id), count(*) FILTER (WHERE is_current) FROM dim_customer_scd2
""").fetchall()[0]
print(f"{n_versions:,} versions of {n_customers:,} customers | {n_current:,} current (expected: 12,059 / 7,601 / 7,442)")
U.check(n_current == n_app, "one current version per live customer")
con.sql("SELECT * FROM dim_customer_scd2 WHERE customer_id = 100898 ORDER BY valid_from").df()

# %% [markdown]
# Three versions of 100898, and each ends where the next begins. Marketing's question: order 5019534, from 27 August.

# %%
con.sql("""
    SELECT o.order_id, o.created_at, now_city.city AS city_now, then_city.city AS city_when_ordered
    FROM app.orders o
    JOIN customers now_city USING (customer_id)
    JOIN dim_customer_scd2 then_city
      ON o.customer_id = then_city.customer_id
     AND o.created_at::TIMESTAMP >= then_city.valid_from
     AND (o.created_at::TIMESTAMP < then_city.valid_to OR then_city.valid_to IS NULL)
    WHERE o.order_id = 5019534
""").df()

# %% [markdown]
# 🔍 That join, to the version valid at the time, is an **as-of join**. And notice: the 159 customers who asked to be
# erased are gone from our copy, but their closed versions, personal data included, are still in this table. Day 4 returns to that.
#
# ## ✱ Stretch · As-of join: revenue by customer city at order time
#
# Complete `revenue_by_city`: September's delivered revenue (`sept_orders`, given) per city, once with the as-of join and
# once with the current version, side by side. The totals match; only the split between cities moves.

# %%
con.sql("""
    CREATE VIEW sept_orders AS
    SELECT o.order_id, o.customer_id, o.created_at::TIMESTAMP AS created_at,
           sum(i.quantity * i.unit_price - i.discount) AS revenue
    FROM app.orders o JOIN app.order_items i USING (order_id)
    WHERE o.status = 'delivered' AND o.created_at >= '2026-09-01'
    GROUP BY ALL
""")


def revenue_by_city():
    # BEGIN SOLUTION
    return con.sql("""
        WITH at_order AS (
            SELECT d.city, sum(o.revenue) AS at_order_time
            FROM sept_orders o JOIN dim_customer_scd2 d
              ON o.customer_id = d.customer_id
             AND o.created_at >= d.valid_from AND (o.created_at < d.valid_to OR d.valid_to IS NULL)
            GROUP BY ALL
        ), cur AS (
            SELECT d.city, sum(o.revenue) AS current_city
            FROM sept_orders o JOIN dim_customer_scd2 d ON o.customer_id = d.customer_id AND d.is_current
            GROUP BY ALL
        )
        SELECT city, round(at_order_time) AS at_order_time, round(current_city) AS current_city
        FROM at_order JOIN cur USING (city) ORDER BY city
    """).df()
    # END SOLUTION

# %%
try:
    display(revenue_by_city())
except NotImplementedError:
    print("✱ Stretch not attempted: that's fine.")

# %% [markdown]
# ## Checkpoint
#
# **1.** Why order by `lsn` and not by arrival?
#
# <details><summary>Answer</summary>Arrival is when the event reached us: restarts re-send events and they can overtake
# each other. The lsn is the event's position in the database's own log, the true order of the changes.</details>
#
# **2.** Your MERGE runs twice on the same batch. What happens?
#
# <details><summary>Answer</summary>Nothing changes the second time: the updates write the same values, the deletes find
# nothing, and the inserted rows now match and get updated with the same values. So a retry is safe.</details>
#
# **3.** Which Qafila columns deserve SCD2 history, and which are fine overwritten?
#
# <details><summary>Answer</summary>History where past values change the answer: a customer's city, a store's manager.
# Overwrite where the old value was only wrong: a typo in a name. The choice can differ per column.</details>
#
# ## Carry forward
#
# You can follow every change of a source table, keep it current and keep its history. Lab 2.4 turns to the other messy
# source: the couriers' events. Dimensions return on Day 3 (Lab 3.1). Want more? `tools/sql_windows.ipynb`.
