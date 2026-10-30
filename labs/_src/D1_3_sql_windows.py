# %% [markdown]
# # Lab 1.3 · Window functions
#
# Day 1 · Session 3 · 25 min. What you'll do:
# - see how `OVER` keeps every row where `GROUP BY` collapses them;
# - remove repeated delivery events with `ROW_NUMBER`, and keep the latest event of each order;
# - measure how long orders wait at each step with `LEAD`.
#
# **Tasks**
# - [ ] 1 · GROUP BY vs. OVER on eight orders (4')
# - [ ] 2 · Running revenue per city (5')
# - [ ] 3 · ROW_NUMBER: remove repeats, keep the latest (7')
# - [ ] 4 · LEAD: minutes at each step (6')
# - [ ] 5 · Break it: order by arrival instead of event time (3')
# - [ ] ✱ Stretch · ROW_NUMBER, RANK and DENSE_RANK on a tie

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
import de_viz as V

con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
con.sql("SET TimeZone = 'UTC'")   # the sources' timestamps are UTC: cast them the same way on any machine
U.attach_app(con)
RAW = U.RAW.as_posix()

# Eight small orders from three customers, few enough to check by eye
con.sql("""
    CREATE TABLE orders AS
    SELECT * FROM (VALUES
        (1, 101, DATE '2026-09-01', 40), (2, 102, DATE '2026-09-01', 25), (3, 101, DATE '2026-09-03', 60),
        (4, 103, DATE '2026-09-04', 15), (5, 102, DATE '2026-09-06', 35), (6, 101, DATE '2026-09-07', 20),
        (7, 103, DATE '2026-09-08', 15), (8, 102, DATE '2026-09-09', 50)
    ) t(order_id, customer_id, created_at, amount)""")

# The courier app's events. The three timestamp formats are read into one UTC column here; Lab 2.4 teaches how.
con.sql(f"""
    CREATE TABLE courier AS
    SELECT event_id, order_id, store_id, event_type, ingest_time::TIMESTAMP AS ingest_time,
           coalesce(make_timestamp(try_cast(event_time AS BIGINT) * 1000),
                    try_cast(event_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC') AS event_ts
    FROM read_json_auto('{RAW}/events/delivery_events.jsonl')""")
n_events = con.sql("SELECT count(*) FROM courier").fetchall()[0][0]
print(f"{n_events:,} delivery events")

# %% [markdown]
# ## 1 · GROUP BY vs. OVER on eight orders (4')
#
# `GROUP BY customer_id` turns each customer's rows into one row.

# %%
con.sql("SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id ORDER BY customer_id")

# %% [markdown]
# 🤔 **Predict** · The next query uses the same `sum(amount)`, followed by `OVER (PARTITION BY customer_id)`. How many rows come back: 3 or 8?

# %%
con.sql("""
    SELECT order_id, customer_id, amount,
           sum(amount) OVER (PARTITION BY customer_id) AS total
    FROM orders ORDER BY customer_id, order_id""")

# %% [markdown]
# 🔍 Eight rows. A **window function** computes over the rows related to the current row (its **window**) and writes the answer
# **next to** the row. `PARTITION BY customer_id` says: a row's window is every row of the same customer.
#
# ▶ **Step through it.** Press **Play** or **Next**: the highlighted rows are what `sum` reads for each row.
# (If the buttons do nothing, run the cell again.)

# %%
V.animate_window(con, "orders",
    "sum(amount) OVER (PARTITION BY customer_id) AS total",
    show=["order_id", "customer_id", "amount"],
    step_text="adds up the amounts of every row in its window: {rows}")

# %% [markdown]
# ## 2 · Running revenue per city (5')
#
# Add `ORDER BY` inside `OVER` and the window changes: from the start of the partition **up to this row**. That is a running total.

# %%
V.animate_window(con, "orders",
    "sum(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running",
    show=["order_id", "customer_id", "created_at", "amount"],
    step_text="adds up the amounts from the start of its window up to itself: {rows}")

# %% [markdown]
# Now the real thing. Finance wants September's month-to-date revenue, per city. Here is one row per city per Riyadh day:

# %%
con.sql("""
    CREATE TABLE daily AS
    SELECT s.city, (o.created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE AS day,
           round(sum(i.quantity * i.unit_price - i.discount), 2) AS revenue
    FROM app.orders o JOIN app.stores s USING (store_id) JOIN app.order_items i USING (order_id)
    WHERE o.status = 'delivered' AND (o.created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE >= DATE '2026-09-01'
    GROUP BY ALL""")
con.sql("FROM daily ORDER BY city, day LIMIT 3")

# %% [markdown]
# ✏️ **Exercise 1** · Add a column `month_to_date`: the running sum of `revenue`, restarting for each city, in day order. Fill the two blanks.
# Hint: the same shape as the animation above.

# %%
def month_to_date():
    # return con.sql("SELECT city, day, revenue, sum(revenue) OVER (PARTITION BY ___ ORDER BY ___) AS month_to_date FROM daily ORDER BY city, day")
    # BEGIN SOLUTION
    return con.sql("SELECT city, day, revenue, sum(revenue) OVER (PARTITION BY city ORDER BY day) AS month_to_date FROM daily ORDER BY city, day")
    # END SOLUTION

# %%
REF_MTD = "SELECT city, day, revenue, sum(revenue) OVER (PARTITION BY city ORDER BY day) AS month_to_date FROM daily ORDER BY city, day"
try:
    mtd = month_to_date()
    U.check(mtd.fetchall() == con.sql(REF_MTD).fetchall(), "each city's running total ends at its September revenue")
except NotImplementedError:
    print("⏭️ Exercise 1 not done yet. Expected: 90 rows, restarting at each city. Using the reference result.")
    mtd = con.sql(REF_MTD)
con.sql("SELECT city, max(day) AS last_day, round(max(month_to_date), 2) AS september_revenue, count(*) AS days FROM mtd GROUP BY city ORDER BY 3 DESC")

# %% [markdown]
# 🔍 **Look at** the two `ORDER BY`s in the exercise. The one inside `OVER` defines the window; the one at the end only sorts the output.
#
# ## 3 · ROW_NUMBER: remove repeats, keep the latest (7')
#
# The courier app sometimes sends the same event twice: same `event_id`, delivered again. Count them.

# %%
con.sql("SELECT count(*) AS events, count(DISTINCT event_id) AS different_events, count(*) - count(DISTINCT event_id) AS repeats FROM courier")

# %% [markdown]
# `row_number()` numbers the rows of each partition: 1, 2, 3... To keep one copy of each event, number the copies and keep number 1.
# `WHERE` can't do this, because it runs before any window exists. `QUALIFY` is a `WHERE` that runs **after** the windows.
# (It works in DuckDB, Snowflake, BigQuery and Databricks. In PostgreSQL, wrap the query in a subquery instead.)
#
# ✏️ **Exercise 2** · Build the view `courier_clean`: one row per `event_id`, keeping the copy that arrived first. Fill the blanks.
# Hint: partition by what must be unique, and order so that the row you want gets number 1.

# %%
def make_courier_clean():
    # con.sql("""
    #     CREATE OR REPLACE VIEW courier_clean AS
    #     SELECT * FROM courier
    #     QUALIFY row_number() OVER (PARTITION BY ___ ORDER BY ___) = 1""")
    # BEGIN SOLUTION
    con.sql("""
        CREATE OR REPLACE VIEW courier_clean AS
        SELECT * FROM courier
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1""")
    # END SOLUTION

# %%
REF_CLEAN = """CREATE OR REPLACE VIEW courier_clean AS SELECT * FROM courier
               QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1"""
try:
    make_courier_clean()
    a, b = con.sql("SELECT count(*), count(DISTINCT event_id) FROM courier_clean").fetchall()[0]
    U.check(a == b == 22206, f"{n_events:,} events received, {a:,} after removing repeats")
except (NotImplementedError, duckdb.Error) as e:
    print("⏭️ Exercise 2 not done yet. Expected: 22,206 rows, one per event_id. Using the reference result.")
    con.sql(REF_CLEAN)

# %% [markdown]
# The same pattern answers "what is the latest of each?". Order from the newest down, and number 1 is the latest event per order:

# %%
V.animate_window(con, "(SELECT event_id, order_id, event_type, event_ts FROM courier_clean WHERE order_id IN (5043883, 5043885) ORDER BY event_ts)",
    "row_number() OVER (PARTITION BY order_id ORDER BY event_ts DESC) AS rn",
    show=["order_id", "event_type", "event_ts"],
    step_text="counts the rows from the start of its window up to itself: {rows}",
    keep="rn = 1", keep_label="Keep rn = 1 (the latest event of each order)")

# %%
con.sql("""
    CREATE TABLE latest_status AS
    SELECT order_id, event_type FROM courier_clean
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1""")
con.sql("SELECT event_type AS last_step, count(*) AS orders FROM latest_status GROUP BY 1 ORDER BY 2 DESC")

# %% [markdown]
# 🔍 **Latest per key** is the most used pattern of your first job. Lab 2.3 uses it to apply a feed of changes to a table.
#
# ## 4 · LEAD: minutes at each step (6')
#
# `lag(x)` reads `x` from the **previous** row of the window and `lead(x)` from the **next** one. At the edge of a partition there is no neighbour: `NULL`.
#
# ✏️ **Exercise 3** · For each event, `next_ts` is the time of the next event of the same order. Fill the blanks.
# Hint: which rows belong together, and in what order did they happen?

# %%
def make_steps():
    # con.sql("""
    #     CREATE OR REPLACE VIEW steps AS
    #     SELECT order_id, store_id, event_type, event_ts + INTERVAL 3 HOUR AS riyadh_time,
    #            lead(event_ts) OVER (PARTITION BY ___ ORDER BY ___) AS next_ts,
    #            date_diff('second', event_ts, next_ts) / 60 AS minutes
    #     FROM courier_clean""")
    # BEGIN SOLUTION
    con.sql("""
        CREATE OR REPLACE VIEW steps AS
        SELECT order_id, store_id, event_type, event_ts + INTERVAL 3 HOUR AS riyadh_time,
               lead(event_ts) OVER (PARTITION BY order_id ORDER BY event_ts) AS next_ts,
               date_diff('second', event_ts, next_ts) / 60 AS minutes
        FROM courier_clean""")
    # END SOLUTION

# %%
REF_STEPS = """CREATE OR REPLACE VIEW steps AS
    SELECT order_id, store_id, event_type, event_ts + INTERVAL 3 HOUR AS riyadh_time,
           lead(event_ts) OVER (PARTITION BY order_id ORDER BY event_ts) AS next_ts,
           date_diff('second', event_ts, next_ts) / 60 AS minutes
    FROM courier_clean"""
try:
    make_steps()
    picked = con.sql("SELECT round(minutes, 1) FROM steps WHERE order_id = 5043883 AND event_type = 'assigned'").fetchall()[0][0]
    U.check(picked == 10.3, f"Khalid's bag waited {picked} minutes between assigned and picked up")
except (NotImplementedError, duckdb.Error):
    print("⏭️ Exercise 3 not done yet. Expected for order 5043883: 10.3, 1.5, 6.6 minutes, then NULL. Using the reference result.")
    con.sql(REF_STEPS)
con.sql("SELECT event_type, riyadh_time, round(minutes, 1) AS minutes FROM steps WHERE order_id = 5043883 ORDER BY riyadh_time")

# %% [markdown]
# 🔍 The last row is `NULL`: no next event. Without `PARTITION BY`, `lead` would read another order's event, with no error.
#
# Operations' question: at which store do orders wait longest for a courier to pick them up?

# %%
con.sql("""
    SELECT store_id, round(median(minutes), 1) AS median_pickup_wait, count(*) AS orders
    FROM steps WHERE event_type = 'assigned'
    GROUP BY store_id ORDER BY 2 DESC LIMIT 4""")

# %% [markdown]
# ## 5 · Break it: order by arrival instead of event time (3')
#
# Every event has two clocks: `event_ts`, when it happened, and `ingest_time`, when it reached us. A courier's phone offline in a basement sends its events late.
# 🤔 **Predict** · If "latest" means latest to arrive, will any order get a different status?

# %%
wrong = con.sql("""
    SELECT count(*) FROM (
        SELECT order_id, event_type FROM courier_clean
        QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY ingest_time DESC) = 1
    ) a JOIN latest_status b USING (order_id)
    WHERE a.event_type <> b.event_type""").fetchall()[0][0]
print(wrong, "orders get the wrong status when ordered by arrival")

# %% [markdown]
# 🔍 An older step **arrived** after a newer one, and became "the latest". No error, just a wrong dashboard. **Order by when it happened, never by when it arrived.**
#
# ## ✱ Stretch · ROW_NUMBER, RANK and DENSE_RANK on a tie
#
# Orders 4 and 7 tie at 15, the smallest amount. 🤔 **Predict** the three columns for the first three rows, then run.

# %%
con.sql("""
    SELECT customer_id, order_id, amount,
           row_number() OVER w AS row_number,
           rank()       OVER w AS rank,
           dense_rank() OVER w AS dense_rank
    FROM orders
    WINDOW w AS (ORDER BY amount)
    ORDER BY amount, order_id""")

# %% [markdown]
# 🔍 `row_number` always counts up, breaking ties arbitrarily, so add a tie-breaker to its `ORDER BY` when the tied rows differ. `rank` skips after a tie (1, 1, 3) and `dense_rank` doesn't (1, 1, 2).
#
# ## Checkpoint
#
# 1. What does `PARTITION BY` decide, and what does the `ORDER BY` inside `OVER` decide?
# 2. Why can't you write `WHERE row_number() OVER (...) = 1`?
# 3. You want the latest status of each order. Which column goes in the `ORDER BY`: when it happened, or when it arrived?
#
# <details><summary>Answers</summary>
#
# 1. `PARTITION BY` decides which rows belong to a row's window. The `ORDER BY` decides their order, which gives running totals, numbering and neighbours their meaning.
# 2. `WHERE` runs before window functions exist. Use `QUALIFY`, or a subquery.
# 3. When it happened (`event_ts`). Late arrivals would otherwise overwrite newer steps.
#
# </details>
#
# ## Carry forward
#
# Dedupe and latest-per-key return in Lab 2.3 (changes), Lab 2.4 (Polars' `.over()`) and Lab 4.3 (streaming). Each of those labs starts from its own data.
#
# Want more? `tools/sql_windows.ipynb` covers frames, ties and a customer's city history.
