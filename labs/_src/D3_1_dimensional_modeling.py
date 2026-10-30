# %% [markdown]
# # Lab 3.1 · A star schema for Qafila
#
# Day 3 · Session 1 · 25 min. Finance asks "how much did we sell?" and you build the tables that answer it, in plain SQL.
# What you'll do:
# - write down what "revenue" means, and test what one row of the sales table stands for (its **grain**);
# - build the star: `fct_order_items` in the middle, four dimensions around it, with an **unknown member**;
# - walk into the **fan-out** trap on purpose, then fix it.
#
# **Tasks**
# - [ ] 1 · Compare three revenue totals (4')
# - [ ] 2 · Test the grain of the order lines (4')
# - [ ] 3 · Build the dimensions with an unknown member (6')
# - [ ] 4 · Build the fact table and reconcile it (6')
# - [ ] 5 · Spot the fan-out and fix it (5')
# - [ ] ✱ Stretch · Measure the National Day uplift per city

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
import shutil

import duckdb

U.build_warehouse()                      # the raw tables, as loaded from Qafila's sources
OUT = U.lab_dir("D3_1", reset=True)
DB = OUT / "qafila.duckdb"
shutil.copy(U.WAREHOUSE_DB, DB)          # your private copy: nothing here touches other labs
con = duckdb.connect(str(DB))
con.sql("SET enable_progress_bar = false")
con.sql("CREATE SCHEMA marts")           # marts = the tables people query
q = lambda sql: con.sql(sql).df()
print("raw tables:", [r[0] for r in con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' ORDER BY 1").fetchall()])

# %% [markdown]
# ## 1 · Compare three revenue totals (4')
#
# Finance asks: how much did we sell in August and September? Here are three honest answers from the same tables.
# A line's **net amount** is `quantity × unit_price − discount`.

# %%
q("""
    WITH lines AS (
        SELECT i.*, o.status, i.quantity * i.unit_price - i.discount AS net
        FROM raw.order_items i JOIN raw.orders o USING (order_id)
    )
    SELECT 'every order line' AS answer, round(sum(net)) AS sar FROM lines
    UNION ALL SELECT 'delivered orders only', round(sum(net) FILTER (WHERE status = 'delivered')) FROM lines
    UNION ALL SELECT 'delivered, plus delivery fees',
           round(sum(net) FILTER (WHERE status = 'delivered')) + (SELECT sum(delivery_fee) FROM raw.orders WHERE status = 'delivered') FROM lines
""")

# %% [markdown]
# 🔍 Nobody made a mistake: each total answers a slightly different question. The fix is a written **business definition**.
# For this course:
#
# > **Product revenue** is the net amount of delivered order lines with a positive quantity. Delivery fees are a separate
# > number. The city is the store's city.
#
# Why "positive quantity"? Look at the lines where it isn't. They are entry errors, not refunds (refunds are an order status).

# %%
q("""
    SELECT count(*) AS bad_lines,
           count(*) FILTER (WHERE o.status = 'delivered') AS on_delivered_orders,
           round(sum(i.quantity * i.unit_price - i.discount) FILTER (WHERE o.status = 'delivered')) AS sar_on_delivered
    FROM raw.order_items i JOIN raw.orders o USING (order_id)
    WHERE i.quantity <= 0
""")

# %% [markdown]
# 🔍 What is revenue under the definition, given the table above? Work it out from 6,494,684, then check against Task 4.
#
# ## 2 · Test the grain of the order lines (4')
#
# The **grain** is the one sentence that says what one row means. Ours:
#
# > **One row per order line**: one product in one order.
#
# A grain is a claim about the data, so test it. Khalid's order 5043883 has two lines:

# %%
q("SELECT order_id, line_no, product_id, quantity, unit_price, discount FROM raw.order_items WHERE order_id = 5043883")

# %% [markdown]
# ✏️ **Exercise** · Complete the query: count all rows of `raw.order_items`, and count the distinct `(order_id, line_no)` pairs.
# The grain holds if the two numbers are equal.
#
# *Hint:* `count(DISTINCT (order_id, line_no))` counts distinct pairs.

# %%
def grain_counts_sql():
    # BEGIN SOLUTION
    return "SELECT count(*), count(DISTINCT (order_id, line_no)) FROM raw.order_items"
    # END SOLUTION

# %%
try:
    n_lines, n_keys = con.sql(grain_counts_sql()).fetchall()[0]
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    n_lines, n_keys = con.sql("SELECT count(*), count(DISTINCT (order_id, line_no)) FROM raw.order_items").fetchall()[0]
print(f"rows: {n_lines:,} | distinct keys: {n_keys:,}   (expected: 172,545 and 172,545)")
U.check(n_lines == n_keys == 172_545, "(order_id, line_no) is unique: the grain holds")

# %% [markdown]
# ## 3 · Build the dimensions with an unknown member (6')
#
# A **dimension** table holds the descriptions you filter and group by: when, what, where, who. Its key is the id the
# source already uses (`product_id`, `store_id`, `customer_id`). Many warehouses mint their own integer key instead, called
# a surrogate key, so one source id can have several versions; we don't need that here.
#
# The date dimension is generated, one row per day. The other three come from the raw tables. Direct identifiers (name,
# email, mobile, national ID, IBAN) stay out of `dim_customers`: analysis doesn't need them.

# %%
con.sql("""
    CREATE TABLE marts.dim_date AS
    SELECT d AS date_day, year(d) AS year, month(d) AS month, dayname(d) AS day_name, weekofyear(d) AS iso_week,
           dayofweek(d) IN (5, 6) AS is_weekend,            -- the Saudi weekend: Friday, Saturday
           d = DATE '2026-09-23' AS is_national_day
    FROM (SELECT unnest(range(DATE '2026-01-01', DATE '2027-01-01', INTERVAL 1 DAY))::DATE AS d)
""")
con.sql("""
    CREATE TABLE marts.dim_products AS
    SELECT product_id, trim(sku) AS sku, name_en AS product_name_en, category, brand, unit_price AS list_price
    FROM raw.products
""")
con.sql("CREATE TABLE marts.dim_stores AS SELECT store_id, name_en AS store_name_en, city FROM raw.stores")
q("SELECT * FROM marts.dim_date WHERE date_day BETWEEN '2026-09-22' AND '2026-09-26'")

# %% [markdown]
# Before building `dim_customers`, see the problem. Some orders point to customers who no longer exist (erased on request),
# so an inner join quietly drops their sales:

# %%
q("""
    WITH o AS (SELECT count(*) AS n FROM raw.orders), j AS (SELECT count(*) AS n FROM raw.orders JOIN raw.customers USING (customer_id))
    SELECT o.n AS all_orders, j.n AS after_inner_join, o.n - j.n AS orders_lost FROM o, j
""")

# %% [markdown]
# The fix is an **unknown member**: one extra row with `customer_id = -1`. The fact points lost customers at it, so every
# sale still joins.
#
# ✏️ **Exercise** · Complete `dim_customers`: add the unknown member with `UNION ALL`. The columns are
# `customer_id, city, district, signup_date`; use `-1, 'Unknown', 'Unknown', NULL`.
#
# *Hint:* `... FROM raw.customers UNION ALL SELECT ...`

# %%
def dim_customers_sql():
    base = "SELECT customer_id, city, district, created_at::DATE AS signup_date FROM raw.customers"
    # BEGIN SOLUTION
    return base + " UNION ALL SELECT -1, 'Unknown', 'Unknown', NULL"
    # END SOLUTION

# %%
try:
    con.sql(f"CREATE OR REPLACE TABLE marts.dim_customers AS {dim_customers_sql()}")
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    con.sql("""CREATE OR REPLACE TABLE marts.dim_customers AS
               SELECT customer_id, city, district, created_at::DATE AS signup_date FROM raw.customers
               UNION ALL SELECT -1, 'Unknown', 'Unknown', NULL""")
print("expected: exactly one row with customer_id = -1")
U.check(q("SELECT count(*) AS n FROM marts.dim_customers WHERE customer_id = -1").n[0] == 1, "unknown member present")
q("""SELECT 'dim_date' AS dimension, count(*) AS rows FROM marts.dim_date
     UNION ALL SELECT 'dim_products', count(*) FROM marts.dim_products
     UNION ALL SELECT 'dim_stores', count(*) FROM marts.dim_stores
     UNION ALL SELECT 'dim_customers', count(*) FROM marts.dim_customers""")

# %% [markdown]
# ## 4 · Build the fact table and reconcile it (6')
#
# A **fact** table holds one row per event at the declared grain: keys that point to dimensions, and numbers you add up.
# Read this query slowly. It makes three decisions for you:
# - `order_date` is the **Riyadh** date: the app stores UTC, Riyadh is UTC+3;
# - `customer_id` falls back to `-1`, your unknown member;
# - lines with a quantity of zero or less stay out. In Lab 3.3 they are reported, not lost.

# %%
con.sql("""
    CREATE OR REPLACE TABLE marts.fct_order_items AS
    SELECT i.order_id * 100 + i.line_no                                 AS order_line_id,
           i.order_id, i.line_no,
           (o.created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE            AS order_date,
           coalesce(c.customer_id, -1)                                  AS customer_id,
           o.store_id,
           i.product_id,
           o.status                                                     AS order_status,
           i.quantity, i.unit_price, i.discount,
           round(i.quantity * i.unit_price - i.discount, 2)             AS net_amount
    FROM raw.order_items i
    JOIN raw.orders o USING (order_id)
    LEFT JOIN marts.dim_customers c ON c.customer_id = o.customer_id
    WHERE i.quantity > 0
""")
q("SELECT order_line_id, order_date, customer_id, store_id, product_id, order_status, net_amount FROM marts.fct_order_items WHERE order_id = 5043883")

# %% [markdown]
# 🔍 Khalid ordered at 22:35 UTC on 27 September. Which date does the fact show, and why?
#
# **Reconcile** before anyone trusts it: every source line must be either in the fact or set aside on purpose, and the money
# must match.

# %%
recon = q("""
    SELECT (SELECT count(*) FROM raw.order_items)                                     AS source_lines,
           (SELECT count(*) FROM marts.fct_order_items)                               AS fact_lines,
           (SELECT count(*) FROM raw.order_items WHERE quantity <= 0)                 AS set_aside,
           (SELECT count(*) FROM marts.fct_order_items WHERE customer_id = -1)        AS lines_on_unknown_customer,
           (SELECT round(sum(quantity * unit_price - discount)) FROM raw.order_items WHERE quantity > 0) AS source_sar,
           (SELECT round(sum(net_amount)) FROM marts.fct_order_items)                 AS fact_sar
""")
recon

# %%
U.check(recon.source_lines[0] == recon.fact_lines[0] + recon.set_aside[0], "every source line is in the fact or set aside")
U.check(abs(recon.source_sar[0] - recon.fact_sar[0]) <= 1, "the money reconciles")
U.check(recon.lines_on_unknown_customer[0] > 0, "the sales of erased customers are kept")

# %% [markdown]
# Now the star does its job. Every star query looks the same: join to a dimension, filter, group, sum. Revenue per city, by
# the definition:

# %%
by_city = q("""
    SELECT s.city, round(sum(f.net_amount), 2) AS revenue_sar, count(DISTINCT f.order_id) AS orders
    FROM marts.fct_order_items f JOIN marts.dim_stores s USING (store_id)
    WHERE f.order_status = 'delivered'
    GROUP BY s.city ORDER BY revenue_sar DESC
""")
print(f"total product revenue: {by_city.revenue_sar.sum():,.0f} SAR")
by_city

# %% [markdown]
# ## 5 · Spot the fan-out and fix it (5')
#
# Finance now wants "revenue including delivery fees". A colleague joins the **order-level** fee onto the **line-level** fact:

# %%
naive_sql = """
    SELECT round(sum(f.net_amount + o.delivery_fee)) AS revenue_incl_fees
    FROM marts.fct_order_items f JOIN raw.orders o USING (order_id)
    WHERE f.order_status = 'delivered'
"""
naive = q(naive_sql).revenue_incl_fees[0]
q("""SELECT f.order_id, f.line_no, f.net_amount, o.delivery_fee
     FROM marts.fct_order_items f JOIN raw.orders o USING (order_id) WHERE f.order_id = 5043883""")

# %% [markdown]
# 🔍 Khalid paid one 9.00 fee. How many times does it appear? A join makes one row per match, so summing the fee repeats it
# once per line. That is **fan-out**: a number at a coarse grain, repeated by a join to a finer grain.
#
# ✏️ **Exercise** · Fix it. Roll the lines up to one row per order first, then add the fee once per order. Complete
# `lines` so it returns one row per delivered order with its `net` total.
#
# *Hint:* `SELECT order_id, sum(net_amount) AS net FROM ... WHERE ... GROUP BY order_id`

# %%
def lines_per_order_sql():
    # BEGIN SOLUTION
    return """SELECT order_id, sum(net_amount) AS net
              FROM marts.fct_order_items WHERE order_status = 'delivered'
              GROUP BY order_id"""
    # END SOLUTION

# %%
FIXED = """
    WITH lines AS ({lines})
    SELECT round(sum(l.net + o.delivery_fee)) AS revenue_incl_fees
    FROM lines l JOIN raw.orders o USING (order_id)
"""
REFERENCE = "SELECT order_id, sum(net_amount) AS net FROM marts.fct_order_items WHERE order_status = 'delivered' GROUP BY order_id"
try:
    fixed = q(FIXED.format(lines=lines_per_order_sql())).revenue_incl_fees[0]
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    fixed = q(FIXED.format(lines=REFERENCE)).revenue_incl_fees[0]
expected = q(FIXED.format(lines=REFERENCE)).revenue_incl_fees[0]
print(f"naive: {naive:,.0f} SAR | fixed: {fixed:,.0f} SAR (expected {expected:,.0f}) | overstated by {naive - fixed:,.0f} SAR, {(naive - fixed) / fixed:.1%}")
U.check(fixed == expected, "each order's fee counted once")
U.check(naive > fixed, "the naive join counted each fee once per line")

# %% [markdown]
# 🔍 The number is plausible, so nobody questions it, and that's the danger: a dashboard can be wrong for months. (The fixed
# total is 45 SAR short of revenue + all fees: 5 delivered orders hold only bad lines, so they have no row in the fact.) In the
# model, order-level numbers belong at order grain: Lab 3.2's project has a `fct_orders` with one row per order for this.
#
# ## ✱ Stretch · Measure the National Day uplift per city
#
# For each city, compare delivered revenue on National Day (`is_national_day`) with the average of the other September days.

# %%
def stretch():
    # BEGIN SOLUTION
    display(q("""
        WITH daily AS (
            SELECT d.date_day, d.is_national_day, s.city, sum(f.net_amount) AS revenue
            FROM marts.fct_order_items f
            JOIN marts.dim_date d ON d.date_day = f.order_date
            JOIN marts.dim_stores s USING (store_id)
            WHERE f.order_status = 'delivered' AND d.month = 9
            GROUP BY ALL
        )
        SELECT city,
               round(max(revenue) FILTER (WHERE is_national_day)) AS national_day_sar,
               round(avg(revenue) FILTER (WHERE NOT is_national_day)) AS other_days_avg_sar,
               round(national_day_sar / other_days_avg_sar, 2) AS uplift
        FROM daily GROUP BY city ORDER BY city
    """))
    # END SOLUTION


try:
    stretch()
except NotImplementedError:
    print("✱ Optional: write stretch() above if you have time.")

# %% [markdown]
# ## Checkpoint
#
# 1. Why are three different totals for "how much did we sell?" all correct, and what ends the argument?
# 2. An order has 5 lines and a 9 SAR fee. You join orders to lines and sum the fee. What do you get for that order, and what's it called?
# 3. Why keep a `customer_id = -1` row instead of using an inner join?
#
# <details><summary>Answers</summary>
#
# 1. They answer different questions: all lines, delivered lines, delivered lines plus fees. A written business definition,
#    kept in one model, ends it.
# 2. 45 SAR instead of 9: fan-out, an order-level number repeated once per line.
# 3. An inner join silently drops the sales of erased customers, so revenue no longer matches. The unknown row keeps every sale.
#
# </details>
#
# ## Carry forward
#
# Lab 3.2 builds this same star with the same names (`fct_order_items`, `dim_customers`, `dim_products`, `dim_stores`,
# `dim_date`) and the same keys, as a dbt project that runs itself in the right order. It starts from the raw data, so you
# don't need anything from this notebook.
#
# Want more? `tools/duckdb.ipynb`.
