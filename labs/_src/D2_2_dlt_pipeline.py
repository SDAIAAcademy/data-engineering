# %% [markdown]
# # Lab 2.2 · The same job with dlt
#
# Day 2 · Session 2 · 25 min
#
# In Lab 2.1 you wrote pages, retries, a cursor and an upsert by hand. **dlt** (data load tool) is a Python library that
# does all of that for you. What you'll do:
# - load the payments API into DuckDB incrementally, in a few lines, and re-run it safely;
# - choose a write disposition per table: replace for stores, merge for orders;
# - watch dlt handle a new column, and read the load log it keeps.
#
# **Tasks**
# - [ ] 1 · Write the resource and run the pipeline (7')
# - [ ] 2 · Read what dlt created: tables, columns, cursor (3')
# - [ ] 3 · Re-run it, then prove the merge updated rows (5')
# - [ ] 4 · Load the app tables: replace stores, merge orders (5')
# - [ ] 5 · Spot the new `wallet_provider` column; read the load log (5')
# - [ ] ✱ Stretch · Load the supplier CSVs: what happens in week 39?

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
os.environ["RUNTIME__LOG_LEVEL"] = "ERROR"          # keep dlt's log quiet in the notebook
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.sql_database import sql_table

OUT = U.lab_dir("D2_2_dlt", reset=True)
LAKE_DB = OUT / "qafila_lake.duckdb"                # our destination: a DuckDB file

# The same mock provider as Lab 2.1: pages of 500, a cursor parameter, and a 429 every 25th request. "Today" is 15 September.
api = U.PaymentsAPI(page_size=500, rate_limit_every=25, clock="2026-09-15T00:00:00.000Z")
BASE = api.start()
print("payments API running at", BASE)

# %% [markdown]
# ## 1 · Write the resource and run the pipeline (7')
#
# A dlt **resource** is a Python generator that yields records (or pages of records), with load instructions on its
# decorator. Everything you wrote by hand in Lab 2.1 now has a one-line home:
#
# | Lab 2.1, by hand | dlt |
# |---|---|
# | the `while page` loop, and `get_with_retry` | `RESTClient`: follows the `next` link, retries 429s with backoff |
# | the `ingest_state` cursor | `dlt.sources.incremental("updated_at", ...)`: gives you `.last_value` each run |
# | `load_upsert` | `primary_key="payment_id"`, `write_disposition="merge"` |
# | `CREATE TABLE` and `COLS` | nothing: dlt infers the table from the data |
#
# ✏️ **Exercise** · Complete the resource in one line: yield every page from `/payments`, passing the cursor.
#
# Hint: `yield from client.paginate("/payments", params={"updated_since": updated_at.last_value})`.

# %%
client = RESTClient(base_url=BASE, paginator=JSONLinkPaginator(next_url_path="next"), data_selector="data")


@dlt.resource(name="payments", primary_key="payment_id", write_disposition="merge")
def payments(updated_at=dlt.sources.incremental("updated_at", initial_value="2026-08-01T00:00:00.000Z")):
    # BEGIN SOLUTION
    yield from client.paginate("/payments", params={"updated_since": updated_at.last_value})
    # END SOLUTION


pipe = dlt.pipeline(pipeline_name="qafila_ingest", destination=dlt.destinations.duckdb(str(LAKE_DB)),
                    dataset_name="raw", pipelines_dir=str(OUT / ".dlt"))

# %%
def q(sql):
    """Run SQL against the pipeline's destination and return a DataFrame."""
    with pipe.sql_client() as c:
        with c.execute_query(sql) as cur:
            return cur.df()


try:
    info = pipe.run(payments())
except Exception as e:                             # dlt wraps errors from inside a resource, so look at the message
    if "NotImplementedError" not in repr(e) and "NotImplementedError" not in str(e):
        raise
    print("✏️ Not done yet: using the reference resource so the rest of the lab runs.")
    pipe.drop_pending_packages()

    @dlt.resource(name="payments", primary_key="payment_id", write_disposition="merge")
    def payments(updated_at=dlt.sources.incremental("updated_at", initial_value="2026-08-01T00:00:00.000Z")):
        yield from client.paginate("/payments", params={"updated_since": updated_at.last_value})

    info = pipe.run(payments())
print(info)
loaded = q("SELECT count(*) AS rows, count(DISTINCT payment_id) AS ids FROM raw.payments")
print("expected: 29,526 rows, 29,526 ids")
U.check(loaded.rows[0] == loaded.ids[0] == 29526, "one row per payment_id")
print(f"requests: {api.requests} | 429s dlt retried for you: {api.throttled}")

# %% [markdown]
# 🔍 `pipe.run` did three steps: **extract** (rows to files in a *load package*), **normalize** (infer the schema, pick
# types) and **load** (create the tables, merge the rows). If load fails, the package stays on disk and the next run
# picks it up without calling the API again.
#
# ## 2 · Read what dlt created: tables, columns, cursor (3')
#
# Nothing to write. Three questions to answer from the output: which tables exist, what type did `updated_at` get, and
# where is the cursor?

# %%
q("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' ORDER BY 1""")

# %%
q("""SELECT column_name, data_type FROM information_schema.columns
     WHERE table_schema = 'raw' AND table_name = 'payments' ORDER BY ordinal_position""")

# %%
cursor = pipe.state["sources"]["qafila_ingest"]["resources"]["payments"]["incremental"]["updated_at"]["last_value"]
print("cursor stored in the pipeline's state:", cursor)
q("SELECT version, pipeline_name, created_at FROM raw._dlt_pipeline_state ORDER BY version")

# %% [markdown]
# 🔍 `updated_at` is a real timestamp, not text: dlt recognised the format. `_dlt_load_id` says which load wrote each
# row. The cursor lives in `_dlt_pipeline_state`, **in the destination, in the same load as the data**: it moves with the
# rows or not at all, which is what the stretch of Lab 2.1 had to build by hand. Delete the local folder, and the next
# run reads it back from the database.
#
# ## 3 · Re-run it, then prove the merge updated rows (5')
#
# Run it again without changing anything. Then move the clock forward two weeks: new payments, plus refunds that update
# old ones.

# %%
rows_before = q("SELECT count(*) AS n FROM raw.payments").n[0]
pipe.run(payments())
rows_rerun = q("SELECT count(*) AS n FROM raw.payments").n[0]
U.check(rows_rerun == rows_before, f"the re-run changed nothing ({rows_before:,} rows)")

api.set_clock("2026-09-29T00:00:00.000Z")          # two weeks later
pipe.run(payments())
after = q("SELECT count(*) AS rows, count(DISTINCT payment_id) AS ids FROM raw.payments")
print(f"on 29 Sep: {after.rows[0]:,} rows, {after.ids[0]:,} distinct payment ids")
q("SELECT payment_id, order_id, amount, method, status FROM raw.payments WHERE order_id = 5043883")

# %% [markdown]
# ✏️ **Exercise** · Prove the merge really **updated** rows instead of appending them. Payments created before
# 15 September were all loaded by the first run. Complete the query: how many of them now carry a **different**
# `_dlt_load_id` than the first load's?
#
# Hint: one `SELECT count(*) ... WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}'`.

# %%
first_load = q("SELECT min(load_id) AS id FROM raw._dlt_loads").id[0]


def count_updated_by_merge():
    # BEGIN SOLUTION
    return q(f"""
        SELECT count(*) AS n FROM raw.payments
        WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}'
    """).n[0]
    # END SOLUTION

# %%
try:
    updated_by_merge = count_updated_by_merge()
except NotImplementedError:
    print("✏️ Not done yet: using the reference query so the rest of the lab runs.")
    updated_by_merge = q(f"""SELECT count(*) AS n FROM raw.payments
        WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}'""").n[0]
print(f"old payments replaced by the merge: {updated_by_merge:,} (expected: 61)")
U.check(updated_by_merge == 61, "61 old payments were updated in place")
U.check(after.rows[0] == after.ids[0], "still exactly one row per payment")
q(f"""SELECT status, count(*) AS n FROM raw.payments
      WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}' GROUP BY ALL""")

# %% [markdown]
# ## 4 · Load the app tables: replace stores, merge orders (5')
#
# The **write disposition** says what a run does with rows already in the table:
#
# | Disposition | A run… | Use it for |
# |---|---|---|
# | `append` | adds its rows | events and logs, where every row is new |
# | `replace` | swaps the table for its rows | small tables you can resend whole |
# | `merge` | upserts on the `primary_key` | records that change: payments, orders |
#
# dlt's `sql_table` reads a database table the same way. Stores are a small list: replace them each run. Orders are big
# and growing: load them incrementally and merge.
#
# ✏️ **Exercise** · Make `orders_table` return an incremental, merged `sql_table` on `updated_at`, primary key `order_id`.
# Start from `2026-09-25 00:00:00` so the first run is small.
#
# Hint: copy the `stores` call, then add `incremental=dlt.sources.incremental("updated_at", initial_value="2026-09-25 00:00:00")`,
# `primary_key="order_id"`, `write_disposition="merge"`.

# %%
APP_DB = f"sqlite:///{(U.RAW / 'qafila_oltp.sqlite').as_posix()}"
stores = sql_table(credentials=APP_DB, table="stores", write_disposition="replace", backend="pyarrow")


def orders_table():
    # BEGIN SOLUTION
    return sql_table(credentials=APP_DB, table="orders", backend="pyarrow",
                     incremental=dlt.sources.incremental("updated_at", initial_value="2026-09-25 00:00:00"),
                     primary_key="order_id", write_disposition="merge")
    # END SOLUTION

# %%
try:
    orders = orders_table()
except NotImplementedError:
    print("✏️ Not done yet: using the reference table so the rest of the lab runs.")
    orders = sql_table(credentials=APP_DB, table="orders", backend="pyarrow",
                       incremental=dlt.sources.incremental("updated_at", initial_value="2026-09-25 00:00:00"),
                       primary_key="order_id", write_disposition="merge")
pipe.run([stores, orders])
n1 = q("SELECT count(*) AS n FROM raw.orders").n[0]
pipe.run([stores, orders])
n2 = q("SELECT count(*) AS n FROM raw.orders").n[0]
n_stores = q("SELECT count(*) AS n FROM raw.stores").n[0]
print(f"orders {n1:,} -> {n2:,} after a re-run | stores {n_stores} (expected: orders stay the same, stores 12)")
U.check(n1 == n2, "re-running the orders load adds nothing")
U.check(n_stores == 12, "stores replaced, not duplicated")

# %% [markdown]
# 🔍 A merge only touches the keys it receives. If a store closes and leaves the list, why does `replace` notice, when
# `merge` never would? And if an order row is **deleted** in the app, does this cursor see it? (No. Lab 2.3 handles that.)
#
# ## 5 · Spot the new `wallet_provider` column; read the load log (5')
#
# On 15 September the provider started sending `wallet_provider`. Nobody told us. Your Lab 2.1 script dropped it. dlt?

# %%
q("""SELECT column_name, data_type FROM information_schema.columns
     WHERE table_schema = 'raw' AND table_name = 'payments' AND column_name = 'wallet_provider'""")

# %%
q("""SELECT method, wallet_provider, count(*) AS n FROM raw.payments
     WHERE method = 'wallet' GROUP BY ALL ORDER BY n DESC""")

# %% [markdown]
# 🔍 dlt **evolved** the table: a new column, `NULL` on older rows. Nothing failed and nothing was lost. Good for bronze,
# and also a risk: nobody was told. Downstream checks have to catch surprises (Day 3).
#
# Every load is recorded. `_dlt_loads` has one row per load, and every data row carries the `_dlt_load_id` of the load
# that wrote it. So you can always answer "where did this number come from?"

# %%
load_log = q("""
    SELECT l.load_id, l.inserted_at, count(p._dlt_id) AS payments_rows
    FROM raw._dlt_loads l LEFT JOIN raw.payments p ON p._dlt_load_id = l.load_id
    GROUP BY ALL ORDER BY l.inserted_at
""")
U.check(load_log.payments_rows.sum() == after.rows[0], "every payment row is traceable to a load")
load_log

# %% [markdown]
# 🔍 The first load inserted 29,526 rows but now "owns" fewer: the refunds of the later load replaced some. Why are there
# no rows for the re-run in Task 3? (It found nothing new, so it wrote no payments.) The last loads are the app tables.
#
# ## ✱ Stretch · Load the supplier CSVs: what happens in week 39?
#
# Load the four weekly price files with a resource (append, one file per run, in order) and inspect the columns after
# each run. What did dlt do with week 38's `"9,49"` values, and with week 39's `cost_sar`?

# %%
import csv


def load_suppliers():
    # BEGIN SOLUTION
    @dlt.resource(name="supplier_prices", write_disposition="append")
    def supplier_file(path):
        with open(path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                for k in ("unit_cost", "cost_sar"):
                    if k in row:
                        try:
                            row[k] = float(row[k])
                        except ValueError:
                            pass   # leave unparseable text as it is
                row["_file"] = Path(path).name
                yield row

    for path in sorted((U.RAW / "suppliers").glob("prices_*.csv")):
        pipe.run(supplier_file(path))
    return q("SELECT _file, count(unit_cost) AS unit_cost, count(unit_cost__v_text) AS unit_cost_text, "
             "count(cost_sar) AS cost_sar FROM raw.supplier_prices GROUP BY ALL ORDER BY _file")
    # END SOLUTION

# %%
try:
    display(load_suppliers())
except NotImplementedError:
    print("✱ Stretch not attempted: that's fine.")

# %% [markdown]
# 🔍 dlt never fails on these files: week 38's text lands in a **variant column** `unit_cost__v_text`, and week 39's
# renamed column becomes a new column, leaving `unit_cost` empty. Bronze is complete, but a naive report on `unit_cost`
# silently loses two weeks. Tolerant ingestion needs strict checks downstream (Lab 3.3).

# %%
api.stop()

# %% [markdown]
# ## Checkpoint
#
# **1.** Where does dlt keep the incremental cursor, and why does it matter that it's committed with the data?
#
# <details><summary>Answer</summary>In the destination, in <code>_dlt_pipeline_state</code>, written in the same load as
# the rows. The cursor can never be ahead of the data (rows lost) or behind it by surprise, even if the machine running
# the job is replaced.</details>
#
# **2.** Which disposition would you pick for delivery events, for the list of stores, and for orders?
#
# <details><summary>Answer</summary>Events: <code>append</code>, every row is new. Stores: <code>replace</code>, small and
# sent whole, so a closed store disappears. Orders: <code>merge</code> on <code>order_id</code>, because rows change.</details>
#
# **3.** The provider renames a column without warning. What does dlt do, and whose job is it to notice?
#
# <details><summary>Answer</summary>dlt adds the new column and leaves the old one empty on new rows. The load succeeds,
# so it's your job: a data-quality check on silver has to fail loudly (Lab 3.3).</details>
#
# ## Carry forward
#
# You can load APIs and databases into bronze, safely. A cursor still can't see deletes, or the versions between two runs.
# Lab 2.3 reads a change feed that can. Want more? `tools/dlt.ipynb`.
