# %% [markdown]
# # Lab 2.1 · Ingest an API by hand
#
# Day 2 · Session 1 · 25 min
#
# Finance needs Qafila's card and wallet payments in the warehouse every day, refunds included. The payment provider
# only offers an HTTP API. You'll write the loader yourself, so that nothing in the next lab is magic. What you'll do:
# - follow an API's pages and survive its rate limit (a 429, then a backoff);
# - compare a full load with an incremental load that remembers a cursor;
# - prove that running the load twice gives the same table as running it once.
#
# **Tasks**
# - [ ] 1 · Make one request and read the response (3')
# - [ ] 2 · Follow every page (5')
# - [ ] 3 · Survive a 429 with backoff (6')
# - [ ] 4 · Compare a full load with an incremental load (4')
# - [ ] 5 · Prove a re-run doesn't double-load (7')
# - [ ] ✱ Stretch · Keep the rows and the cursor in one transaction

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
import time

import duckdb
import pandas as pd
import requests

# A local stand-in for the payment provider. Its clock lets us pretend time passes: "today" is 15 September.
api = U.PaymentsAPI(page_size=500, rate_limit_every=0, clock="2026-09-15T00:00:00.000Z")
BASE = api.start()
print("payments API running at", BASE)

# %% [markdown]
# ## 1 · Make one request and read the response (3')
#
# An **API** answers HTTP requests with data, here as JSON. This one takes two parameters: `updated_since` (only records
# changed at or after this time) and `page`.

# %%
r = requests.get(f"{BASE}/payments", params={"updated_since": "2026-09-14T00:00:00.000Z", "page": 1}, timeout=10)
body = r.json()
print("status:", r.status_code)
print({k: v for k, v in body.items() if k != "data"})
print("records on this page:", len(body["data"]))
pd.DataFrame(body["data"][:3])

# %% [markdown]
# 🔍 Which field tells you there is more to fetch? What would it hold on the last page? Look at `updated_at`: is it the
# time the payment was made, or the last time it changed?
#
# ## 2 · Follow every page (5')
#
# The API never sends everything at once: each answer holds at most 500 records (a **page**) and `next_page` says what
# to ask for next. `next_page` is `null` on the last page, which Python reads as `None`.
#
# ✏️ **Exercise** · Complete the loop in `fetch_all`: fetch a page with `get`, add its `data` to `rows`, and move `page`
# on to `next_page`.
#
# Hint: three lines. `get(f"{BASE}/payments", {"updated_since": updated_since, "page": page})` returns the JSON body.

# %%
def get_page(url, params):
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()          # raises requests.HTTPError on 4xx and 5xx
    return r.json()


def fetch_all(updated_since, get=get_page):
    rows, page = [], 1
    while page:
        # BEGIN SOLUTION
        body = get(f"{BASE}/payments", {"updated_since": updated_since, "page": page})
        rows += body["data"]
        page = body["next_page"]
        # END SOLUTION
    return rows

# %%
SINCE = "2026-09-12T00:00:00.000Z"
before = api.requests
try:
    rows = fetch_all(SINCE)
except NotImplementedError:
    print("✏️ Not done yet: using the reference loop so the rest of the lab runs.")
    def fetch_all(updated_since, get=get_page):
        rows, page = [], 1
        while page:
            body = get(f"{BASE}/payments", {"updated_since": updated_since, "page": page})
            rows += body["data"]
            page = body["next_page"]
        return rows
    rows = fetch_all(SINCE)
print(f"{len(rows):,} payments since 12 Sep, in {api.requests - before} requests (expected: 2,008 in 5)")
U.check(len(rows) == 2008, "every page fetched")
U.check(len(rows) == len({r["payment_id"] for r in rows}), "no payment twice")

# %% [markdown]
# ## 3 · Survive a 429 with backoff (6')
#
# Providers protect themselves with a **rate limit**: call too often and you get **429 Too Many Requests** instead of
# data. Real ones do this every day, so a loader that dies on a 429 is a loader that dies every night. Turn the limit
# on (every 5th request) and run the same code over a longer period.

# %%
api.rate_limit_every = 5
try:
    fetch_all("2026-09-01T00:00:00.000Z")
except requests.HTTPError as e:
    print("💥", e)
r = None
while r is None or r.status_code != 429:      # ask until we get one, to see what it says
    r = requests.get(f"{BASE}/payments", params={"page": 1}, timeout=10)
print("status:", r.status_code, "| Retry-After:", r.headers["Retry-After"], "| body:", r.json())

# %% [markdown]
# The polite answer is to wait, then try again. **Backoff** means waiting longer after each failure: 1, 2, 4, 8 seconds.
# If the server says how long to wait (the `Retry-After` header), believe it.
#
# ✏️ **Exercise** · Fill in the `429` branch of `get_with_retry`: work out `wait`, sleep, and go round the loop again.
#
# Hint: three lines. `float(r.headers.get("Retry-After", 2 ** attempt))`, then `time.sleep(wait)`, then `continue`.

# %%
def get_with_retry(url, params, tries=6):
    for attempt in range(tries):
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 429:
            # BEGIN SOLUTION
            wait = float(r.headers.get("Retry-After", 2 ** attempt))
            time.sleep(wait)
            continue
            # END SOLUTION
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"gave up after {tries} tries")

# %%
before, throttled = api.requests, api.throttled
try:
    rows = fetch_all("2026-09-01T00:00:00.000Z", get=get_with_retry)
except NotImplementedError:
    print("✏️ Not done yet: using the reference retry so the rest of the lab runs.")
    def get_with_retry(url, params, tries=6):
        for attempt in range(tries):
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", 2 ** attempt)))
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"gave up after {tries} tries")
    rows = fetch_all("2026-09-01T00:00:00.000Z", get=get_with_retry)
print(f"{len(rows):,} payments | {api.requests - before} requests | {api.throttled - throttled} of them answered 429")
U.check(len(rows) == 9720, "all 9,720 payments since 1 Sep arrived")
U.check(api.throttled > throttled, "we hit the rate limit and survived it")

# %% [markdown]
# 🔍 Why not retry straight away, in a tight loop? (You'd keep hitting the limit, and many clients doing that at the same
# moment is how a provider goes down. Real clients also add a little randomness, **jitter**, so they don't all come back
# together.) Which other status codes deserve a retry? (5xx, the server's fault. A 400 or 404 won't get better.)
#
# ## 4 · Compare a full load with an incremental load (4')
#
# A **full load** fetches everything, every time. An **incremental load** fetches only what changed since last time.
# It needs a **cursor**: a value that says how far we got. Here, the highest `updated_at` we've seen.
#
# A day passes. Compare the two. Nothing to write: run it and read the numbers.

# %%
api.rate_limit_every = 25            # be kinder from now on: waiting is boring

def timed_fetch(since):
    before, t0 = api.requests, time.perf_counter()
    rows = fetch_all(since, get=get_with_retry)
    return rows, api.requests - before, time.perf_counter() - t0

held, _, _ = timed_fetch("2026-08-01T00:00:00.000Z")     # what we hold on 15 September
cursor = max(r["updated_at"] for r in held)
print(f"Loaded {len(held):,} payments. Cursor: {cursor}")

api.set_clock("2026-09-16T00:00:00.000Z")                  # one day later
full, full_requests, full_s = timed_fetch("2026-08-01T00:00:00.000Z")
incr, incr_requests, incr_s = timed_fetch(cursor)
pd.DataFrame({"payments fetched": [len(full), len(incr)], "requests": [full_requests, incr_requests],
              "seconds": [round(full_s, 2), round(incr_s, 2)]}, index=["full load", "incremental load"])

# %% [markdown]
# 🔍 The incremental load is far cheaper, and it gets cheaper still as the history grows. So why would anyone run a full
# load? (Small tables, where it's simplest. And a cursor on `updated_at` can never see a row that was **deleted** at the
# source: it's gone, so nothing changed "since". Session 3 solves that.)
#
# ## 5 · Prove a re-run doesn't double-load (7')
#
# Schedulers retry failed jobs, and people press "run" twice. A load is **idempotent** when running it twice gives the
# same result as running it once. First, the version most people write first: just insert what you fetched.

# %%
COLS = ["payment_id", "order_id", "amount", "currency", "method", "status", "card_last4", "created_at", "updated_at"]
api.set_clock("2026-09-15T00:00:00.000Z")
con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
con.sql("""CREATE TABLE naive (payment_id VARCHAR, order_id BIGINT, amount DOUBLE, currency VARCHAR, method VARCHAR,
                               status VARCHAR, card_last4 VARCHAR, created_at VARCHAR, updated_at VARCHAR)""")
batch = pd.DataFrame(fetch_all("2026-08-01T00:00:00.000Z", get=get_with_retry))[COLS]
for run in ["first run", "the scheduler retries"]:
    con.sql("INSERT INTO naive SELECT * FROM batch")
    n, money = con.sql("SELECT count(*), round(sum(amount)) FROM naive").fetchall()[0]
    print(f"{run:24} {n:>7,} rows | {money:>12,.0f} SAR")

# %% [markdown]
# Twice the rows, twice the revenue, and no error anywhere. An **upsert** fixes it: for every key in the batch, the new
# row replaces the old one. A key that isn't in the table yet is simply added.
#
# ✏️ **Exercise** · Complete `load_upsert`, in two statements: delete from `payments` the rows whose `payment_id` is in
# `batch`, then insert everything from `batch`.
#
# Hint: `DELETE FROM payments WHERE payment_id IN (SELECT payment_id FROM batch)`.

# %%
con.sql("CREATE TABLE payments AS SELECT * FROM naive WHERE false")
con.sql("CREATE TABLE ingest_state (cursor VARCHAR)")
con.sql("INSERT INTO ingest_state VALUES ('2026-08-01T00:00:00.000Z')")


def load_upsert(rows):
    batch = pd.DataFrame(rows)[COLS]
    # BEGIN SOLUTION
    con.sql("DELETE FROM payments WHERE payment_id IN (SELECT payment_id FROM batch)")
    con.sql("INSERT INTO payments SELECT * FROM batch")
    # END SOLUTION


def run_once(label):
    """One scheduled run: read the cursor, fetch what changed, upsert it, move the cursor."""
    since = con.sql("SELECT cursor FROM ingest_state").fetchall()[0][0]
    rows = fetch_all(since, get=get_with_retry)
    load_upsert(rows)
    con.sql("UPDATE ingest_state SET cursor = ?", params=[max(r["updated_at"] for r in rows)])
    n, ids, refunds, money = con.sql("""SELECT count(*), count(DISTINCT payment_id),
                                               count(*) FILTER (WHERE status = 'refunded'), round(sum(amount))
                                        FROM payments""").fetchall()[0]
    print(f"{label:22} fetched {len(rows):>6,} | table {n:>6,} rows, {ids:>6,} ids, {refunds:>5,} refunded, {money:>11,.0f} SAR")
    return n, ids, refunds, money

# %%
try:
    first = run_once("run on 15 Sep")
except NotImplementedError:
    print("✏️ Not done yet: using the reference upsert so the rest of the lab runs.")
    def load_upsert(rows):
        batch = pd.DataFrame(rows)[COLS]
        con.sql("DELETE FROM payments WHERE payment_id IN (SELECT payment_id FROM batch)")
        con.sql("INSERT INTO payments SELECT * FROM batch")
    first = run_once("run on 15 Sep")
again = run_once("re-run, same day")
U.check(first == again, "the re-run changed nothing")

api.set_clock("2026-09-29T00:00:00.000Z")           # two weeks later: new payments, and refunds of old ones
later = run_once("run on 29 Sep")
U.check(later[0] == later[1], "still one row per payment")
U.check(later[2] > first[2], "refunds replaced their old 'captured' rows")

# %% [markdown]
# 🔍 The re-run fetched a row anyway. Why? (`updated_since` is **inclusive**, so the newest row comes back every time. We
# keep it inclusive on purpose: two payments can share a timestamp, and `>` could skip the second. The upsert makes the
# overlap harmless.) That is a pattern worth remembering: **overlap a little, and make the load idempotent.**
#
# Khalid's payment for order 5043883 arrived in the last run. And look at what our script quietly ignored.

# %%
display(con.sql("SELECT payment_id, order_id, amount, method, status, updated_at FROM payments WHERE order_id = 5043883").df())
last = fetch_all("2026-09-28T00:00:00.000Z", get=get_with_retry)
print("fields the provider sends that we don't keep:", sorted(set().union(*last) - set(COLS)))

# %% [markdown]
# 🔍 `COLS` was written in August. `wallet_provider` appeared on 15 September, and our loader dropped it without a word.
#
# Count what this script doesn't do yet: no column types (every timestamp is text), no reaction to new fields, no record
# of which run wrote which row, and a cursor kept apart from the data. The next lab has a library do all of it.
#
# ## ✱ Stretch · Keep the rows and the cursor in one transaction
#
# `run_once` writes the rows, then the cursor. If the job dies in between, the next run re-fetches a little (harmless).
# Swap the order and a crash **loses** data: the cursor says "done", the rows never landed. A **transaction** makes both
# writes happen together or not at all. Complete `run_safely`: `BEGIN`, upsert, stop if `crash`, move the cursor,
# `COMMIT`; on any exception, `ROLLBACK` and re-raise.

# %%
class Crash(Exception):
    pass


def run_safely(rows, new_cursor, crash=False):
    # BEGIN SOLUTION
    con.sql("BEGIN")
    try:
        load_upsert(rows)
        if crash:
            raise Crash("the job died here")
        con.sql("UPDATE ingest_state SET cursor = ?", params=[new_cursor])
        con.sql("COMMIT")
    except Exception:
        con.sql("ROLLBACK")
        raise
    # END SOLUTION

# %%
api.set_clock(None)
state = lambda: con.sql("SELECT (SELECT count(*) FROM payments), (SELECT cursor FROM ingest_state)").fetchall()[0]
before_crash = state()
rows = fetch_all(before_crash[1], get=get_with_retry)
try:
    try:
        run_safely(rows, max(r["updated_at"] for r in rows), crash=True)
    except Crash as e:
        print("💥", e)
    U.check(state() == before_crash, f"after the crash: rows and cursor both unchanged {before_crash}")
    run_safely(rows, max(r["updated_at"] for r in rows))
    print("after the clean run:", state())
except NotImplementedError:
    print("✱ Stretch not attempted: that's fine.")

# %%
api.stop()

# %% [markdown]
# ## Checkpoint
#
# **1.** Your job fetched page 40 of 80 when the laptop lost power. You run it again. With what you built today, what
# happens to the 40 pages already loaded?
#
# <details><summary>Answer</summary>They're fetched and upserted again, replacing themselves. Nothing doubles. Only the
# time is wasted.</details>
#
# **2.** Why wait for `Retry-After` on a 429, but not retry a 404?
#
# <details><summary>Answer</summary>A 429 means "not now": the same request will work later. A 404 means "no such thing":
# it will fail the same way every time, and retrying only hides the bug.</details>
#
# **3.** A customer's payment row is deleted at the provider. Does the incremental load notice?
#
# <details><summary>Answer</summary>No. It only asks what changed since the cursor, and a deleted row can't answer. You
# need a periodic full load, or a change feed that says "deleted" (Lab 2.3).</details>
#
# ## Carry forward
#
# You've written pages, retries, a cursor and an upsert by hand. Lab 2.2 does the same job with dlt in a few lines, and
# you'll recognise each piece. Want more? `tools/dlt.ipynb`.
