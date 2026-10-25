# %% [markdown]
# # dbt, from an empty folder to a tested report
#
# Day 2, before Lab 2.3 · CPU only. Finance asks how much delivered product revenue came from each store city. You
# build that report as a small dbt project, file by file, and read every piece of SQL dbt generates, so nothing in
# Lab 2.3's big project is magic. §9, the tests, opens session D2_4.
#
# **The data:** a few invented rows, small enough to check by hand (SAR, without delivery fees).
#
# 🤔 asks you to predict before you run a cell · 🔍 says what the output means · ▶ players replay what dbt just did,
# one step at a time · ➕ parts are for your own pace.
#
# **Contents**
# - [ ] 1 · Raw data: what dbt does not load
# - [ ] 2 · The problem: three SQL files, run by hand
# - [ ] 3 · The mental model: a graph of `SELECT`s
# - [ ] 4 · A project, a profile and its sources
# - [ ] 5 · A `SELECT` becomes a view
# - [ ] 6 · `ref()` builds a graph, and the graph sets the order
# - [ ] 7 · Materializations: view, table, incremental; break one on purpose
# - [ ] 8 · Macros: write an expression once
# - [ ] 9 · Tests: a build that succeeds is not a correct build (opens D2_4)
# - [ ] 10 · Lineage and documentation
# - [ ] 11 · Summary

# %% [markdown] 🎓
# 🎓 **Instructor notes** (instructor copy only)
#
# - Split over two sessions. **D2_3** = §1–8, 10–11, core ≈ 27': §2 4' · §3 2' · §4 2' · §5 3' · §6 5' · §7 7'
#   (7a–b; 7c–d ➕) · §8 2' · §10 1' · §11 1'. §1 runs before class. **D2_4 opens with §9** (10': 9a–c; 9d ➕), then
#   `tools/pandera`. Run the notebook down to §9 before D2_4 (a few minutes). Lab 2.3 §3 applies 7b to the real fact
#   without re-teaching it.
# - §6 writes `stg_stores` and `mart_city_sales` in walked cells: ask the room for the `source()` and `ref()`
#   arguments before running them. Every later cell needs both models.
# - In class, run the cells of a ➕ part without stopping on them: later cells may use what they build.
# - §9's warn / contract / unit-test cells teach the mechanisms Lab 2.4 uses, on different cases: Lab 2.4 writes a
#   `relationships` test with `severity: warn`, breaks a contract with a type change, and unit-tests mobile numbers.
# - Every `dbt(...)` call takes a few seconds in process; a full run of this notebook is a few minutes.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                    "duckdb>=1.4", "dbt-core>=1.10,<2", "dbt-duckdb>=1.11"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # tools/ and solutions/tools/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import json
import textwrap
import duckdb
from IPython.display import display
import de_utils as U
import de_viz as V

# Rerunning THIS cell starts the lab again from an empty folder (it only deletes this lab's own outputs).
OUT = U.lab_dir("tools_dbt", reset=True)
PROJ = OUT / "qafila_intro"
PROJ.mkdir()
DB = OUT / "qafila_intro.duckdb"
print("Empty project folder:", PROJ, "| files:", list(PROJ.iterdir()))


def write_file(relative_path, content):
    path = PROJ / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    print("Wrote", relative_path)


def sql(query):
    # Each query opens and closes its own connection, so the database is free when dbt opens it.
    with duckdb.connect(str(DB)) as connection:
        return connection.sql(query).df()


def execute_sql(statement):
    with duckdb.connect(str(DB)) as connection:
        connection.execute(statement)

# %% [markdown]
# dbt **transforms** inside the warehouse (silver in `staging/`, gold in `marts/`) and **tests** what it built. The
# documentation and lineage come from the same project.

# %%
V.platform_map(tool="dbt")

# %% [markdown]
# ## 1 · Raw data: what dbt does not load
#
# This cell plays the **ingestion step** (dlt in Lab 1.3): it loads three tables into the schema `raw`, a namespace
# inside the DuckDB file. dbt never loads data.

# %%
RAW_SQL = """
CREATE SCHEMA raw;
CREATE TABLE raw.stores (store_id VARCHAR, city VARCHAR);
INSERT INTO raw.stores VALUES ('RUH-04', 'Riyadh'), ('JED-03', 'Jeddah');

CREATE TABLE raw.orders (order_id BIGINT, store_id VARCHAR, status VARCHAR, updated_at TIMESTAMP);
INSERT INTO raw.orders VALUES
    (101, 'RUH-04', 'delivered', '2026-09-20 09:00:00'),
    (102, 'JED-03', 'delivered', '2026-09-20 10:00:00'),
    (103, 'JED-03', 'cancelled', '2026-09-20 11:00:00');

CREATE TABLE raw.order_items (
    order_id BIGINT, line_no INTEGER, quantity INTEGER, unit_price DECIMAL(12,2), discount DECIMAL(12,2)
);
INSERT INTO raw.order_items VALUES
    (101, 1, 1, 33.46, 0.00),
    (101, 2, 2, 121.20, 0.00),
    (102, 1, 1, 33.65, 0.00),
    (103, 1, 1, 12.00, 0.00);
"""
execute_sql(RAW_SQL)
display(sql("SELECT * FROM raw.orders ORDER BY order_id"))
display(sql("SELECT * FROM raw.order_items ORDER BY order_id, line_no"))

# %% [markdown]
# **The answer to check against.** Revenue = delivered lines with a positive quantity, worth
# `quantity * unit_price - discount`. Riyadh (order 101): 33.46 + 2 × 121.20 = **275.86**. Jeddah: **33.65** (order
# 103 is cancelled).
#
# ## 2 · The problem: three SQL files, run by hand
#
# First, the way many teams start: a folder of SQL files, each creating a table from the one before
# (`clean_orders.sql` → `order_sales.sql` → `city_sales.sql`). They run on an in-memory copy of the raw data, so the
# real one stays untouched.

# %%
FILES = {
    "clean_orders.sql": """
        CREATE OR REPLACE TABLE orders_clean AS
        SELECT order_id, store_id, status FROM raw.orders
    """,
    "order_sales.sql": """
        CREATE OR REPLACE TABLE order_sales AS
        SELECT o.order_id, o.store_id, o.status, i.quantity * i.unit_price - i.discount AS net_amount
        FROM raw.order_items i JOIN orders_clean o ON i.order_id = o.order_id
    """,
    "city_sales.sql": """
        CREATE OR REPLACE TABLE city_sales AS
        SELECT s.city, SUM(f.net_amount) AS revenue_sar
        FROM order_sales f JOIN raw.stores s ON f.store_id = s.store_id
        WHERE f.status = 'delivered'
        GROUP BY s.city
    """,
}


def run_by_hand(con, *names):
    for name in names:
        con.execute(FILES[name])
        print("ran", name)

# %% [markdown]
# 🤔 **Predict** · A scheduler runs every `.sql` file in the folder, in alphabetical order, on a new database. What happens?

# %%
by_hand = duckdb.connect()   # in memory
by_hand.execute(RAW_SQL)
try:
    run_by_hand(by_hand, *sorted(FILES))
except duckdb.Error as e:
    print("❌", str(e).splitlines()[0])

# %% [markdown]
# 🔍 `city_sales.sql` comes first alphabetically, and `order_sales` doesn't exist yet. The files depend on each other,
# but nothing in the folder says so: the order lives in someone's head. Run them in the right order:

# %%
run_by_hand(by_hand, "clean_orders.sql", "order_sales.sql", "city_sales.sql")
by_hand.sql("FROM city_sales ORDER BY city").df()

# %% [markdown]
# Correct. Now order 103 is **delivered** after all, and Finance asks for a fresh report, so you rerun the report file.
#
# 🤔 **Predict** · Does Jeddah now include order 103's 12 SAR?

# %%
by_hand.execute("UPDATE raw.orders SET status = 'delivered' WHERE order_id = 103")
run_by_hand(by_hand, "city_sales.sql")
stale = by_hand.sql("SELECT revenue_sar FROM city_sales WHERE city = 'Jeddah'").fetchall()[0][0]
U.check(float(stale) == 33.65, "no error, and Jeddah is still 33.65: the report was rebuilt from old tables")

# %% [markdown]
# 🔍 The report reran, but it read `order_sales`, which was built from `orders_clean`, which was built **before** the
# update. Follow order 103 through the tables:

# %%
V.table_steps([
    {"title": "raw.orders (now)", "df": by_hand.sql("SELECT order_id, status FROM raw.orders ORDER BY 1").df(),
     "new": ["status"]},
    {"title": "orders_clean (built earlier)", "df": by_hand.sql("SELECT order_id, status FROM orders_clean ORDER BY 1").df(),
     "new": ["status"]},
    {"title": "order_sales (built earlier)",
     "df": by_hand.sql("SELECT order_id, status, net_amount FROM order_sales ORDER BY 1").df(), "new": ["status"]},
    {"title": "city_sales (rebuilt just now)", "df": by_hand.sql("FROM city_sales ORDER BY city").df()},
])

# %% [markdown]
# To get it right you had to know the chain and rerun all three files in order. With 3 files that's annoying; with
# 300 it's how wrong numbers reach a dashboard.
#
# Every file also repeats `CREATE OR REPLACE TABLE ... AS` around the only part that matters, the `SELECT`. Here is
# the same `order_sales` as a **dbt model**:

# %%
by_hand.close()
V.sql_steps([
    ("By hand · order_sales.sql", FILES["order_sales.sql"]),
    ("With dbt · models/order_sales.sql", """
        SELECT o.order_id, o.store_id, o.status, i.quantity * i.unit_price - i.discount AS net_amount
        FROM {{ source('raw', 'order_items') }} i JOIN {{ ref('orders_clean') }} o ON i.order_id = o.order_id
    """, "no CREATE: only the SELECT. ref() says what it reads"),
])

# %% [markdown]
# ## 3 · The mental model: a graph of `SELECT`s
#
# You write files; dbt reads them all, draws the graph, turns each `SELECT` into SQL for the warehouse and sends it
# there in graph order. Saving a file changes nothing in the database: only a dbt command (`dbt run`, `dbt build`)
# does.
#
# | You declare | dbt does |
# |---|---|
# | a `SELECT` in a file named after the result | wraps it in `CREATE VIEW` / `CREATE TABLE` / `INSERT`, under a temporary name, then swaps it in |
# | what it reads: `source()` for raw tables, `ref()` for models | draws the graph, builds parents before children, independent branches in parallel |
# | how to store it: `view`, `table`, `incremental` | writes the right SQL for this database (the **adapter**) |
# | what must be true: tests, contracts | runs each test right after its model, and skips what depends on a failure |
# | descriptions | documentation and lineage, from the same files |
#
# dbt is a **compiler and a runner**, not a database: it never stores a row, and it never loads data. The model design
# (what one row means, which rows count as revenue) is still yours (Lab 2.2).
#
# ## 4 · A project, a profile and its sources
#
# Two files make a folder a dbt project: `dbt_project.yml` says what the project is and how to build its models;
# `profiles.yml` says which database runs the SQL.

# %%
write_file("dbt_project.yml", """
name: qafila_intro
version: '1.0.0'
config-version: 2
profile: qafila_intro
model-paths: [models]
test-paths: [tests]
models:
  qafila_intro:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: marts
      +materialized: table
""")
write_file("profiles.yml", f"""
qafila_intro:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '{DB.as_posix()}'
      schema: analytics
      threads: 1
""")
(PROJ / "models").mkdir(exist_ok=True)
(PROJ / "tests").mkdir(exist_ok=True)

# %% [markdown]
# **`dbt_project.yml`**, key by key:
#
# | Key | Means |
# |---|---|
# | `name` | the project's name (it reappears under `models:`) |
# | `version` | your project's version: a label, nothing checks it |
# | `config-version: 2` | the format of this file; always 2 |
# | `profile` | which entry of `profiles.yml` to connect with |
# | `model-paths`, `test-paths` | the folders holding models and tests (these are the defaults, written out) |
# | `models:` → `qafila_intro:` → `staging:` | settings for every model in `models/staging/` |
# | `+materialized` | how those models are stored: `view`, `table`, ... (§7) |
# | `+schema` | the schema they go to, **appended** to the profile's: `analytics` + `staging` → `analytics_staging` |
#
# The `+` marks a setting, so dbt doesn't read `materialized` as a folder name. A model can override its folder's
# settings with `{{ config(...) }}` at the top of its file (§7).
#
# **`profiles.yml`**:
#
# | Key | Means |
# |---|---|
# | `qafila_intro` | the profile's name, matching `profile:` above |
# | `target: dev` | which output to use unless you pass `--target prod` |
# | `outputs:` → `dev:` | one block per environment (dev, prod, ...) |
# | `type: duckdb` | the adapter: which database dbt generates SQL for (package `dbt-duckdb`) |
# | `path` | the DuckDB file (a server database would have `host`, `user`, `password` instead) |
# | `schema: analytics` | where dbt writes its models (see below) |
# | `threads: 1` | how many models dbt may build at the same time |
#
# **Why `schema: analytics`?** Ingestion writes to `raw`; dbt only reads it and writes under `analytics`. So a
# `dbt run` can never overwrite source data, and anything under `analytics` can be rebuilt from `raw`. The folder
# settings split it further, so the schema tells readers what they're looking at:
#
# | Schema | Holds | Who reads it |
# |---|---|---|
# | `raw` | source tables, as loaded | ingestion writes; dbt reads |
# | `analytics_staging` | cleaned copies, one per source | other models |
# | `analytics_marts` | facts and reports | analysts, dashboards |
#
# The same split keeps environments apart: each developer's `dev` output gets their own schema (`schema: dev_sara`
# gives `dev_sara_staging`), and only the `prod` output writes to `analytics`.
#
# **Not only DuckDB.** `type` picks an **adapter**, a package that teaches dbt one database's SQL:
#
# | Database | Package | Connection keys |
# |---|---|---|
# | DuckDB, MotherDuck | `dbt-duckdb` | `path` (a file, or `md:` for the cloud) |
# | PostgreSQL | `dbt-postgres` | `host`, `port`, `user`, `password`, `dbname` |
# | Snowflake | `dbt-snowflake` | `account`, `user`, `role`, `warehouse`, `database` |
# | BigQuery | `dbt-bigquery` | `project`, `dataset`, `method` (how to log in) |
# | Databricks, Spark | `dbt-databricks`, `dbt-spark` | `host`, `http_path`, `token` |
#
# A profile can have a DuckDB output for `dev` and a server for `prod`: `dbt build --target prod` then builds the
# same models there, with the password read from the environment (`"{{ env_var('DBT_PROD_PASSWORD') }}"`), never
# written in the file.
#
# Each `dbt(...)` call below is the same command in a terminal: `dbt("debug")` is `dbt debug --profiles-dir .`. It
# checks the connection and builds nothing.

# %%
from dbt.cli.main import dbtRunner


def dbt(*args, expect_success=True, log=True):
    """Run a dbt command. log=True prints one line per model or test, "full" prints dbt's own log, False nothing."""
    flags = ["--no-use-colors", "--no-send-anonymous-usage-stats"] + ([] if log == "full" else ["--log-level", "none"])
    # --log-path keeps dbt.log inside the project; without it dbt writes logs/ next to the notebook.
    paths = ["--project-dir", str(PROJ), "--profiles-dir", str(PROJ), "--log-path", str(PROJ / "logs")]
    result = dbtRunner().invoke([*flags, *args, *paths])
    command = "dbt " + " ".join(args)
    if log is True:
        summarize(result)
    if expect_success and not result.success:
        raise RuntimeError(command + " failed" + ("" if log else ": run it again with log='full'")) from result.exception
    if log:
        U.check(result.success == expect_success, command + (" succeeded" if result.success else " failed, as expected"))
    return U.DbtResult(result)   # like dbt's result, without its page-long printout when a cell ends with dbt(...)


def summarize(result):
    """The lines of dbt's log that say something: each model or test with its status, then the totals."""
    if result.exception is not None:
        print("ERROR", result.exception)
    results = getattr(result.result, "results", None)
    if not results or result.result.args.get("which") not in ("run", "build", "test"):
        return
    counts = {"PASS": 0, "WARN": 0, "ERROR": 0, "SKIP": 0}
    for r in results:
        status = str(r.status).upper()
        status = {"SUCCESS": "OK", "FAIL": "ERROR", "SKIPPED": "SKIP"}.get(status, status)
        ok = status in ("OK", "PASS")
        counts["PASS" if ok else status] = counts.get("PASS" if ok else status, 0) + 1
        kind = r.node.config.materialized if r.node.resource_type == "model" else r.node.resource_type
        detail = f"  {r.message}" if not ok and r.message else ""
        print(f"{status:5} {kind:11} {r.node.name}{detail}")
    print("Done.", " ".join(f"{k}={v}" for k, v in counts.items()))


def generated(kind, path):
    """A SQL file dbt wrote: kind is "compiled" (the rendered SELECT) or "run" (the SQL it executed)."""
    return (PROJ / "target" / kind / "qafila_intro" / path).read_text(encoding="utf-8")


dbt("debug", log="full")

# %% [markdown]
# 🔍 If a check fails: the adapter isn't installed, the profile name doesn't match, the path is wrong, or another
# program holds the database.
#
# **Sources** give the raw tables a name that models use: `source('qafila_raw', 'orders')`.

# %%
write_file("models/staging/_sources.yml", """
version: 2                     # the format of dbt's YAML files
sources:
  - name: qafila_raw           # source()'s first argument
    schema: raw                # where the tables really are
    tables:                    # each name is a second argument
      - name: orders
      - name: order_items
      - name: stores
""")
print(dbt("ls", "--resource-type", "source", "--output", "name", log=False).result)
display(sql("SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY 1, 2"))
U.check(len(sql("FROM information_schema.tables")) == 3, "still the same 3 raw tables: a source declaration loads nothing")

# %% [markdown]
# ## 5 · A `SELECT` becomes a view
#
# The file name `stg_orders.sql` becomes the view's name; its folder makes it a view. (`stg_` = staging: one model per
# source table, rename and cast, no business logic.)
#
# 🤔 **Predict** · Does `dbt compile` create the view?

# %%
write_file("models/staging/stg_orders.sql", """
SELECT order_id, store_id, status, updated_at
FROM {{ source('qafila_raw', 'orders') }}
""")
dbt("compile", "--select", "stg_orders")
V.sql_steps([
    ("We write · models/staging/stg_orders.sql", (PROJ / "models/staging/stg_orders.sql").read_text()),
    ("dbt compile · target/compiled/…/stg_orders.sql", generated("compiled", "models/staging/stg_orders.sql"),
     "Jinja replaced by the table's full name: database.schema.table"),
])

# %%
U.check(sql("FROM information_schema.tables WHERE table_name = 'stg_orders'").empty,
        "compile wrote SQL to target/compiled/, but created nothing in the database")

# %% [markdown]
# 🔍 `{{ ... }}` is **Jinja**: dbt fills it in before any SQL exists, and `compile` stops there. Use it to read the SQL
# dbt will send.
#
# 🤔 **Predict** · We wrote no `CREATE VIEW` and no `CREATE SCHEMA`. What will `dbt run` do?

# %%
tables_before = sql("SELECT table_schema, table_name, table_type FROM information_schema.tables")
schemas_before = set(sql("SELECT schema_name FROM information_schema.schemata").schema_name)
dbt("run", "--select", "stg_orders", log="full")
tables_after = sql("SELECT table_schema, table_name, table_type FROM information_schema.tables")
schemas_after = set(sql("SELECT schema_name FROM information_schema.schemata").schema_name)
V.database_map(tables_after, before=tables_before)

# %% [markdown]
# 🔍 That is dbt's log, as a terminal shows it: a `START` and an `OK` line per model, then the totals. From here on
# the notebook prints only the result lines: status, materialization, name.

# %%
V.sql_steps([
    ("We write · stg_orders.sql", (PROJ / "models/staging/stg_orders.sql").read_text()),
    ("dbt compiles", generated("compiled", "models/staging/stg_orders.sql")),
    ("dbt runs · target/run/…/stg_orders.sql", generated("run", "models/staging/stg_orders.sql"),
     "the same SELECT, wrapped in the SQL of a view"),
])

# %%
U.check("analytics_staging" in schemas_after - schemas_before, "dbt created the missing schema analytics_staging")
U.check(sql("SELECT table_type FROM information_schema.tables WHERE table_name = 'stg_orders'").table_type[0] == "VIEW",
        "stg_orders is a view: its folder's default")

# %% [markdown]
# 🔍 dbt builds under a temporary name (`__dbt_tmp`) and renames at the end, so nobody reads a half-built model.
#
# ## 6 · `ref()` builds a graph, and the graph sets the order
#
# `stg_order_items` gives each line an id (`101-2`) and its `net_amount`: one row per line. `stg_stores` reads the
# stores the same way.

# %%
write_file("models/staging/stg_order_items.sql", """
SELECT
    CAST(order_id AS VARCHAR) || '-' || CAST(line_no AS VARCHAR) AS order_line_id,
    order_id, line_no, quantity, unit_price, discount,
    quantity * unit_price - discount AS net_amount
FROM {{ source('qafila_raw', 'order_items') }}
""")
write_file("models/staging/stg_stores.sql", """
SELECT store_id, city
FROM {{ source('qafila_raw', 'stores') }}
""")
dbt("run", "--select", "stg_stores")
U.check(len(sql("FROM analytics_staging.stg_stores")) == 2, "stg_stores has the 2 stores")
U.check('"raw"."stores"' in generated("compiled", "models/staging/stg_stores.sql"), "source() resolved to raw.stores")

# %% [markdown]
# The **fact**: one row per line, with its order's store and status. Models read other models with `ref()`. The fact
# keeps every status; the report decides what counts.

# %%
FACT_SELECT = """
SELECT
    i.order_line_id, i.order_id, i.line_no,
    o.store_id, o.status, o.updated_at,
    i.quantity, i.net_amount
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('stg_orders') }} o ON i.order_id = o.order_id
WHERE i.quantity > 0
"""
write_file("models/marts/fct_order_items.sql", FACT_SELECT)

# %% [markdown]
# Finance's report joins the fact to the stores, keeps the delivered lines and sums them per city.

# %%
write_file("models/marts/mart_city_sales.sql", """
SELECT s.city, SUM(f.net_amount) AS revenue_sar
FROM {{ ref('fct_order_items') }} f
JOIN {{ ref('stg_stores') }} s ON f.store_id = s.store_id
WHERE f.status = 'delivered'
GROUP BY s.city
""")

# %% [markdown]
# 🤔 **Predict** · `+mart_city_sales` = the report **and everything it needs**. In what order will dbt build them?

# %%
dbt("run", "--select", "+mart_city_sales")
display(sql("SELECT * FROM analytics_marts.mart_city_sales ORDER BY city"))

# %%
city = lambda name: round(float(sql(f"SELECT revenue_sar FROM analytics_marts.mart_city_sales WHERE city = '{name}'").revenue_sar[0]), 2)
U.check(city("Riyadh") == 275.86, "Riyadh matches the hand calculation")
U.check(city("Jeddah") == 33.65, "cancelled order 103 contributes nothing to Jeddah")

# %% [markdown]
# ▶ **Watch it run.** `V.animate_dbt` replays the last command from `target/manifest.json` (the graph) and
# `target/run_results.json` (what ran). Press **Play**, or **Next**. (Buttons do nothing? Run the cell again.)

# %%
V.animate_dbt(PROJ)

# %% [markdown]
# 🔍 Each `ref()` is an arrow. dbt reads them all first, then runs each model after its parents. The staging views
# don't depend on each other, so any order works (with more threads, in parallel).
#
# 🧪 **Experiment** · A model that names the fact's table directly, without `ref()`. What are its parents?

# %%
write_file("models/marts/mart_hardcoded.sql", "SELECT COUNT(*) AS n_lines FROM analytics_marts.fct_order_items")
print("+mart_city_sales →", dbt("ls", "--select", "+mart_city_sales", "--output", "name", log=False).result)
print("+mart_hardcoded  →", dbt("ls", "--select", "+mart_hardcoded", "--output", "name", log=False).result)
(PROJ / "models/marts/mart_hardcoded.sql").unlink()

# %% [markdown]
# 🔍 None. dbt may build it before the fact, `fct_order_items+` won't refresh it, and the lineage loses it. Always
# `ref()` models and `source()` raw tables.
#
# | `--select` | Selects |
# |---|---|
# | `fct_order_items` | that model |
# | `+fct_order_items` | it and its parents |
# | `fct_order_items+` | it and its children |
# | `staging` | the `models/staging/` folder |
#
# The folders are **layers**: dbt's **silver** and **gold** zones. **Bronze** is the `raw` schema, which dbt only
# reads. In Lab 2.3's project:
#
# | Folder | Qafila example (Lab 2.3) | Its job | Zone |
# |---|---|---|---|
# | `staging/` | `stg_orders` | one model per source table: rename, cast, derive the order date; no business logic | silver |
# | `intermediate/` | `int_payments_per_order` | reshape for reuse: payment attempts → one row per order | silver |
# | `marts/` | `fct_order_items`, `fct_orders`, `dim_customers` | facts and dimensions at a declared grain | gold |
# | `marts/` | `mart_daily_city_sales` | a report for one business question | gold |
# | `quarantine/` | `qrn_order_items` | the rows kept out of the facts, each with its reason | beside silver |
#
# Careful with the name: `stg_` models are **cleaned**, although a warehouse's "stage" area usually means the landed
# copy. And the payments become one row per order **before** they meet the orders, so several attempts can't multiply
# an order's amount (the fan-out of Lab 2.2).
#
# ## 7 · Materializations: view, table, incremental; break one on purpose
#
# | `materialized=` | Stores | The work happens |
# |---|---|---|
# | `view` | a query | on every read |
# | `table` | rows, rebuilt each run | at `dbt run`, over all rows |
# | `incremental` | rows, updated each run | at `dbt run`, over the rows its filter selects |
# | `ephemeral` | nothing (pasted into its readers as a CTE) | inside the readers |
#
# ### 7a · A view sees a change at once; a table needs a rebuild
#
# 🤔 **Predict** · Order 103 is now delivered in the source. Which shows it first: the view or the table?

# %%
execute_sql("UPDATE raw.orders SET status = 'delivered', updated_at = '2026-09-22 09:00:00' WHERE order_id = 103")
display(sql("""
    SELECT 'stg_orders (view)' AS model, status FROM analytics_staging.stg_orders WHERE order_id = 103
    UNION ALL
    SELECT 'fct_order_items (table)', status FROM analytics_marts.fct_order_items WHERE order_id = 103
"""))

# %%
U.check(sql("SELECT status FROM analytics_staging.stg_orders WHERE order_id = 103").status[0] == "delivered",
        "the view re-reads the source every time: it already sees the update")
U.check(sql("SELECT status FROM analytics_marts.fct_order_items WHERE order_id = 103").status[0] == "cancelled",
        "the table still holds the rows of its last build")

# %% [markdown]
# 🔍 A view is never stale but pays on every read; a table is fast but frozen at its last build. `fct_order_items+`
# rebuilds the fact and its children:

# %%
dbt("run", "--select", "fct_order_items+")
U.check(city("Jeddah") == 45.65, "after the rebuild, Jeddah includes order 103's 12 SAR")
V.animate_dbt(PROJ)

# %% [markdown]
# ### 7b · An incremental model processes only the rows that changed
#
# It loads everything on the first run, then only the rows its filter selects. Three choices are ours:
# - **filter:** orders updated at most 1 day before the newest one loaded (the **overlap** catches late changes);
# - **`unique_key`:** how to recognize a row already in the table;
# - **`delete+insert`:** delete the stored rows whose key comes back, then insert the new versions.

# %%
INCREMENTAL_SQL = """
{{ config(materialized='incremental', unique_key='order_line_id', incremental_strategy='delete+insert') }}
""" + FACT_SELECT + """
{% if is_incremental() %}
  AND o.updated_at >= (
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') - INTERVAL 1 DAY
    FROM {{ this }}
  )
{% endif %}
"""
write_file("models/marts/fct_order_items_incremental.sql", INCREMENTAL_SQL)
dbt("run", "--select", "+fct_order_items_incremental")
U.check(len(sql("FROM analytics_marts.fct_order_items_incremental")) == 4, "the first run loads all four lines")

# %% [markdown]
# The model, part by part:
#
# | Part | Means |
# |---|---|
# | `{{ config(...) }}` | settings for this model only; they override the folder's `+materialized: table` |
# | `materialized='incremental'` | keep the table between runs and add to it |
# | `unique_key='order_line_id'` | the column that identifies a row |
# | `incremental_strategy='delete+insert'` | how the new rows are written into the table (7c lists the others) |
# | `{% if is_incremental() %} ... {% endif %}` | the filter, left out on the first run and with `--full-refresh` |
# | `{{ this }}` | the model's own table, so the filter can ask what's already loaded |
#
# Now order 101 is refunded.
#
# 🤔 **Predict** · The newest `updated_at` loaded is 22 Sept 09:00. Which lines will the next run reprocess?
#
# We compile first and run that `SELECT` ourselves: it returns exactly what dbt will hand to `delete+insert`.

# %%
execute_sql("UPDATE raw.orders SET status = 'refunded', updated_at = '2026-09-25 12:00:00' WHERE order_id = 101")
before = sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id")
cutoff = sql("SELECT MAX(updated_at) - INTERVAL 1 DAY AS c FROM analytics_marts.fct_order_items_incremental").c[0]
dbt("compile", "--select", "fct_order_items_incremental")
candidates = sql(generated("compiled", "models/marts/fct_order_items_incremental.sql"))
dbt("run", "--select", "fct_order_items_incremental")
after = sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id")
display(after)

# %% [markdown]
# ▶ **Watch it run.** Left: the model's `SELECT`. Right: the incremental table.

# %%
V.animate_incremental(
    all_rows=sql(generated("compiled", "models/marts/fct_order_items.sql")), candidates=candidates, before=before,
    key="order_line_id", cutoff=cutoff, show=["order_line_id", "status", "updated_at", "net_amount"])

# %%
U.check(sorted(candidates.order_line_id) == ["101-1", "101-2", "103-1"], "3 lines in the window: 101's two, and 103's")
U.check(len(after) == 4 and after.order_line_id.is_unique, "still 4 lines, one per order_line_id: no duplicates")
U.check(set(after[after.order_id == 101].status) == {"refunded"}, "both of order 101's lines are refunded")
dbt("run", "--select", "fct_order_items_incremental")
U.check(sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id").equals(after),
        "running again over the same data changes nothing: reprocessing the overlap is safe")

# %% [markdown]
# 🔍 103-1 was reprocessed unchanged: that's the overlap, and it's harmless (replaced, not duplicated). 102-1 was
# outside the window and never read. All the SQL of that run:

# %%
DELETE_INSERT_RUN = generated("run", "models/marts/fct_order_items_incremental.sql")
V.sql_steps([
    ("We write · fct_order_items_incremental.sql", INCREMENTAL_SQL),
    ("dbt compiles (is_incremental() is true)", generated("compiled", "models/marts/fct_order_items_incremental.sql"),
     "the filter is in, and {{ this }} is the model's own table"),
    ("dbt runs · target/run/…", DELETE_INSERT_RUN,
     "the SELECT went into a temporary table first; then delete + insert"),
])

# %% [markdown]
# `unique_key` matches rows; it doesn't **check** that they're unique. Only a test does (§9).
#
# ### 7c · Other strategies, and one side by side ➕
#
# ➕ `incremental_strategy` decides how the batch the filter selected is written into the table:
#
# | Strategy | What one run does | Typical use |
# |---|---|---|
# | `append` | inserts the batch; no key, nothing is replaced | events that never change (clicks, logs) |
# | `delete+insert` | deletes the stored rows whose key comes back, then inserts the batch | this lab; DuckDB's default |
# | `merge` | one `MERGE INTO`: updates the rows whose key matches, inserts the new keys | default on Snowflake, BigQuery, Databricks |
# | `insert_overwrite` | replaces every partition (e.g. a day) the batch touches | partitioned tables on BigQuery, Spark |
# | `microbatch` | splits the work into time slices (e.g. one per day), each rerunnable on its own | large event tables |
#
# Which ones you can use depends on the adapter. Let's run `merge` once on the same model, then switch back.

# %%
MERGE_SQL = INCREMENTAL_SQL.replace("incremental_strategy='delete+insert'", "incremental_strategy='merge'")
write_file("models/marts/fct_order_items_incremental.sql", MERGE_SQL)
dbt("run", "--select", "fct_order_items_incremental")
U.check(sql("SELECT * FROM analytics_marts.fct_order_items_incremental ORDER BY order_line_id").equals(after),
        "same table: the strategy changes how the rows are written, not which rows end up there")
V.sql_steps([
    ("delete+insert · target/run/…", DELETE_INSERT_RUN, "two statements: DELETE the keys that came back, then INSERT"),
    ("merge · target/run/…", generated("run", "models/marts/fct_order_items_incremental.sql"),
     "one statement: UPDATE on a match, INSERT otherwise"),
])
write_file("models/marts/fct_order_items_incremental.sql", INCREMENTAL_SQL)   # back to delete+insert for the lab

# %% [markdown]
# 🔍 A practical plus of `merge`: it can update only some columns of a matched row
# (`merge_update_columns=['status', 'updated_at']` in the `config()`); `delete+insert` always replaces the whole row.
#
# Every strategy writes only what the filter selected. So what happens when a row changes and the filter doesn't
# select it?
#
# ### 7d · 🧪 Break it on purpose: a change the filter can't see ➕
#
# 🤔 **Predict** · Line 102-1 gets a 2 SAR discount. The line changes; its order's `updated_at` doesn't. After
# rebuilding both facts, which one has the discount?

# %%
execute_sql("UPDATE raw.order_items SET discount = 2.00 WHERE order_id = 102 AND line_no = 1")
dbt("run", "--select", "fct_order_items", "fct_order_items_incremental")
line_102 = sql("""
    SELECT 'fct_order_items (table)' AS model, net_amount FROM analytics_marts.fct_order_items WHERE order_line_id = '102-1'
    UNION ALL
    SELECT 'fct_order_items_incremental', net_amount FROM analytics_marts.fct_order_items_incremental WHERE order_line_id = '102-1'
""")
line_102

# %%
U.check(line_102.net_amount.tolist() == [31.65, 33.65], "the table has the discount, the incremental model missed it")

# %% [markdown]
# 🔍 No error, and the incremental model stays wrong: its filter only watches `orders.updated_at`. The filter defines
# what you can miss:
#
# | Change | Found? |
# |---|---|
# | an order updated inside the overlap | yes |
# | a line changed, its order untouched (just now) | no |
# | a late record, older than the overlap | no |
# | a deleted source row | no |
# | a fix to the model's logic | only for new rows |
#
# Fixes: watch the right timestamp, widen the overlap, or rebuild with `--full-refresh` (the `SELECT` over all rows).

# %%
dbt("run", "--select", "fct_order_items_incremental", "--full-refresh")
U.check(sql("SELECT net_amount FROM analytics_marts.fct_order_items_incremental WHERE order_line_id = '102-1'")
        .net_amount[0] == 31.65, "the full refresh picked up the discount")
dbt("run", "--select", "fct_order_items+")
U.check(city("Jeddah") == 43.65, "and the report shows it: 31.65 + 12.00 for Jeddah, nothing for refunded Riyadh")

# %% [markdown]
# ## 8 · Macros: write an expression once
#
# `quantity * unit_price - discount` is our revenue definition; copies of it drift apart. A **macro** is named Jinja
# that returns SQL text. dbt pastes it in before the SQL reaches the database.

# %%
write_file("macros/net_amount.sql", """
{% macro net_amount(quantity, unit_price, discount) -%}
    ({{ quantity }} * {{ unit_price }} - {{ discount }})
{%- endmacro %}
""")

# %% [markdown]
# `stg_order_items` now calls it, with the column names as strings.

# %%
write_file("models/staging/stg_order_items.sql", """
SELECT
    CAST(order_id AS VARCHAR) || '-' || CAST(line_no AS VARCHAR) AS order_line_id,
    order_id, line_no, quantity, unit_price, discount,
    {{ net_amount('quantity', 'unit_price', 'discount') }} AS net_amount
FROM {{ source('qafila_raw', 'order_items') }}
""")
dbt("run", "--select", "stg_order_items+")
V.sql_steps([
    ("We write · stg_order_items.sql", (PROJ / "models/staging/stg_order_items.sql").read_text()),
    ("dbt compiles", generated("compiled", "models/staging/stg_order_items.sql"), "the macro call became its SQL"),
])
U.check("(quantity * unit_price - discount)" in generated("compiled", "models/staging/stg_order_items.sql"),
        "the macro expanded into the expression")
U.check(city("Jeddah") == 43.65, "same revenue as before: the definition moved, it didn't change")

# %% [markdown]
# ## 9 · Tests: a build that succeeds is not a correct build
#
# A **test** is a `SELECT` of the rows that **break** a rule: 0 rows = pass. **Generic** tests (`unique`, `not_null`,
# ...) are declared in YAML; **singular** tests are SQL files in `tests/`.

# %%
write_file("models/marts/_models.yml", """
version: 2
models:
  - name: fct_order_items
    description: One row per order line with a positive quantity; keeps the order's status.   # shown in the docs
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]          # two tests on this column
      - name: status
        data_tests:
          - accepted_values:                    # a test with settings: a new or misspelled status fails
              arguments:
                values: [placed, delivered, cancelled,
                         refunded, failed_delivery]
  - name: fct_order_items_incremental
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]
  - name: mart_city_sales
    description: Delivered product revenue in SAR by store city, excluding delivery fees.
    columns:
      - name: city
        data_tests: [unique, not_null]
""")
write_file("tests/assert_fact_stores_exist.sql", """
-- every line's store must exist in stg_stores: return the lines whose store doesn't
SELECT f.order_line_id, f.store_id
FROM {{ ref('fct_order_items') }} f
LEFT JOIN {{ ref('stg_stores') }} s ON f.store_id = s.store_id
WHERE s.store_id IS NULL
""")
dbt("build")

# %% [markdown]
# `dbt build` = run + test along the graph: a model, its tests, then its children. What `[unique]` turns into:

# %%
UNIQUE_TEST = "models/marts/_models.yml/unique_fct_order_items_order_line_id.sql"
V.sql_steps([
    ("We write · _models.yml", "columns:\n  - name: order_line_id\n    data_tests: [unique, not_null]"),
    ("dbt generates · target/compiled/…/unique_fct_order_items_order_line_id.sql", generated("compiled", UNIQUE_TEST),
     "the keys that appear more than once"),
])
display(sql(generated("compiled", UNIQUE_TEST)))

# %% [markdown]
# 🤔 **Predict** · Line 102-1 is now delivered twice. Which test fails? Is the report rebuilt? Is the duplicate
# already in the fact?

# %%
execute_sql("INSERT INTO raw.order_items SELECT * FROM raw.order_items WHERE order_id = 102 AND line_no = 1")
dbt("build", "--select", "+mart_city_sales", expect_success=False)
statuses = {r["unique_id"].split(".")[2]: r["status"]
            for r in json.loads((PROJ / "target/run_results.json").read_text(encoding="utf-8"))["results"]}
statuses

# %% [markdown]
# ▶ **Watch it run.** Follow the red.

# %%
V.animate_dbt(PROJ)

# %%
display(sql(generated("compiled", UNIQUE_TEST)))
U.check(statuses["unique_fct_order_items_order_line_id"] == "fail", "the unique test found the duplicated line")
U.check(statuses["mart_city_sales"] == "skipped", "the report was skipped: dbt doesn't build on a failed test")
U.check(len(sql("FROM analytics_marts.fct_order_items")) == 5, "but the fact already holds the 5 rows, duplicate included")

# %% [markdown]
# 🔍 The report was **skipped** and keeps its last good rows. But the fact was built **before** its test, so the
# duplicate is already in the database. A test stops what comes after; it doesn't undo what came before.
#
# Fix the source, then rebuild:

# %%
execute_sql("CREATE OR REPLACE TABLE raw.order_items AS SELECT DISTINCT * FROM raw.order_items")
dbt("build")
U.check(len(sql("FROM analytics_marts.fct_order_items")) == 4, "the repaired fact has 4 lines again")

# %% [markdown]
# Tests check rules, not numbers. Last check: recompute the report straight from the raw tables.

# %%
expected = sql("""
    SELECT s.city, SUM(i.quantity * i.unit_price - i.discount) AS revenue_sar
    FROM raw.orders o
    JOIN raw.order_items i ON o.order_id = i.order_id
    JOIN raw.stores s ON o.store_id = s.store_id
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY s.city ORDER BY s.city
""")
actual = sql("SELECT * FROM analytics_marts.mart_city_sales ORDER BY city")
display(actual)
U.check(actual.equals(expected), "the report reconciles with a direct calculation from the raw tables")

# %% [markdown]
# ### 9a · Warn or error: decide what a failure means
#
# The source team adds a status: order 101 is now `refunded`. Someone wrote a test on `stg_orders` that only knows
# three statuses.
#
# 🤔 **Predict** · `dbt build` of the report. With the default severity (`error`), what happens to the fact and the
# report?

# %%
STATUS_TEST = """
version: 2
models:
  - name: stg_orders
    columns:
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: [placed, delivered, cancelled]
"""
write_file("models/staging/_staging.yml", STATUS_TEST)
dbt("build", "--select", "+mart_city_sales", expect_success=False)
statuses = {r["unique_id"].split(".")[2]: r["status"]
            for r in json.loads((PROJ / "target/run_results.json").read_text(encoding="utf-8"))["results"]}
U.check(statuses["fct_order_items"] == "skipped" and statuses["mart_city_sales"] == "skipped",
        "error: the failing test on stg_orders skipped the fact and the report")

# %% [markdown]
# 🔍 One unknown value stopped Finance's report. That's right for a duplicated key in a fact. Here the new status is a
# known source change that someone should look at, not a reason to stop. `severity: warn` records the failure and lets
# the build go on:

# %%
write_file("models/staging/_staging.yml", STATUS_TEST + """              config:
                severity: warn
""")
dbt("build", "--select", "+mart_city_sales")
statuses = {r["unique_id"].split(".")[2]: r["status"]
            for r in json.loads((PROJ / "target/run_results.json").read_text(encoding="utf-8"))["results"]}
U.check([s for name, s in statuses.items() if name.startswith("accepted_values_stg_orders")] == ["warn"],
        "warn: the failure is recorded...")
U.check(statuses["mart_city_sales"] == "success", "...and the report is built")
(PROJ / "models/staging/_staging.yml").unlink()

# %% [markdown]
# Three responses to a bad row:
#
# | Response | What happens | Qafila example |
# |---|---|---|
# | **error** | the test fails and `dbt build` skips everything downstream | a duplicated fact key |
# | **warn** | the failure is recorded, the build continues; someone must follow it up | a new source status, an erased customer |
# | **quarantine** | a model routes the rejected rows to their own table, with a reason; the rest is published | non-positive quantities (`qrn_order_items`, Lab 2.3) |
#
# Quarantine is not a test setting: it's a model whose `WHERE` is the opposite of the fact's. Rows in the fact plus
# rows in quarantine should add up to the source.
#
# ### 9b · A contract: the columns consumers rely on
#
# Tests check **values**. A dashboard also relies on the **shape** of a model: its column names and types. An enforced
# **contract** declares them, and dbt refuses to build a model whose `SELECT` doesn't match, before it replaces the
# table.

# %%
CONTRACT_YML = """
version: 2
models:
  - name: fct_order_items
    description: One row per order line with a positive quantity; keeps the order's status.
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: [placed, delivered, cancelled,
                         refunded, failed_delivery]
  - name: fct_order_items_incremental
    columns:
      - name: order_line_id
        data_tests: [unique, not_null]
  - name: mart_city_sales
    description: Delivered product revenue in SAR by store city, excluding delivery fees.
    config:
      contract:
        enforced: true
    columns:
      - name: city
        data_type: varchar
        data_tests: [unique, not_null]
      - name: revenue_sar
        data_type: decimal(38,2)
"""
write_file("models/marts/_models.yml", CONTRACT_YML)
dbt("build", "--select", "mart_city_sales")

# %% [markdown]
# A developer tidies the report and renames `revenue_sar` to `revenue`. The SQL is valid.
#
# 🤔 **Predict** · Does the build succeed? What does Finance's dashboard see afterwards?

# %%
REPORT = (PROJ / "models/marts/mart_city_sales.sql").read_text(encoding="utf-8")
write_file("models/marts/mart_city_sales.sql", REPORT.replace("AS revenue_sar", "AS revenue"))
broken = dbt("build", "--select", "mart_city_sales", log=False, expect_success=False)
print([line for line in str(broken.result[0].message).splitlines() if "revenue" in line][:4])
U.check(not broken.success, "the contract stopped the build")
U.check(list(sql("FROM analytics_marts.mart_city_sales").columns) == ["city", "revenue_sar"],
        "and the table the dashboard reads still has revenue_sar")
write_file("models/marts/mart_city_sales.sql", REPORT)
dbt("build", "--select", "mart_city_sales")

# %% [markdown]
# 🔍 Nothing reached the database. Changing a contract is a decision taken with the consumers (a new model version,
# Day 4), not a quiet edit. A contract checks names and types, not values: a `revenue_sar` of -5 still fits it, which
# is why the tests stay.
#
# ### 9c · A unit test: check the logic on rows you choose
#
# Data tests check today's data. A **unit test** gives a model a few input rows **you** write and compares its output
# with the rows you expect, so it can test a case that today's data doesn't contain, like a discount on a line of
# three.

# %%
write_file("models/staging/_unit_tests.yml", """
unit_tests:
  - name: net_amount_subtracts_the_discount
    model: stg_order_items
    given:
      - input: source('qafila_raw', 'order_items')
        rows:
          - {order_id: 1, line_no: 1, quantity: 2, unit_price: 10.00, discount: 0.00}
          - {order_id: 1, line_no: 2, quantity: 3, unit_price: 5.50, discount: 1.50}
    expect:
      rows:
        - {order_line_id: "1-1", net_amount: 20.00}
        - {order_line_id: "1-2", net_amount: 15.00}
""")
result = dbt("test", "--select", "test_type:unit")
U.check([r.status for r in result.result] == ["pass"], "the model computes 2 × 10 = 20 and 3 × 5.50 − 1.50 = 15")

# %% [markdown]
# 🧪 **Break it on purpose** · Someone "simplifies" the macro and drops the discount.
#
# 🤔 **Predict** · Which tests notice: the data tests (`unique`, `not_null`, `accepted_values`, the singular test) or
# the unit test?

# %%
MACRO = (PROJ / "macros/net_amount.sql").read_text(encoding="utf-8")
write_file("macros/net_amount.sql", MACRO.replace(" - {{ discount }}", ""))
data_tests = dbt("build", "--exclude", "test_type:unit", log=False)
unit = dbt("test", "--select", "test_type:unit", log=False, expect_success=False)
print("data tests:", sorted({str(r.status) for r in data_tests.result if r.node.resource_type == "test"}))
print("unit test: ", [str(r.status) for r in unit.result])
U.check(data_tests.success and not unit.success, "only the unit test fails: the data still looks fine")
U.check(city("Jeddah") == 45.65, "and the report is wrong by the 2 SAR discount (43.65 before)")
write_file("macros/net_amount.sql", MACRO)
dbt("build")
U.check(city("Jeddah") == 43.65, "macro restored: the report is right again")

# %% [markdown]
# 🔍 The data tests pass: no key is duplicated, no value is null, every store exists. The numbers are simply wrong by
# every discount. Only a test that knows the expected answer catches a wrong rule. A plain `dbt build` runs each unit
# test **before** its model, so with the unit test in place this rule would never have reached the table.
#
# ### 9d · Where each check belongs ➕
#
# ➕ Each check catches a different kind of problem:
#
# | Where | Check | Catches | In this course |
# |---|---|---|---|
# | a file or DataFrame at the door | Pandera schema | bad values before they are loaded | Lab 2.4 |
# | the logic of a model | dbt unit test | a wrong rule, on rows you chose | §9c, Lab 2.4 |
# | the tables just built | dbt data tests | broken keys, nulls, unknown values, orphans | §9, Labs 2.3–2.4 |
# | the model's interface | dbt contract | renamed or retyped columns | §9b, Lab 2.4 |
# | production, after release | freshness, volume, Unknown rate | changes nobody tested for | Day 4 |
#
# **Before a change is merged**, CI (continuous integration) builds it in its own schema and runs these checks, only
# on what the change touches:
#
# ```
# dbt build --select state:modified+ --defer --state prod-artifacts/
# ```
#
# `state:modified+` = the models whose files differ from production's `manifest.json` (in `prod-artifacts/`), and their
# children; `--defer` reads the unchanged parents from production instead of rebuilding them.

# %% [markdown]
# ## 10 · Lineage and documentation
#
# `dbt docs generate` writes the documentation from the same files. The graph in `target/manifest.json` is also what
# an orchestrator reads to run dbt model by model (Lab 4.1).

# %%
dbt("docs", "generate")
manifest = json.loads((PROJ / "target/manifest.json").read_text(encoding="utf-8"))
for name in ["fct_order_items", "mart_city_sales"]:
    print(name, "depends on:", manifest["nodes"][f"model.qafila_intro.{name}"]["depends_on"]["nodes"])
print("Documentation files:", [f for f in ["index.html", "manifest.json", "catalog.json"] if (PROJ / "target" / f).exists()])
print("Your complete project is in:", PROJ)

# %% [markdown]
# To browse the docs: `dbt docs serve --profiles-dir .` in a terminal, in the project folder (`Ctrl+C` stops it).
#
# ## 11 · Summary
#
# | | |
# |---|---|
# | **It does** | SQL `SELECT`s as a project in Git · the build order from `ref()` / `source()` · views, tables, incremental models · macros · data tests, unit tests, contracts · documentation and lineage (`manifest.json`) |
# | **It doesn't** | load data (dlt, Lab 1.3) · store or compute anything itself (the warehouse does) · schedule runs (Dagster, Lab 4.1) · design the model for you (Lab 2.2) |
# | **In this course** | Lab 2.3 builds the Qafila project (`dbt_qafila/`) · Lab 2.4 adds tests, a contract and a unit test · Lab 4.1 runs it from Dagster, one asset per model · Lab 4.2's contracts and freshness |
# | **As a service** | dbt Cloud (the same project with scheduling, an IDE and CI) · SQLMesh / Tobiko Cloud · warehouse-native tools: Dataform (BigQuery), Snowflake Dynamic Tables, Databricks Lakeflow Declarative Pipelines |
#
# **Cheat sheet**
#
# | Code | Does |
# |---|---|
# | `dbt debug` | checks the connection |
# | `dbt compile` | renders the Jinja into `target/compiled/`, builds nothing |
# | `dbt run` / `dbt test` | builds models / tests what's built |
# | `dbt build` | builds and tests, along the graph |
# | `dbt ls --select ...` | lists what a selection selects |
# | `--select x` · `+x` · `x+` · `staging` | a model · it and its parents · it and its children · a folder |
# | `dbt run --full-refresh` | rebuilds incremental models from all rows |
# | `dbt test --select test_type:unit` | only the unit tests |
# | `dbt docs generate`, `dbt docs serve` | writes / serves the documentation |
# | `dbt source freshness` | checks when each source last received data (Day 4) |
# | `dbt build --target prod` | the same project on the profile's `prod` output |
# | `{{ source('src', 'table') }}`, `{{ ref('model') }}` | a raw table, a model: the arrows of the graph |
# | `{{ config(materialized='incremental', unique_key=..., incremental_strategy=...) }}` | per-model settings; strategies: `append`, `delete+insert`, `merge`, `insert_overwrite`, `microbatch` |
# | `{% if is_incremental() %} ... {{ this }} ... {% endif %}` | the incremental filter, on the model's own table |
# | `{% macro name(args) %} ... {% endmacro %}` | SQL text written once, in `macros/` |
# | `data_tests: [unique, not_null]`, `accepted_values`, `config: {severity: warn}` | generic tests in YAML |
# | `contract: {enforced: true}` + `data_type` per column | the model's columns and types, checked before it is replaced |
# | `unit_tests:` → `given` / `expect` | a model's logic on rows you choose |
# | `{{ env_var('NAME') }}`, `{{ target.name }}` | a secret from the environment; `dev` or `prod` |
#
# ## Checkpoint
#
# 1. Why did `_sources.yml` load nothing, and why did `dbt compile` create no view? What does change the database?
# 2. What does dbt lose when a model names a table instead of using `ref()`?
# 3. View, table, incremental: which sees a source change at once, and which can miss one even after `dbt run`?
# 4. A `unique` test failed in `dbt build`. What was skipped, and was the bad data already in the database? When
#    would you set `severity: warn` instead?
# 5. A contract, a data test and a unit test: which one catches a renamed column, a duplicated key, a wrong formula?
#
# Next: [Lab 2.3 · The Qafila dbt project](../D2_3_dbt_project.ipynb). Docs:
# [materializations](https://docs.getdbt.com/docs/build/materializations) ·
# [incremental](https://docs.getdbt.com/docs/build/incremental-models) ·
# [macros](https://docs.getdbt.com/docs/build/jinja-macros) ·
# [tests](https://docs.getdbt.com/docs/build/data-tests) ·
# [contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) ·
# [unit tests](https://docs.getdbt.com/docs/build/unit-tests) ·
# [selection](https://docs.getdbt.com/reference/node-selection/syntax).
#
# **What you built:** the project's graph, from `target/manifest.json`, and the database dbt built, compared with the
# one before its first run.

# %%
V.dbt_graph(PROJ)

# %%
V.database_map(sql("SELECT table_schema, table_name, table_type FROM information_schema.tables"), before=tables_before)
