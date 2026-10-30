# %% [markdown]
# # Setup check
#
# Day 1 · Session 1: Lab 1.1 starts here (2–5 minutes, run it in a second tab). It:
#
# - installs packages on Colab and generates the Qafila course data,
# - checks your Python packages and tool versions,
# - builds the shared DuckDB warehouse that later labs start from,
# - runs a short smoke test of DuckDB, Iceberg, dbt, and Java for Spark.
#
# Locally: `cd labs && uv sync && uv run jupyter lab`. On Colab: just run the cells (a CPU runtime is enough).

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

# %% [markdown]
# ## 1 · Packages

# %%
import platform
from importlib.metadata import PackageNotFoundError, version

packages = ["duckdb", "polars", "pyarrow", "pandas", "dlt", "dbt-core", "dbt-duckdb", "pyiceberg",
            "pandera", "dagster", "dagster-dbt", "pyspark"]
rows = []
for name in packages:
    try:
        rows.append((name, version(name)))
    except PackageNotFoundError:
        rows.append((name, "optional: Lab 4.2 only" if name == "pyspark" else "MISSING"))

import pandas as pd

print("Python", platform.python_version(), "|", platform.system(), "| CPUs:", os.cpu_count())
pd.DataFrame(rows, columns=["package", "version"])

# %% [markdown]
# ## 2 · Data and the shared warehouse
#
# `prepare_data.py` generated Qafila's raw sources (deterministic and synthetic: no real personal data). `build_warehouse()` loads them into the `raw` schema of `warehouse/qafila.duckdb`, the starting point of many labs.

# %%
import duckdb

con = duckdb.connect()
U.attach_app(con)
sources = {t: con.sql(f"SELECT count(*) FROM app.{t}").fetchall()[0][0] for t in ["customers", "stores", "products", "orders", "order_items"]}
for name, path in [("payments (API)", "api/payments.jsonl"), ("customer CDC", "cdc/customers_changes.jsonl"),
                   ("delivery events", "events/delivery_events.jsonl")]:
    sources[name] = con.sql(f"SELECT count(*) FROM read_json('{(U.RAW / path).as_posix()}', format='newline_delimited')").fetchall()[0][0]
sources["supplier files"] = len(list((U.RAW / "suppliers").glob("*.csv")))
sources["reviews"] = con.sql(f"SELECT count(*) FROM read_csv('{(U.RAW / 'reviews.csv').as_posix()}')").fetchall()[0][0]
con.close()

db = U.build_warehouse()
with U.connect(db, read_only=True) as wh:
    raw_tables = wh.sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw'").fetchall()[0][0]
U.check(raw_tables >= 9, f"warehouse ready: {raw_tables} raw tables in {db.name}")
pd.DataFrame(sources.items(), columns=["source", "rows / files"])

# %% [markdown]
# ## 3 · Smoke test: Iceberg, DuckDB and dbt

# %%
import pyarrow as pa

lab = U.lab_dir("00_setup", reset=True)
catalog = U.get_catalog(lab / "iceberg")
catalog.create_namespace_if_not_exists("smoke")
table = catalog.create_table("smoke.hello", schema=pa.schema([("id", pa.int64()), ("city", pa.string())]))
table.append(pa.table({"id": [1, 2, 3], "city": ["Riyadh", "Jeddah", "Dammam"]}))

con = duckdb.connect()
U.duckdb_iceberg(con)
n = con.sql(f"SELECT count(*) FROM iceberg_scan('{table.metadata_location}')").fetchall()[0][0]
con.close()
U.check(n == 3, "PyIceberg wrote a table and DuckDB read it back")

res = U.run_dbt(["parse"], db_path=lab / "dbt_smoke.duckdb", quiet=True)
U.check(res.success, "dbt parsed the Qafila project")

# %% [markdown]
# ## 4 · Java for Spark (Lab 4.2)
#
# Spark 4 needs **Java 17+**. On Colab, Lab 4.2 installs it for you. Locally, install a JDK (for example Temurin 21), set `JAVA_HOME`, and run `uv sync --extra spark`. If you don't have Java, run Lab 4.2 on Colab.

# %%
java = U._java_major()
if java >= 17:
    print(f"Java {java}: Lab 4.2 can run locally (after `uv sync --extra spark`).")
elif IN_COLAB:
    print("Colab: Lab 4.2 installs Java 17 when it starts Spark.")
else:
    print(f"Java {java or 'not found'}: run Lab 4.2 on Colab, or install Java 17+.")

# %% [markdown]
# ## You're ready
#
# If every package shows a version (PySpark is optional) and every ✅ printed, go back to Lab 1.1.
