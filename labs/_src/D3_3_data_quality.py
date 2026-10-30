# %% [markdown]
# # Lab 3.3 · Tests and quarantine
#
# Day 3 · Session 3 · 25 min. A number nobody can check is a number nobody should trust. What you'll do:
# - run the project's **dbt tests** and read pass, warn and error;
# - add a **relationships** test that catches real orphans, and choose whether it warns or stops the build;
# - check the supplier files with **Pandera** at the door, and set the bad rows aside in **quarantine**.
#
# **Tasks**
# - [ ] 1 · Run the tests and read the warnings (5')
# - [ ] 2 · Add a relationships test that finds orphans (6')
# - [ ] 3 · Choose warn or error (5')
# - [ ] 4 · Validate the supplier files with Pandera (6')
# - [ ] 5 · Quarantine the bad rows (3')
# - [ ] ✱ Stretch · Store the failing rows in the warehouse

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
import warnings

import duckdb
import pandas as pd
import polars as pl

warnings.filterwarnings("ignore", category=FutureWarning)          # pyspark's pandas-3 notice, via pandera
import pandera.polars as pa

OUT = U.lab_dir("D3_3", reset=True)
PROJ, DB = U.dbt_lab_copy(OUT)                   # a private copy of the project (the Lab 3.2 star) and of the raw tables


def dbt(*args):
    """Run dbt; return a table of results, one row per model or test. A failed test is a result here, not a crash."""
    from dbt.cli.main import dbtRunner

    os.environ["QAFILA_DUCKDB"] = DB.resolve().as_posix()
    res = dbtRunner().invoke(["--quiet", *args, "--project-dir", str(PROJ), "--profiles-dir", str(PROJ)])
    if res.result is None or not hasattr(res.result, "results"):
        raise RuntimeError(f"dbt {' '.join(args)} could not run: {res.exception}")
    return pd.DataFrame([{"node": r.node.name, "type": r.node.resource_type, "status": str(r.status), "failures": r.failures}
                         for r in res.result.results])


def sql(query):
    with duckdb.connect(str(DB)) as c:
        return c.sql(query).df()

# %% [markdown]
# ## 1 · Run the tests and read the warnings (5')
#
# A **dbt test** is a query that looks for bad rows: no rows back means pass. `dbt build` runs each model, then its tests.
# The tests are declared in YAML next to the model. Look at the ones on `stg_orders`:

# %%
staging_yml = PROJ / "models/staging/_staging.yml"
text = staging_yml.read_text(encoding="utf-8")
print(text[text.index("  - name: stg_orders"):text.index("  - name: stg_order_items")])

# %% [markdown]
# Four kinds come with dbt: `unique`, `not_null`, `accepted_values`, `relationships` (every value exists in another table).
# Now build everything. This takes a moment.

# %%
build = dbt("build")
tests = build[build.type == "test"]
print(f"{len(build[build.type == 'model'])} models, {len(tests)} tests:", tests.status.value_counts().to_dict())
tests[tests.status != "pass"]

# %% [markdown]
# 🔍 Three warnings, each a real problem in the data:
# - `positive_stg_order_items_quantity`: 142 lines with a non-positive quantity. The same lines you left out of the fact in Lab 3.1;
#   the project also keeps them, with a reason, in `quarantine.qrn_order_items`.
# - `positive_stg_supplier_prices_unit_cost_sar`: 4 supplier prices at zero or below.
# - `assert_order_totals_match_payments`: orders where the provider's payment differs from the order total. Finance's problem,
#   not a reason to stop the dashboard.
#
# ## 2 · Add a relationships test that finds orphans (6')
#
# An **orphan** is a row whose reference points at nothing: an order whose customer doesn't exist. Nothing tests
# `stg_orders.customer_id` yet.
#
# ✏️ **Exercise** · Write the YAML for a `customer_id` column with a `relationships` test to `ref('stg_customers')`, field
# `customer_id`. Copy the shape of `store_id` printed above; the indentation has to line up exactly.

# %%
def customer_test_yaml():
    # BEGIN SOLUTION
    return """      - name: customer_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
"""
    # END SOLUTION

# %%
REFERENCE_TEST = """      - name: customer_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
"""
try:
    NEW_TEST = customer_test_yaml()
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    NEW_TEST = REFERENCE_TEST
if not NEW_TEST.endswith("\n"):
    NEW_TEST += "\n"
anchor = "      - name: store_id\n"                     # the new column goes just before store_id


def write_test(severity=None):
    """Insert your test into the YAML, with a severity if given."""
    block = NEW_TEST
    if severity:
        block += f"              config:\n                severity: {severity}\n"
    staging_yml.write_text(text.replace(anchor, block + anchor), encoding="utf-8")


write_test()
res = dbt("test", "--select", "stg_orders")
orphans = res[res.node.str.startswith("relationships_stg_orders_customer_id")]
print("expected: status fail, 1,040 failing rows")
U.check(len(orphans) == 1 and orphans.failures.iloc[0] == 1040, "the test finds the 1,040 orders of erased customers")
orphans

# %% [markdown]
# 🔍 1,040 orders point at customers who no longer exist. Their customers asked to be erased, so these orphans are expected.
# In the fact they go to the unknown member, `-1`.
#
# ## 3 · Choose warn or error (5')
#
# Every test has a **severity**. `error` is the default: a failure stops everything downstream. `warn` records it and goes
# on. See what each does to the models that read `stg_orders`. Set your choice in the second run.

# %%
write_test("error")
as_error = dbt("build", "--select", "stg_orders+")
print("severity error:", as_error.status.value_counts().to_dict())
as_error[as_error.type == "model"]

# %%
MY_SEVERITY = "warn"                                  # ✏️ your choice: "warn" or "error"
write_test(MY_SEVERITY)
as_chosen = dbt("build", "--select", "stg_orders+")
print(f"severity {MY_SEVERITY}:", as_chosen.status.value_counts().to_dict())
as_chosen[as_chosen.type == "model"]

# %%
U.check((as_error[as_error.type == "model"].status == "skipped").any(), "with error, the downstream models were skipped")
if MY_SEVERITY == "warn":
    U.check((as_chosen[as_chosen.type == "model"].status == "success").all(), "with warn, every model was built")

# %% [markdown]
# 🔍 Ask two questions. **What does a wrong number cost compared with a late one?** And **is this failure expected?**
# Orphans here are expected and handled, so `warn`. But `fct_order_items.customer_id → dim_customers` is already an `error`
# in the project: there, an orphan means the model has a bug. Every warning needs an owner, or it turns into wallpaper.
#
# ## 4 · Validate the supplier files with Pandera (6')
#
# dbt tests look at data already in the warehouse. A supplier's file should be checked **before** it gets in, at the door.
# **Pandera** does that: rules for a DataFrame, written as a Python class.
#
# Four weekly CSV files arrive, and the format drifts: a renamed column, decimal commas, Arabic digits. The parser is given;
# read it, then see the rows. **Polars** is the DataFrame library from Day 2.

# %%
def read_supplier_file(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema=False, encoding="utf8-lossy")             # everything as text first
    cost = "cost_sar" if "cost_sar" in df.columns else "unit_cost"
    digits = str.maketrans("٠١٢٣٤٥٦٧٨٩٫", "0123456789.")
    return df.select(
        pl.col("supplier_id"), pl.col("sku").str.strip_chars(), pl.col("product_name_ar"),
        pl.col(cost).map_elements(lambda s: s.translate(digits).replace(",", "."), return_dtype=pl.String)
          .cast(pl.Float64, strict=False).alias("unit_cost_sar"),
        (pl.col("currency") if "currency" in df.columns else pl.lit("SAR")).alias("currency"),
        pl.col("valid_from").str.to_date(), pl.col("min_order_qty").cast(pl.Int64),
        pl.lit(path.name).alias("file"),
    )


prices = pl.concat([read_supplier_file(p) for p in sorted((U.RAW / "suppliers").glob("prices_*.csv"))])
known_skus = sql("SELECT trim(sku) AS sku FROM raw.products").sku.tolist()
print(prices.shape)
prices.head(3)

# %% [markdown]
# ✏️ **Exercise** · Complete the schema with two rules: `sku` must be one of `known_skus`, and `unit_cost_sar` must be greater
# than 0.
#
# *Hint:* `pa.Field(isin=known_skus)` and `pa.Field(gt=0, nullable=False)`.

# %%
def supplier_schema():
    class SupplierPrice(pa.DataFrameModel):
        supplier_id: str = pa.Field(str_startswith="SUP-")
        currency: str = pa.Field(isin=["SAR"])
        min_order_qty: int = pa.Field(isin=[6, 12, 24, 48])
        # BEGIN SOLUTION
        sku: str = pa.Field(isin=known_skus)
        unit_cost_sar: float = pa.Field(gt=0, nullable=False)
        # END SOLUTION
    return SupplierPrice

# %%
class ReferenceSchema(pa.DataFrameModel):
    supplier_id: str = pa.Field(str_startswith="SUP-")
    currency: str = pa.Field(isin=["SAR"])
    min_order_qty: int = pa.Field(isin=[6, 12, 24, 48])
    sku: str = pa.Field(isin=known_skus)
    unit_cost_sar: float = pa.Field(gt=0, nullable=False)


try:
    schema = supplier_schema()
except NotImplementedError:
    print("⚠️ Not done yet: using the reference answer so the rest of the lab runs.")
    schema = ReferenceSchema

failures = None
try:
    schema.validate(prices, lazy=True)                # lazy=True: report every failure, not just the first
except pa.errors.SchemaErrors as e:
    failures = e.failure_cases
if failures is None or failures.height < 7:           # a rule is missing: the next step needs the full list
    print("⚠️ Your schema finds too few problems: using the reference schema.")
    try:
        ReferenceSchema.validate(prices, lazy=True)
    except pa.errors.SchemaErrors as e:
        failures = e.failure_cases
print("expected: 7 failures, 4 on unit_cost_sar and 3 on sku")
U.check(failures.height == 7, "seven failing values found")
failures.select("column", "check", "failure_case", "index")

# %% [markdown]
# ## 5 · Quarantine the bad rows (3')
#
# Rejecting the whole file for 7 bad rows leaves Purchasing with nothing. Loading it all lets bad prices in. **Quarantine** is
# the middle way: good rows go on, bad rows go aside with the reason, and someone gets the list.

# %%
reasons = failures.group_by("index").agg(pl.col("check").str.join("; ").alias("reason"))
numbered = prices.with_row_index("index").with_columns(pl.col("index").cast(pl.Int32))
quarantine = numbered.join(reasons, on="index", how="inner")
clean = numbered.join(reasons, on="index", how="anti").drop("index")
quarantine.write_csv(OUT / "supplier_prices_quarantine.csv")
clean.write_parquet(OUT / "supplier_prices_clean.parquet")
print(f"{clean.height} clean rows go on, {quarantine.height} go to quarantine")
quarantine.select("file", "sku", "unit_cost_sar", "reason")

# %%
U.check(quarantine.height == 7, "4 bad costs + 3 unknown SKUs quarantined")
U.check(clean.filter(pl.col("file") == "prices_2026-W38.csv").height == 160, "week 38's commas and Arabic digits were parsed, not rejected")

# %% [markdown]
# 🔍 Who reads `supplier_prices_quarantine.csv`? If nobody owns it, quarantine is just a slow way to lose data. Name the
# person for your capstone.
#
# ## ✱ Stretch · Store the failing rows in the warehouse
#
# `dbt test --store-failures` saves each failing test's rows as a table, so you can query or report them. Run it for the
# payments test and look at the biggest differences.

# %%
def stretch():
    # BEGIN SOLUTION
    dbt("test", "--select", "assert_order_totals_match_payments", "--store-failures")
    audit = sql("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema LIKE '%audit%'")
    display(audit)
    display(sql(f"SELECT * FROM {audit.table_schema[0]}.assert_order_totals_match_payments ORDER BY abs(difference) DESC LIMIT 5"))
    # END SOLUTION


try:
    stretch()
except NotImplementedError:
    print("✱ Optional: write stretch() above if you have time.")

# %% [markdown]
# ## Checkpoint
#
# 1. A `relationships` test on `stg_orders.customer_id` fails with 1,040 rows. Warn or error, and why?
# 2. With severity `error`, what happens to the models downstream, and what do they keep?
# 3. Why check supplier files with Pandera when dbt can test the same rule?
#
# <details><summary>Answers</summary>
#
# 1. Warn: the orphans come from erased customers, they're expected, and the fact maps them to `-1`. Someone still owns the warning.
# 2. They're skipped, and keep what they held from their last build: stale but not wrong.
# 3. dbt tests data already in the warehouse. Pandera stops a bad row before it's loaded, and gives the supplier a list of exactly which rows to fix.
#
# </details>
#
# ## Carry forward
#
# Lab 3.4 runs the project's tests on a schedule, and adds checks of its own that can block the rows behind them. It starts
# from a fresh copy.
#
# Want more? `tools/pandera.ipynb`.
