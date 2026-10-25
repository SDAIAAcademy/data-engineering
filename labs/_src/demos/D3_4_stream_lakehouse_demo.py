# %% [markdown]
# # Demo 3.4 · Streams into the lakehouse, live
#
# Day 3 · Session 4 · instructor walkthrough, about 12 minutes. **No exercises:** we run every cell together and look at
# what happens to the table. You practise in **Lab 3.4** right after.
#
# Session 3's courier events now land in an Iceberg table every 30 minutes, the way a Spark or Flink job would write them.
#
# 1. Micro-batches and offsets
# 2. The naive sink: commit the data, then save the offset
# 3. The fix: the offset rides in the same commit
# 4. Streaming makes small files
# 5. The table is a stream too
# 6. Kappa: fix it by replaying the log
# 7. The same pattern in Spark
#
# 🤔 marks a question to ask the room **before** running the cell.

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
        os.chdir("..")  # demos/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
from pyiceberg.transforms import DayTransform

def parse_ts(s: str) -> datetime:
    """The three event_time formats from Session 3 -> aware UTC datetime."""
    if s.isdigit():
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

events = [json.loads(line) for line in open(U.RAW / "events" / "delivery_events.jsonl", encoding="utf-8")]
for e in events:
    e["event_ts"], e["ingest_ts"] = parse_ts(e["event_time"]), parse_ts(e["ingest_time"])

START = datetime(2026, 9, 24, tzinfo=timezone.utc)
stream = [e for e in events if e["ingest_ts"] < START + timedelta(days=2)]   # two days of the stream

SCHEMA = pa.schema([("event_id", pa.string()), ("order_id", pa.int64()), ("store_id", pa.string()), ("city", pa.string()),
                    ("courier_id", pa.string()), ("event_type", pa.string()), ("event_ts", pa.timestamp("us", tz="UTC")),
                    ("ingest_ts", pa.timestamp("us", tz="UTC"))])

def to_arrow(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist([{f: r[f] for f in SCHEMA.names} for r in rows], schema=SCHEMA)

DEMO = U.lab_dir("D3_4_demo", reset=True)
catalog = U.get_catalog(DEMO / "lakehouse")
catalog.create_namespace("bronze")

def new_table(name):
    tbl = catalog.create_table(f"bronze.{name}", schema=SCHEMA)
    with tbl.update_spec() as spec:
        spec.add_field("event_ts", DayTransform(), "event_day")
    return tbl

print(f"{len(stream):,} events over two days")

# %% [markdown]
# ## 1 · Micro-batches and offsets
#
# Spark Structured Streaming works in **micro-batches**: every trigger interval, take whatever arrived, process it, commit
# it. The **offset** is the position in the log after the batch: "everything before here is done".

# %%
def micro_batches(rows, every=timedelta(minutes=30)):
    """Yield (end_offset, rows, batch_end) per processing-time interval; offsets index the arrival-ordered stream."""
    i, t = 0, START
    while i < len(rows):
        t += every
        j = i
        while j < len(rows) and rows[j]["ingest_ts"] < t:
            j += 1
        if j > i:
            yield j, rows[i:j], t
        i = j

batches = list(micro_batches(stream))
print(len(batches), "micro-batches of 30 minutes")
pd.DataFrame([{"batch_end (UTC)": f"{t:%m-%d %H:%M}", "events": len(rows), "end offset": end}
              for end, rows, t in batches[:6]])

# %% [markdown]
# ## 2 · The naive sink: commit the data, then save the offset
#
# A first attempt: append each batch to Iceberg, **then** write the offset to a checkpoint file. On restart, resume
# from the checkpoint. The job gets killed once, right after batch 30's data commit.
#
# 🤔 After the restart, how many rows does the table have compared with the stream?

# %%
class Crash(Exception):
    pass

CKPT = DEMO / "checkpoint.json"

def naive_job(tbl, crash_after=None):
    start = json.loads(CKPT.read_text())["offset"] if CKPT.exists() else 0
    n = 0
    for end, rows, _ in micro_batches(stream):
        if end <= start:
            continue
        tbl.append(to_arrow(rows))                         # 1. commit the data
        n += 1
        if n == crash_after:
            raise Crash(f"💥 killed after batch {n}: data committed, checkpoint not yet written")
        CKPT.write_text(json.dumps({"offset": end}))       # 2. then save the progress

naive = new_table("events_naive")
try:
    naive_job(naive, crash_after=30)
except Crash as err:
    print(err)
print("restart from checkpoint offset", json.loads(CKPT.read_text())["offset"])
naive_job(naive)

rows = naive.scan(selected_fields=("event_id",)).to_arrow().num_rows
print(f"rows in table {rows:,} | events in stream {len(stream):,} | extra {rows - len(stream):,} "
      f"| batch 30 had {len(batches[29][1]):,} events")

# %% [markdown]
# The extra rows are exactly batch 30, written twice. The data and the checkpoint are **two separate writes**; any crash
# between them replays a batch. Making the checkpoint write "faster" doesn't help: the gap is still there.
#
# ## 3 · The fix: the offset rides in the same commit
#
# Every Iceberg snapshot carries a free-form **summary**. Put the offset there, and one atomic commit saves both the data
# and the progress. There is no moment where one exists without the other.

# %%
safe = new_table("delivery_events")
end, rows_1, batch_end = batches[0]
safe.append(to_arrow(rows_1), snapshot_properties={"qafila.offset": str(end), "qafila.batch_end": batch_end.isoformat()})

snap = safe.current_snapshot()
{k: v for k, v in snap.summary.additional_properties.items() if k.startswith(("qafila", "added-records"))}

# %% [markdown]
# On restart, the job reads the offset back from the table's latest snapshot. That's the same idea as Spark's
# checkpoints tied to Iceberg/Delta commit ids, or Flink's two-phase commit. **Lab 3.4** writes this sink, crashes it,
# and proves zero duplicates (and also removes the phones' duplicate events).
#
# ## 4 · Streaming makes small files
#
# Back to the naive table: two days of 30-minute batches.
#
# 🤔 How many data files, and how big?

# %%
files = naive.inspect.files().to_pandas()
files["day"] = files.partition.map(lambda p: p["event_day"])
print(f"snapshots {len(naive.snapshots())} | data files {len(files)} | "
      f"average file {files.file_size_in_bytes.mean() / 1e3:.1f} kB | rows {files.record_count.sum():,}")
files.groupby("day").agg(files=("file_path", "size"), rows=("record_count", "sum"),
                         avg_kB=("file_size_in_bytes", lambda s: round(s.mean() / 1e3, 1)))

# %% [markdown]
# One file per batch, a few kB each, and that's only two days and one small topic. Each file costs an open, a footer read,
# a manifest entry and an object-store request; analytics engines like files of 100 MB or more. Shorter triggers mean
# fresher data **and** more, smaller files.
#
# The fix is **compaction**: rewrite each partition's small files into a few large ones, as one more commit. Lab 3.4 does
# it by hand and measures the scan before and after; in production it's Spark's `rewrite_data_files` or a managed service.
#
# ## 5 · The table is a stream too
#
# Each micro-batch is one snapshot, and Iceberg remembers what each one **added**.

# %%
snaps = naive.inspect.snapshots().to_pandas()
snaps.assign(added_records=snaps.summary.map(lambda s: int(dict(s)["added-records"])))[
    ["committed_at", "snapshot_id", "operation", "added_records"]].tail(5)

# %% [markdown]
# So a downstream job (bronze → silver) doesn't need to rescan the table. It remembers the last snapshot it processed,
# like a consumer offset, and asks for "rows appended since snapshot X". That's how Flink and Spark stream **from**
# Iceberg, and how jobs chain through the lakehouse instead of through more Kafka topics. Lab 3.4 does that read.
#
# ## 6 · Kappa: fix it by replaying the log
#
# The naive table has a replayed batch and the phones' duplicates in it. In a **Kappa** architecture you don't patch it
# with a separate batch job: you fix the code and **replay the retained log** from offset 0 into a new table.

# %%
seen, fixed = set(), []
for e in stream:                                   # replay from offset 0 with the fixed logic: one row per event_id
    if e["event_id"] not in seen:
        seen.add(e["event_id"])
        fixed.append(e)

rebuilt = new_table("delivery_events_v2")
rebuilt.append(to_arrow(fixed), snapshot_properties={"qafila.offset": str(len(stream))})
ids = rebuilt.scan(selected_fields=("event_id",)).to_arrow()["event_id"]
print(f"naive: {rows:,} rows | rebuilt: {len(ids):,} rows, {len(set(ids.to_pylist())):,} distinct event ids")

# %% [markdown]
# One code path, and the log is the source of truth. The catch: the log must be retained long enough to replay, and the
# sink must be replaceable. The common middle ground in 2026: stream into Iceberg (bronze), then run the **same dbt
# models** every 5-15 minutes on the new snapshots.
#
# ## 7 · The same pattern in Spark
#
# Shown for reading, not run here:
#
# ```python
# (events_stream                                            # readStream from Kafka, parsed and deduplicated
#     .writeStream.format("iceberg")
#     .trigger(processingTime="30 minutes")                 # micro-batches
#     .option("checkpointLocation", "s3://…/chk/events")    # offsets, tied to Iceberg commit ids: exactly-once
#     .toTable("lake.bronze.delivery_events"))
#
# spark.sql("CALL lake.system.rewrite_data_files('bronze.delivery_events')")    # compaction
# spark.sql("CALL lake.system.expire_snapshots('bronze.delivery_events')")      # keep storage bounded
# ```
#
# ## Recap → Lab 3.4
#
# | We saw | Because |
# |---|---|
# | one batch written twice | data commit and checkpoint were two separate writes |
# | the offset stored in the snapshot | one atomic commit = data + progress |
# | one tiny file per micro-batch | every commit writes new files; nothing merges them |
# | `added-records` per snapshot | the table can be read incrementally, like a log |
# | a clean table from offset 0 | Kappa: replay the retained log with fixed code |
#
# **Lab 3.4**: the crash-proof sink with dedupe, compaction with before/after timings, the live SLA query on the table
# with DuckDB, and incremental reads for the next job.
