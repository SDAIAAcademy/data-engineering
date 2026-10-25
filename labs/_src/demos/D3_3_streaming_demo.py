# %% [markdown]
# # Demo 3.3 · Streaming ideas, made visible
#
# Day 3 · Session 3 · instructor walkthrough, about 15 minutes. **No exercises:** we run every cell together and look at
# the courier events. You practise in **Lab 3.3** right after.
#
# Kafka and Flink are big systems, but the ideas underneath are small enough to print. Here we look at them on a
# handful of real Qafila courier events:
#
# 1. A topic is a set of partitioned, append-only logs
# 2. Offsets and consumer groups: reading doesn't delete
# 3. Commit before or after processing? Lost vs. duplicated
# 4. Two clocks: event time vs. processing time
# 5. Windows: tumbling, hopping, session
# 6. Watermarks: when is a window finished?
# 7. The same ideas in a real engine
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
import zlib
from datetime import timedelta

import pandas as pd

pd.set_option("display.width", 200)
events = [json.loads(line) for line in open(U.RAW / "events" / "delivery_events.jsonl", encoding="utf-8")]
print(f"{len(events):,} courier events, in the order the platform received them")
pd.DataFrame(events[:6])[["event_id", "order_id", "city", "courier_id", "event_type", "event_time", "ingest_time"]]

# %% [markdown]
# ## 1 · A topic is a set of partitioned, append-only logs
#
# A **partition** is a list you only append to; a record's position is its **offset**. The producer picks the
# partition by hashing the record's **key**. That's all a Kafka topic is, minus replication and disks:

# %%
class Log:
    """A tiny Kafka topic: N partitions, each an append-only list; a record's offset is its index."""

    def __init__(self, partitions: int):
        self.parts = [[] for _ in range(partitions)]

    def produce(self, key, value) -> tuple[int, int]:
        p = zlib.crc32(str(key).encode()) % len(self.parts)
        self.parts[p].append(value)
        return p, len(self.parts[p]) - 1          # (partition, offset)

# %% [markdown]
# 🤔 We produce the first 18 events, keyed by `order_id`, into 3 partitions. Will one order's events stay together? In
# order?

# %%
topic = Log(3)
for e in events[:18]:
    topic.produce(e["order_id"], e)

def show(log):
    cols = {f"P{p}": [f"…{e['order_id'] % 1000:03d} {e['event_type']}" for e in part] for p, part in enumerate(log.parts)}
    return pd.DataFrame({k: pd.Series(v) for k, v in cols.items()}).fillna("").rename_axis("offset")

show(topic)

# %% [markdown]
# Every order lives in exactly one partition, and its `assigned → picked_up → out_for_delivery → delivered` stays in
# order there. Across partitions there's **no** order: P0's offset 3 and P1's offset 3 have nothing to do with each
# other. Choose the key for the ordering you need. Partitions are also the unit of **parallelism**: 3 partitions, at
# most 3 consumers in one group.
#
# ## 2 · Offsets and consumer groups: reading doesn't delete
#
# Two teams read the same topic. Each **consumer group** keeps its own committed offset per partition.

# %%
committed = {"sla-dashboard": [0, 0, 0], "fraud": [0, 0, 0]}

def consume(group, max_per_partition):
    for p, part in enumerate(topic.parts):
        start = committed[group][p]
        got = part[start:start + max_per_partition]
        committed[group][p] = start + len(got)       # commit after reading

consume("sla-dashboard", 2)
consume("fraud", 10)
print("records in each partition:", [len(p) for p in topic.parts])
pd.DataFrame(committed, index=["P0", "P1", "P2"]).T.rename_axis("group: next offset to read")

# %% [markdown]
# The groups are independent, and the log is untouched: Kafka **retains** records for days (or forever with tiered
# storage). A new group can start at offset 0 and **replay** history: that's how you fix a bug in a stream job or
# backfill a new metric.
#
# ## 3 · Commit before or after processing? Lost vs. duplicated
#
# The job counts **delivered** events. It reads batches of 100 events and crashes once, halfway through batch 5.
#
# 🤔 One consumer commits its offset **before** processing each batch, the other **after**. Which one loses data,
# and which one double-counts?

# %%
def run(commit_first: bool, crash_in_batch=5, size=100):
    committed, position, counted, n = 0, 0, 0, 0
    while position < len(events):
        batch = events[position:position + size]
        position += len(batch)
        n += 1
        if commit_first:
            committed = position
        crashed = n == crash_in_batch
        for e in batch[:size // 2] if crashed else batch:   # 💥 the crash stops processing halfway
            counted += e["event_type"] == "delivered"
        if crashed:
            position = committed                            # restart from the last committed offset
            continue
        if not commit_first:
            committed = position
    return counted

raw_truth = sum(e["event_type"] == "delivered" for e in events)
pd.DataFrame({"delivered events counted": [raw_truth, run(commit_first=True), run(commit_first=False)]},
             index=["no crash", "commit → process (at-most-once)", "process → commit (at-least-once)"]).assign(
    difference=lambda d: d.iloc[:, 0] - raw_truth)

# %% [markdown]
# - **Commit first**: after the restart, the consumer resumes after the batch it never finished. Those events are **lost**.
# - **Commit after**: it re-reads the batch and counts the first half **again**.
#
# Neither is "exactly-once". The production answer is at-least-once **plus** an idempotent or transactional sink, so a
# replay changes nothing. And there's a second source of duplicates the consumer can't prevent: the phones themselves.

# %%
dupes = len(events) - len({e["event_id"] for e in events})
print(f"{dupes} events arrived twice from the producer side (phones retrying), before any crash")

# %% [markdown]
# Lab 3.3 makes the consumer idempotent and gets the exact counts despite both problems.
#
# ## 4 · Two clocks: event time vs. processing time
#
# Every event carries `event_time` (on the courier's phone, when it happened) and `ingest_time` (when the platform got
# it). And two app versions write `event_time` in **three** formats:

# %%
fmt = lambda s: "epoch millis" if s.isdigit() else "+03:00 offset" if "+" in s else "Z (UTC)"
pd.DataFrame(events).assign(format=lambda d: d.event_time.map(fmt)).groupby("format").agg(
    events=("event_id", "size"), example=("event_time", "first"))

# %% [markdown]
# DuckDB reads the JSON lines and parses both clocks into one type, so we can ask how late events arrive.
#
# 🤔 Most events arrive within seconds. How late is the **latest** one?

# %%
import duckdb

df = duckdb.sql(f"""
    SELECT event_id, order_id, city, courier_id, event_type,
           CASE WHEN regexp_full_match(event_time, '[0-9]+') THEN make_timestamptz(event_time::BIGINT * 1000)
                ELSE event_time::TIMESTAMPTZ END AS ts,
           ingest_time::TIMESTAMPTZ AS arrived
    FROM read_json('{(U.RAW / "events" / "delivery_events.jsonl").as_posix()}', columns = {{
        event_id: 'VARCHAR', order_id: 'BIGINT', city: 'VARCHAR', courier_id: 'VARCHAR',
        event_type: 'VARCHAR', event_time: 'VARCHAR', ingest_time: 'VARCHAR'}})
""").df()
df["ts"], df["arrived"] = df.ts.dt.tz_convert("Asia/Riyadh"), df.arrived.dt.tz_convert("Asia/Riyadh")   # Riyadh time
df["late_s"] = (df.arrived - df.ts).dt.total_seconds()
df.late_s.describe(percentiles=[0.5, 0.9, 0.99]).round(1).to_frame("seconds late")

# %%
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(9, 2.6))
ax.hist(df.late_s.clip(lower=1), bins=[2 ** i for i in range(0, 13)], color="#00876d")
ax.set_xscale("log", base=2)
ax.set_yscale("log")
ax.set_xticks([1, 4, 16, 60, 300, 900, 3600], ["1 s", "4 s", "16 s", "1 min", "5 min", "15 min", "1 h"])
ax.set_xlabel("arrival − event time")
ax.set_ylabel("events (log)")
ax.set_title("A long tail: phones offline in basements and car parks")
plt.show()

# %% [markdown]
# "Deliveries between 21:00 and 21:15" means **event time**. But a delivery at 21:12 may only arrive at 21:55. So every
# windowed metric has to decide how long to wait.
#
# ## 5 · Windows: tumbling, hopping, session
#
# Take one delivered event and see which windows it belongs to.

# %%
e = df[df.event_type == "delivered"].iloc[100]
ts = e.ts
tumbling = ts.floor("15min")
hopping = [ts.floor("5min") - timedelta(minutes=m) for m in (10, 5, 0)]
print(f"event at {ts:%H:%M:%S}")
print(f"tumbling 15 min           → [{tumbling:%H:%M}, {tumbling + timedelta(minutes=15):%H:%M})   exactly one window")
print("hopping 15 min, every 5   →", ", ".join(f"[{h:%H:%M}, {h + timedelta(minutes=15):%H:%M})" for h in hopping),
      f"  {len(hopping)} windows")

# %% [markdown]
# **Session** windows have no fixed size: they close after a gap of inactivity. One courier's day, split wherever
# they had no event for 30 minutes:

# %%
courier = df[df.courier_id == e.courier_id].sort_values("ts")
day = courier[courier.ts.dt.date == ts.date()]
session = (day.ts.diff() > timedelta(minutes=30)).cumsum()
day.groupby(session).agg(start=("ts", "min"), end=("ts", "max"), events=("event_id", "size"),
                         orders=("order_id", "nunique")).assign(
    start=lambda d: d.start.dt.strftime("%H:%M"), end=lambda d: d.end.dt.strftime("%H:%M")).rename_axis(f"{e.courier_id} session")

# %% [markdown]
# ## 6 · Watermarks: when is a window finished?
#
# A stream engine can't wait forever. It tracks a **watermark**: W = (max event time seen so far) − (allowed lateness).
# "I believe I've seen everything before W." When W passes a window's end, the window is emitted and its state dropped.
# An event for an already-closed window is **late** and gets dropped.
#
# Here's the rule on a few Riyadh deliveries, in arrival order, with **5 minutes** of allowed lateness.
#
# 🤔 Which of these events will be dropped?

# %%
riyadh = df[(df.city == "Riyadh") & (df.event_type == "delivered")].reset_index(drop=True)
late_i = riyadh.index[riyadh.late_s.between(22 * 60, 40 * 60)][3]       # a delivery that arrived 22-40 min late
trace = riyadh.iloc[late_i - 5: late_i + 3].copy()

allowed = timedelta(minutes=5)
trace["window"] = trace.ts.dt.floor("15min")
trace["max_event_time_seen"] = riyadh.ts.cummax().iloc[late_i - 5: late_i + 3]
trace["watermark"] = trace.max_event_time_seen - allowed
trace["verdict"] = ["LATE → dropped" if w + timedelta(minutes=15) <= wm else "on time"
                    for w, wm in zip(trace.window, trace.watermark)]
fmt_t = lambda s: s.dt.strftime("%H:%M:%S")
trace.assign(**{c: fmt_t(trace[c]) for c in ["arrived", "ts", "max_event_time_seen", "watermark"]},
             window=fmt_t(trace.window).str[:5])[
    ["order_id", "arrived", "ts", "window", "max_event_time_seen", "watermark", "verdict"]]

# %% [markdown]
# The late event's 15-minute window had already closed: the watermark (driven by **other** couriers' fresh events)
# passed its end. With more allowed lateness it would have been counted, but every window would also be emitted later.
#
# The same rule over a busy hour, drawn. Each dot is a delivery: x = when it arrived, y = when it happened. The step line
# is the watermark; red ✕ are dropped. Left: 2 minutes of patience. Right: 30 minutes.

# %%
import de_viz as V

stream = [{"ts": r.ts.to_pydatetime(), "arrived": r.arrived.to_pydatetime(), "event_type": r.event_type, "city": r.city}
          for r in df.itertuples()]
V.watermark(stream, [timedelta(minutes=2), timedelta(minutes=30)])

# %% [markdown]
# **Latency vs. completeness** is a business decision, not a technical one. Ops wants the SLA board within 2 minutes and
# can live with a slightly low number; finance wants exact totals and can wait (or recompute in batch tonight).
#
# ## 7 · The same ideas in a real engine
#
# Everything above has a name in Spark Structured Streaming (and in Flink SQL, Kafka Streams, RisingWave...). Shown for
# reading, not run here:
#
# ```python
# (spark.readStream.format("kafka")                                    # the log
#       .option("subscribe", "delivery-events")                        # a topic
#       .option("startingOffsets", "earliest").load()                  # offsets: replay from 0
#       .select(F.from_json("value", schema).alias("e")).select("e.*")
#       .withWatermark("event_ts", "10 minutes")                       # allowed lateness
#       .dropDuplicatesWithinWatermark(["event_id"])                   # producer duplicates
#       .filter("event_type = 'delivered'")
#       .groupBy(F.window("event_ts", "15 minutes"), "city").count()   # tumbling window, by event time
#       .writeStream.option("checkpointLocation", "s3://…/chk")        # committed offsets + state
#       .toTable("lake.silver.deliveries_15m"))
# ```
#
# ## Recap → Lab 3.3
#
# | We saw | Because |
# |---|---|
# | one order stayed in one partition, in order | key → partition; order is per partition only |
# | two groups at different offsets, log unchanged | consumers track offsets; reading doesn't delete |
# | commit-first lost events, commit-after double-counted | the crash falls between processing and committing |
# | some events arrive ~50 min late | event time ≠ processing time |
# | the late delivery was dropped | the watermark had passed its window's end |
#
# **Lab 3.3** builds the log and consumer on the full week, makes counting idempotent, writes the timestamp parser, and
# measures the watermark trade-off from 0 to 60 minutes of allowed lateness.
