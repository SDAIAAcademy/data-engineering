# %% [markdown]
# # Lab 4.3 · Streaming: a log, a crash and a watermark
#
# Day 4 · Session 3 · 25 min. Kafka and Flink are big systems, but the ideas underneath are small. You'll build them in
# plain Python on a week of Qafila **courier events**:
# - a log with offsets, and what a crash does to a count;
# - counting that survives crashes and repeats;
# - windows closed by a watermark, with the trade-off measured.
#
# **Tasks**
# - [ ] 1 · Write the events into a log with offsets (4')
# - [ ] 2 · Consume, crash and count too many (5')
# - [ ] 3 · Make the count idempotent (6')
# - [ ] 4 · Event time vs. processing time (4')
# - [ ] 5 · Windows and a watermark: measure the trade-off (6')
# - [ ] ✱ Stretch · Replay the log from offset 0 with a new reader

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
import json
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd

import de_viz as V

events = [json.loads(line) for line in open(U.RAW / "events" / "delivery_events.jsonl", encoding="utf-8")]
print(f"{len(events):,} events, in the order they arrived")
pd.DataFrame(events[:4])[["event_id", "order_id", "event_type", "event_time", "ingest_time"]]

# %% [markdown]
# ## 1 · Write the events into a log with offsets (4')
#
# A **log** is an ordered list you only add to. A record's position is its **offset**. Kafka calls a log a **topic**, and
# splits it into **partitions** so several readers can share the work. The record's **key** picks the partition: same key,
# same partition, so an order's events stay in order. That's the whole idea, in a few lines. Run it.

# %%
class Log:
    """A tiny Kafka topic: N partitions, each an append-only list; a record's offset is its index."""

    def __init__(self, partitions: int = 4):
        self.parts = [[] for _ in range(partitions)]

    def produce(self, key, value) -> tuple[int, int]:
        p = zlib.crc32(str(key).encode()) % len(self.parts)
        self.parts[p].append((key, value))
        return p, len(self.parts[p]) - 1          # (partition, offset)

    def read(self, partition: int, offset: int, max_records: int) -> list:
        return self.parts[partition][offset: offset + max_records]

    def end_offsets(self) -> list[int]:
        return [len(p) for p in self.parts]


log = Log(4)
for e in events:
    log.produce(e["order_id"], e)
print("records per partition:", log.end_offsets())

khalid = [(p, off, v["event_type"]) for p, part in enumerate(log.parts) for off, (k, v) in enumerate(part) if k == 5043883]
print("Khalid's order 5043883 (partition, offset, step):")
for row in khalid:
    print("  ", row)

# %% [markdown]
# 🔍 Khalid's four events sit in one partition, in order, even though other orders are mixed in between. Would that be true
# if we spread events evenly with no key?
#
# ## 2 · Consume, crash and count too many (5')
#
# A **consumer** reads from its saved offsets, does its work, then saves the new offsets (a **commit**). Reading deletes
# nothing, and each reader keeps its own place. The job: count **delivered** events per city.
#
# This consumer saves *after* the work. It crashes once between the two. Run the cells.

# %%
class Consumer:
    """Reads every partition from its saved offsets; offsets live in `store[group]` until committed."""

    def __init__(self, log: Log, group: str, store: dict):
        self.log, self.group, self.store = log, group, store
        self.position = list(store.get(group, [0] * len(log.parts)))   # resume from committed offsets

    def poll(self, max_records: int = 200) -> list:
        batch = []
        for p in range(len(self.log.parts)):
            recs = self.log.read(p, self.position[p], max_records // len(self.log.parts))
            batch += [v for _, v in recs]
            self.position[p] += len(recs)
        return batch

    def commit(self) -> None:
        self.store[self.group] = list(self.position)


truth = Counter()
for e in {e["event_id"]: e for e in events}.values():           # each event once: the right answer
    if e["event_type"] == "delivered":
        truth[e["city"]] += 1
print("right answer:", dict(truth), "| total:", sum(truth.values()))

# %%
def run_naive(crash_at_batch=10):
    store, counts, batches = {}, Counter(), 0
    c = Consumer(log, "naive", store)
    while batch := c.poll():
        for e in batch:
            if e["event_type"] == "delivered":
                counts[e["city"]] += 1
        batches += 1
        if batches == crash_at_batch:                       # 💥 crash before the commit
            c = Consumer(log, "naive", store)               # restart from the saved offsets
            crash_at_batch = None
            continue
        c.commit()
    return counts

naive = run_naive()
repeats = len(events) - len({e["event_id"] for e in events})
print(f"naive count: {sum(naive.values()):,} | too many by: {sum(naive.values()) - sum(truth.values())}")
print(f"repeated events in the log, sent twice by phones: {repeats}")

# %% [markdown]
# 🔍 Two causes, one symptom. The batch was re-read after the crash (**at-least-once**: nothing is lost, some things repeat),
# and the phones also retried when the network dropped. If the consumer saved *before* the work, what would a crash do instead?
#
# ## 3 · Make the count idempotent (6')
#
# You can't stop repeats arriving. You can make them harmless. **Idempotent** means doing it twice has the same effect as doing
# it once. Every event carries an `event_id`, so remember which ones you've counted.
#
# ✏️ **Exercise 1** · Fill in `count_once(e, counts, seen)`: if `e["event_id"]` is already in `seen`, do nothing. Otherwise add
# it to `seen`, and if the event is `delivered`, add 1 to `counts[e["city"]]`.
#
# *Hint:* `if e["event_id"] in seen: return` is the first line. Four lines in all.

# %%
def count_once(e, counts, seen):
    # BEGIN SOLUTION
    if e["event_id"] in seen:
        return
    seen.add(e["event_id"])
    if e["event_type"] == "delivered":
        counts[e["city"]] += 1
    # END SOLUTION

def run_idempotent(crash_at_batch=10, group="idempotent"):
    store, counts, seen, batches = {}, Counter(), set(), 0
    c = Consumer(log, group, store)
    while batch := c.poll():
        for e in batch:
            count_once(e, counts, seen)
        batches += 1
        if batches == crash_at_batch:                       # 💥 same crash as before
            c = Consumer(log, group, store)
            crash_at_batch = None
            continue
        c.commit()
    return counts

# %%
try:
    idem = run_idempotent()
except NotImplementedError:
    print(f"⏭️ Exercise 1 not done yet. Expected: {sum(truth.values()):,} delivered, equal to the right answer. Using the reference.")
    def count_once(e, counts, seen):
        if e["event_id"] not in seen:
            seen.add(e["event_id"])
            if e["event_type"] == "delivered":
                counts[e["city"]] += 1
    idem = run_idempotent()
print(f"idempotent count: {sum(idem.values()):,}")
U.check(idem == truth, "a crash and the phones' repeats, and still exactly the right counts")

# %% [markdown]
# 🔍 In a real job the "seen" memory lives in the output: a `MERGE` on `event_id` (Lab 2.3 did this for customers) does the same
# job as this set. When someone says "exactly-once", they usually mean this: at-least-once delivery plus an idempotent output.
#
# ## 4 · Event time vs. processing time (4')
#
# Every event has two clocks. **Event time** is when it happened, on the courier's phone. **Processing time** is when the platform
# received it (`ingest_time` here). And two app versions write the event time in three formats. The parser is done for you.
#
# ✏️ **Exercise 2** · Write `lateness_seconds(e)`: `e["arrived"]` minus `e["ts"]`, in seconds.
#
# *Hint:* subtracting two datetimes gives a timedelta, and a timedelta has `.total_seconds()`. One line.

# %%
def parse_event_time(s: str) -> datetime:
    if s.isdigit():                                                      # epoch milliseconds
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)   # ...+03:00 or ...Z

for e in events:
    e["ts"], e["arrived"] = parse_event_time(e["event_time"]), parse_event_time(e["ingest_time"])

print(Counter("epoch millis" if e["event_time"].isdigit() else "+03:00" if "+" in e["event_time"] else "Z (UTC)" for e in events))

def lateness_seconds(e):
    # BEGIN SOLUTION
    return (e["arrived"] - e["ts"]).total_seconds()
    # END SOLUTION

# %%
try:
    lateness = pd.Series([lateness_seconds(e) for e in events])
except NotImplementedError:
    print("⏭️ Exercise 2 not done yet. Expected: half under 5 s, the slowest near 50 minutes. Using the reference.")
    lateness = pd.Series([(e["arrived"] - e["ts"]).total_seconds() for e in events])
print(lateness.describe(percentiles=[0.5, 0.9, 0.99, 0.999]).round(1).to_string())
U.check(lateness.min() >= 0 and lateness.max() > 30 * 60, "nothing arrives before it happens; some events are over 30 minutes late")

# %% [markdown]
# 🔍 Most events arrive within seconds, and a long tail arrives very late (a phone offline in a car park). "Deliveries between
# 13:00 and 13:15" means **event time**, or a replay tomorrow would give a different answer. But then: when is 13:00–13:15 finished?
#
# ## 5 · Windows and a watermark: measure the trade-off (6')
#
# A **window** is a slice of event time that a count is grouped by. Here: delivered orders per city per 15 minutes.
# A **watermark** is the engine's guess at "I've seen everything before W":
#
# W = newest event time seen − allowed lateness
#
# When W passes a window's end, the window is published. An event for an already-published window is **late** and gets dropped.
#
# ✏️ **Exercise 3** · The loop is written. Fill in the two rules: `wm` is the watermark, and `is_late` is true when the event's
# window ends at or before `wm`.
#
# *Hint:* `wm = max_ts - allowed_lateness`, and the window ends at `key[1] + WINDOW`. Two lines.

# %%
WINDOW = timedelta(minutes=15)

def window_start(ts: datetime) -> datetime:
    return ts - timedelta(minutes=ts.minute % 15, seconds=ts.second, microseconds=ts.microsecond)

def watermark_rules(max_ts, key, allowed_lateness):
    # BEGIN SOLUTION
    wm = max_ts - allowed_lateness
    is_late = key[1] + WINDOW <= wm
    # END SOLUTION
    return wm, is_late

def stream_windows(stream, allowed_lateness: timedelta):
    state, emitted, delays, dropped = {}, {}, [], 0
    max_ts = datetime.min.replace(tzinfo=timezone.utc)
    for e in stream:
        if e["event_type"] != "delivered":
            continue
        max_ts = max(max_ts, e["ts"])
        key = (e["city"], window_start(e["ts"]))
        wm, is_late = watermark_rules(max_ts, key, allowed_lateness)
        if is_late:
            dropped += 1
            continue
        state.setdefault(key, set()).add(e["order_id"])     # a set: repeats are harmless
        for k in [k for k in state if k[1] + WINDOW <= wm]:  # publish every finished window
            emitted[k] = len(state.pop(k))
            delays.append((e["arrived"] - (k[1] + WINDOW)).total_seconds())
    for k, v in state.items():                              # end of the data: publish what's left
        emitted[k] = len(v)
    return emitted, dropped, delays

# %%
try:
    stream_windows(events[:50], timedelta(minutes=2))
except NotImplementedError:
    print("⏭️ Exercise 3 not done yet. Expected: more lateness, fewer drops, later results. Using the reference.")
    def watermark_rules(max_ts, key, allowed_lateness):
        wm = max_ts - allowed_lateness
        return wm, key[1] + WINDOW <= wm

exact = defaultdict(set)                                    # the right answer, counted with all the data in hand
for e in events:
    if e["event_type"] == "delivered":
        exact[(e["city"], window_start(e["ts"]))].add(e["order_id"])
exact = {k: len(v) for k, v in exact.items()}

rows = []
for minutes in [0, 2, 10, 30, 60]:
    out, dropped, delays = stream_windows(events, timedelta(minutes=minutes))
    right = sum(out.get(k) == v for k, v in exact.items()) / len(exact)
    rows.append({"allowed_lateness_min": minutes, "late_events_dropped": dropped, "windows_exactly_right": f"{right:.1%}",
                 "median_wait_for_result_min": round(pd.Series(delays).median() / 60, 1)})
tradeoff = pd.DataFrame(rows)
U.check(tradeoff.late_events_dropped.is_monotonic_decreasing, "waiting longer drops fewer late events")
U.check(tradeoff.median_wait_for_result_min.is_monotonic_increasing, "...but results arrive later")
tradeoff

# %% [markdown]
# Here it is drawn: one busy hour, when each delivery arrived (x) against when it happened (y). The navy step line is the
# watermark. An event that arrives after its window closed is a red ✕. Left: 2 minutes of patience. Right: 30 minutes.

# %%
V.watermark(events, [timedelta(minutes=2), timedelta(minutes=30)])

# %% [markdown]
# 🔍 No setting gives both. Operations might take 2 minutes and a slightly low count. Finance wants exact totals and can recompute
# in batch tonight. **That choice is a business decision**, so ask, don't guess.
#
# ## ✱ Stretch · Replay the log from offset 0 with a new reader
#
# The log is kept, not deleted on read. A new consumer group starts at offset 0 and rebuilds the answer: that's how you fix a bug
# in a stream job.

# %%
def stretch():
    # BEGIN SOLUTION
    replayed = run_idempotent(crash_at_batch=None, group="replay-after-bug-fix")
    U.check(replayed == truth, "replaying the log reproduces the result")
    # END SOLUTION

try:
    stretch()
except NotImplementedError:
    print("✱ Optional: write `stretch()` if you have time.")
print("the log itself is untouched:", log.end_offsets())

# %% [markdown]
# ## Checkpoint
#
# 1. Your consumer saves its offset **before** doing the work, and crashes in between. What happens?
#
# <details><summary>Answer</summary>The restart skips the unfinished events: they are lost (at-most-once). Saving after the work repeats events instead, and repeats are the kind you can fix.</details>
#
# 2. Which clock for "deliveries per 15 minutes", and why?
#
# <details><summary>Answer</summary>Event time. Processing time shifts with every network hiccup, backlog and replay, so the same delivery could land in a different window each run.</details>
#
# 3. You raise the allowed lateness from 2 to 30 minutes. What gets better, and what gets worse?
#
# <details><summary>Answer</summary>Fewer late events are dropped and counts are more complete, but every window is published about 28 minutes later.</details>
#
# ## Carry forward
#
# Option 3 of the capstone replays these same events with event-time KPIs and SLA alerts: `stream_windows` is a good start.
# Want more? `demos/streaming.ipynb`.
