"""Figures for the labs' visual summaries.

Each lab ends with a few pictures of what it just did. The drawing code lives here so the notebooks stay about data
engineering: every function takes data the lab already computed (query results, file metadata, the event stream) and
shows a figure. Nothing here changes the data.
"""

from __future__ import annotations

import functools
from datetime import timedelta
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle

# Categorical order (teal, coral, purple, amber), checked for colour-vision deficiency. Teal and coral are close for
# protanopes, so every figure also labels its marks with text: colour is never the only cue.
SERIES = ["#00876d", "#d9533d", "#6a5fc1", "#b07d12"]
INK, MUTED, FADE, GRID = "#101820", "#5b6472", "#b9b5aa", "#e4e1d8"
NAVY = "#1c355e"  # "the engine reads / keeps this"
BAD = "#b42318"   # dropped, wrong, deleted
MONO = "DejaVu Sans Mono"

RC = {
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "font.size": 9.5, "axes.titlesize": 10.5, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.titlepad": 10, "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "text.color": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": False, "grid.color": GRID, "grid.linewidth": 0.8, "legend.frameon": False,
}


def _figure(fn):
    """Draw with the course style without changing the notebook's matplotlib settings."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with plt.rc_context(RC):
            fn(*args, **kwargs)
            plt.show()
    return wrapper


def _colors(values) -> dict:
    """Stable colour per entity: sorted distinct values take the categorical slots in order."""
    keys = sorted({v for v in values if not pd.isna(v)}, key=str)
    return {k: SERIES[i % len(SERIES)] for i, k in enumerate(keys)}


def _fmt(v) -> str:
    if v is None or (not isinstance(v, (list, tuple, str)) and pd.isna(v)):
        return "NULL"
    if isinstance(v, pd.Timestamp) or hasattr(v, "strftime"):
        ts = pd.Timestamp(v)
        return ts.strftime("%m-%d") if (ts.hour, ts.minute, ts.second) == (0, 0, 0) else ts.strftime("%m-%d %H:%M")
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else f"{v:.2f}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_fmt(x) for x in v) + "]"
    return str(v)


# ---------------------------------------------------------------------------
# The Qafila platform map · "you are here" at the top of every walkthrough
# ---------------------------------------------------------------------------

STAGES = ["sources", "ingest", "store", "transform", "test", "serve"]
SOURCES = [("App database", "qafila_oltp.sqlite"), ("Payments API", "pages, cursor, 429s"),
           ("CDC feed", "inserts, updates, deletes"), ("Click events", "a stream"),
           ("Supplier files", "weekly CSVs"), ("Reviews", "free text")]
# (key, name, first stage, last stage, what it does here, sessions). Order sets the packing: wide tools first.
TOOLS = [
    ("dlt", "dlt", "ingest", "ingest", "incremental loads, schema drift", "D1_3"),
    ("duckdb", "DuckDB", "store", "serve", "the SQL engine in every lab", "D1_1"),
    ("cdc", "CDC + MERGE", "ingest", "ingest", "changes, deletes, SCD2", "D1_4"),
    ("parquet", "Parquet", "store", "store", "columnar files", "D1_2"),
    ("polars", "Polars", "transform", "test", "DataFrames, lazy plans, checks", "D2_1"),
    ("semantic", "Semantic layer", "serve", "serve", "metrics defined once", "D4_4"),
    ("streaming", "Streaming", "ingest", "ingest", "events as they happen", "D3_3 · D3_4"),
    ("iceberg", "Iceberg", "store", "store", "tables on files, time travel", "D3_1"),
    ("dbt", "dbt", "transform", "test", "models, tests, docs, lineage", "D2_2 · D2_3"),
    ("ai", "Embeddings", "serve", "serve", "search, text-to-SQL", "D4_4"),
    ("spark", "Spark", "transform", "transform", "the same work on a cluster", "D3_2"),
    ("pandera", "Pandera", "test", "test", "schemas for DataFrames", "D2_4"),
    ("contracts", "Contracts", "test", "test", "ODCS: what consumers rely on", "D4_2"),
]
BANDS = [("dagster", "Orchestrate", "Dagster: assets, schedules, checks", "D4_1"),
         ("observe", "Observe", "freshness, volume, schema, distribution, lineage", "D4_2"),
         ("govern", "Govern", "PDPL, masking, access", "D4_3")]

_MAP_CSS = """
#%%ID%% {font-family: system-ui, sans-serif; color: %%INK%%; background: #fff; border: 1px solid %%GRID%%;
     border-radius: 8px; padding: 12px 14px; max-width: 100%; overflow-x: auto; color-scheme: light}
#%%ID%% .pm-here {font-size: 13.5px; margin: 0 0 10px}
#%%ID%% .pm-g {display: grid; grid-template-columns: repeat(6, minmax(118px, 1fr)); gap: 5px; min-width: 760px;
           position: relative}
#%%ID%% .pm-col {border-radius: 6px; background: %%TINT%%; z-index: 0}
#%%ID%% .pm-h {z-index: 1; font-weight: bold; font-size: 12.5px; text-transform: uppercase; letter-spacing: .04em;
           color: %%MUTED%%; padding: 6px 18px 6px 10px; background: #f4f2ec;
           clip-path: polygon(0 0, calc(100% - 10px) 0, 100% 50%, calc(100% - 10px) 100%, 0 100%, 10px 50%)}
#%%ID%% .pm-h:first-child {clip-path: polygon(0 0, calc(100% - 10px) 0, 100% 50%, calc(100% - 10px) 100%, 0 100%)}
#%%ID%% .pm-h.on {background: %%NAVY%%; color: #fff}
#%%ID%% .pm-t, #%%ID%% .pm-s, #%%ID%% .pm-b {z-index: 1; border: 1px solid %%FADE%%; border-radius: 6px;
           background: #fff; padding: 4px 8px; font-size: 12px; line-height: 1.3}
#%%ID%% .pm-t b, #%%ID%% .pm-b b {font-size: 12.5px}
#%%ID%% .pm-t small, #%%ID%% .pm-s small, #%%ID%% .pm-b small {display: block; color: %%MUTED%%; font-size: 11px}
#%%ID%% .pm-t i, #%%ID%% .pm-b i {float: right; font-style: normal; font-size: 10.5px; color: %%MUTED%%;
           font-variant-numeric: tabular-nums; margin-left: 6px}
#%%ID%% .pm-s {border-style: dashed}
#%%ID%% .pm-b {background: #f9f8f4}
#%%ID%% .pm-b b {display: inline-block; min-width: 92px}
#%%ID%% .pm-b small {display: inline}
#%%ID%% .on.pm-t, #%%ID%% .on.pm-b {background: %%NAVY%%; border-color: %%NAVY%%; color: #fff}
#%%ID%% .on.pm-t small, #%%ID%% .on.pm-t i, #%%ID%% .on.pm-b small, #%%ID%% .on.pm-b i {color: #dfe5ef}
#%%ID%% .on.pm-t b::before, #%%ID%% .on.pm-b b::before {content: "▸ "}
#%%ID%% .dim {opacity: .4}
"""


def platform_map(tool=None, stage=None, title=None):
    """The Qafila platform: sources → ingest → store → transform → test → serve, and the tools in each stage.

    `tool` (a key from `TOOLS` / `BANDS`, or a list of them) is drawn filled, and the stages it covers are marked
    "you are here"; `stage` marks stages directly. Tools outside the marked stages fade, so the map still shows the
    whole platform but points at one part of it.
    """
    import html
    import uuid

    from IPython.display import HTML

    tools = [tool] if isinstance(tool, str) else list(tool or [])
    known = {t[0] for t in TOOLS} | {b[0] for b in BANDS}
    if unknown := set(tools) - known:
        raise ValueError(f"unknown tool {sorted(unknown)}; pick from {sorted(known)}")
    stages = [stage] if isinstance(stage, str) else list(stage or [])
    if unknown := set(stages) - set(STAGES):
        raise ValueError(f"unknown stage {sorted(unknown)}; pick from {STAGES}")
    for key, _, a, b, *_ in TOOLS:
        if key in tools and not stage:
            stages += STAGES[STAGES.index(a):STAGES.index(b) + 1]
    marked = set(stages)
    focus = bool(tools or marked)
    esc = html.escape

    rows, cells = [], []   # rows: the set of grid columns already used in each tool row
    for key, name, a, b, what, sessions in TOOLS:
        c0, c1 = STAGES.index(a) + 1, STAGES.index(b) + 1
        r = next((i for i, used in enumerate(rows) if not used & set(range(c0, c1 + 1))), None)
        if r is None:
            rows.append(set())
            r = len(rows) - 1
        rows[r] |= set(range(c0, c1 + 1))
        covers = set(STAGES[c0 - 1:c1])
        cls = "on" if key in tools else ("dim" if focus and not covers & marked else "")
        cells.append(f'<div class="pm-t {cls}" style="grid-column:{c0}/{c1 + 1};grid-row:{r + 2}">'
                     f'<i>{esc(sessions)}</i><b>{esc(name)}</b><small>{esc(what)}</small></div>')
    n = max(len(rows), len(SOURCES))
    src_dim = " dim" if focus and not marked & {"sources", "ingest"} else ""
    for i, (name, what) in enumerate(SOURCES):
        cells.append(f'<div class="pm-s{src_dim}" style="grid-column:1;grid-row:{i + 2}">'
                     f'<b>{esc(name)}</b><small>{esc(what)}</small></div>')
    for i, (key, name, what, sessions) in enumerate(BANDS):
        cls = "on" if key in tools else ("dim" if focus else "")
        cells.append(f'<div class="pm-b {cls}" style="grid-column:2/-1;grid-row:{n + 2 + i}">'
                     f'<i>{esc(sessions)}</i><b>{esc(name)}</b> <small>{esc(what)}</small></div>')
    heads = "".join(f'<div class="pm-h{" on" if s in marked else ""}" style="grid-column:{i + 1};grid-row:1">'
                    f'{s}</div>' for i, s in enumerate(STAGES))
    cols = "".join(f'<div class="pm-col" style="grid-column:{i + 1};grid-row:1/{n + 2}"></div>'
                   for i, s in enumerate(STAGES) if s in marked)

    names = {t[0]: t[1] for t in TOOLS} | {b[0]: b[1] for b in BANDS}
    if title is None and focus:
        what = " + ".join(names[t] for t in tools) or "this session"
        where = " → ".join(s for s in STAGES if s in marked) or ("across the platform" if tools else "")
        title = f"<b>You are here:</b> {esc(what)} · {where}"
    uid = "pm-" + uuid.uuid4().hex[:10]
    style = _MAP_CSS
    for k, v in {"ID": uid, "INK": INK, "MUTED": MUTED, "GRID": GRID, "FADE": FADE, "NAVY": NAVY,
                 "TINT": _tint(NAVY, 0.07)}.items():
        style = style.replace(f"%%{k}%%", v)
    return HTML(f'<div id="{uid}"><style>{style}</style>'
                + (f'<div class="pm-here">{title}</div>' if title else "")
                + f'<div class="pm-g">{cols}{heads}{"".join(cells)}</div></div>')


# ---------------------------------------------------------------------------
# Tables as pictures (Lab 0)
# ---------------------------------------------------------------------------

CHAR_IN = 0.078   # width of one monospace character at the cell font size, in inches
ROW_IN = 0.27


def _table_size(df, gap_by=None):
    """(width in characters, height in rows) of a drawn table."""
    w = sum(max(len(str(c)), *(len(_fmt(v)) for v in df[c]), 3) + 2 for c in df.columns)
    groups = int(df[gap_by].ne(df[gap_by].shift()).sum()) - 1 if gap_by else 0
    return w, len(df) + 0.45 * groups


def _draw_table(ax, df, color_by, new_cols=(), faded=None, gap_by=None, title="", span=None):
    cols = list(df.columns)
    cells = [[_fmt(v) for v in df[c]] for c in cols]
    widths = [max(len(str(c)), *(len(t) for t in col), 3) + 2 for c, col in zip(cols, cells)]
    total = sum(widths)
    colors = _colors(df[color_by]) if color_by else {}
    faded = list(faded) if faded is not None else [False] * len(df)

    y, ys = 0.0, []
    prev = None
    for i in range(len(df)):
        if gap_by is not None and i and df[gap_by].iloc[i] != prev:
            y += 0.45
        prev = df[gap_by].iloc[i] if gap_by is not None else None
        ys.append(y)
        y += 1
    height = y

    x = 0
    for c, w in zip(cols, widths):
        ax.text(x + 1, -0.75, c, ha="left", va="center", family=MONO, fontsize=8.5,
                color=NAVY if c in new_cols else MUTED, weight="bold")
        x += w
    ax.plot([0, total], [-0.2, -0.2], color=GRID, lw=1)
    for i, yi in enumerate(ys):
        col = colors.get(df[color_by].iloc[i], MUTED) if color_by else MUTED
        if not faded[i]:
            ax.add_patch(Rectangle((0, yi + 0.06), total, 0.88, color=col, alpha=0.10, lw=0))
        ax.add_patch(Rectangle((-0.9, yi + 0.12), 0.5, 0.76, color=FADE if faded[i] else col, lw=0))
        x = 0
        for j, (c, w) in enumerate(zip(cols, widths)):
            ax.text(x + 1, yi + 0.5, cells[j][i], ha="left", va="center", family=MONO, fontsize=8.5,
                    color=FADE if faded[i] else INK, weight="bold" if c in new_cols and not faded[i] else "normal")
            x += w
        if faded[i]:
            ax.plot([0.4, total - 0.4], [yi + 0.5, yi + 0.5], color=FADE, lw=1)
    for c in new_cols:   # outline the columns this step computed
        if c in cols:
            j = cols.index(c)
            ax.add_patch(Rectangle((sum(widths[:j]) + 0.3, -1.2), widths[j] - 0.6, height + 1.3, fill=False,
                                   ec=NAVY, lw=1.2))
    span_w, span_h = span or (total, height)
    ax.set_xlim(-1.2, span_w - 1.2)          # one x unit = one character, one y unit = one row, in every panel
    ax.set_ylim(span_h + 0.1, -1.4)
    ax.axis("off")
    ax.text(-1.2, -1.75, title, ha="left", va="bottom", fontsize=10, weight="bold", color=INK)


@_figure
def table_steps(steps, color_by=None, suptitle=None):
    """Draw tables side by side, left to right, like the frames of an animation.

    `steps` is a list of dicts with keys `title`, `df` and optionally `new` (columns to outline: what this step
    computed), `faded` (a boolean per row: rows this step filters out) and `gap_by` (leave a gap between groups).
    """
    sizes = [_table_size(s["df"], s.get("gap_by")) for s in steps]
    widths = [max(w + 1.6, len(s["title"]) * 1.12 + 1) for (w, _), s in zip(sizes, steps)]   # in characters
    rows = max(h for _, h in sizes) + 1.5
    gap_in, margin_in, top_in = 0.5, 0.15, 0.45 + (0.35 if suptitle else 0)
    panel_in = [w * CHAR_IN for w in widths]
    fig_w = sum(panel_in) + gap_in * (len(steps) - 1) + 2 * margin_in
    fig_h = rows * ROW_IN + top_in + 0.1
    fig = plt.figure(figsize=(fig_w, fig_h))
    x_in, axes = margin_in, []
    for s, w_in, w in zip(steps, panel_in, widths):
        ax = fig.add_axes([x_in / fig_w, 0.1 / fig_h, w_in / fig_w, rows * ROW_IN / fig_h])
        _draw_table(ax, s["df"].reset_index(drop=True), color_by, s.get("new", ()), s.get("faded"), s.get("gap_by"),
                    s["title"], span=(w, rows - 1.5))
        axes.append((ax, x_in, w_in))
        x_in += w_in + gap_in
    for (_, x0, w0), (_, x1, _) in zip(axes, axes[1:]):   # arrows between the frames
        fig.add_artist(FancyArrowPatch(((x0 + w0 + 0.08) / fig_w, 0.5), ((x1 - 0.08) / fig_w, 0.5),
                                       transform=fig.transFigure, arrowstyle="-|>", mutation_scale=12, color=MUTED))
    if suptitle:
        fig.text(margin_in / fig_w, 1 - 0.1 / fig_h, suptitle, ha="left", va="top", fontsize=11.5, weight="bold")


@_figure
def frames(df, label_col, frame_cols, current, titles=None):
    """Which rows each window frame contains, for one current row.

    `df` has one row per input row in window order; each column in `frame_cols` holds the list of `label_col`
    values the engine put in that row's frame (e.g. `list(order_id) OVER (...)`). `current` is the label of the
    row to explain.
    """
    labels = list(df[label_col])
    cur = labels.index(current)
    titles = titles or frame_cols
    col = 3.8   # x units per frame column; 1 unit = 0.5 inch
    fig, ax = plt.subplots(figsize=(0.5 * (9.1 + col * len(frame_cols)), 0.42 * len(labels) + 1.3))
    for j, c in enumerate(frame_cols):
        members = set(df[c].iloc[cur])
        for i, lab in enumerate(labels):
            inside = lab in members
            ax.add_patch(FancyBboxPatch((j * col + 0.2, i + 0.12), col - 0.4, 0.76, boxstyle="round,pad=0,rounding_size=0.12",
                                        color=NAVY if inside else "white", ec=NAVY if inside else GRID, lw=1))
            ax.text(j * col + col / 2, i + 0.5, _fmt(lab), ha="center", va="center", family=MONO, fontsize=9,
                    color="white" if inside else FADE)
        ax.text(j * col + col / 2, -0.3, titles[j], ha="center", va="bottom", fontsize=8.5, color=INK, linespacing=1.3)
    for i, lab in enumerate(labels):
        row = df.iloc[i]
        ax.text(-0.15, i + 0.5, "  ".join(_fmt(row[k]) for k in df.columns if k not in frame_cols),
                ha="right", va="center", family=MONO, fontsize=8.5, color=INK if i == cur else MUTED,
                weight="bold" if i == cur else "normal")
    ax.add_patch(Rectangle((-0.05, cur + 0.04), len(frame_cols) * col + 0.05, 0.92, fill=False, ec=BAD, lw=1.4))
    ax.text(len(frame_cols) * col + 0.1, cur + 0.5, "◀ current row", va="center", fontsize=8.5, color=BAD)
    ax.set_xlim(-7.5, len(frame_cols) * col + 1.6)
    ax.set_ylim(len(labels) + 0.1, -2.2)
    ax.axis("off")
    ax.set_title(f"Window frames for the current row ({label_col} = {_fmt(current)}): filled = inside the frame",
                 loc="left")


@_figure
def evaluation_order():
    """SQL's logical evaluation order, and where window functions and QUALIFY sit in it."""
    steps = [("FROM / JOIN", "build the rows"), ("WHERE", "filter rows\n(no windows yet)"),
             ("GROUP BY", "collapse groups"), ("HAVING", "filter groups"),
             ("window functions", "OVER (...)\nrows stay"), ("QUALIFY", "filter on\nwindow results"),
             ("SELECT / DISTINCT", "pick columns"), ("ORDER BY", "sort output"), ("LIMIT", "cut")]
    fig, ax = plt.subplots(figsize=(11.5, 2.1))
    w, gap = 1.12, 0.2
    for i, (name, note) in enumerate(steps):
        x = i * (w + gap)
        hot = name in ("window functions", "QUALIFY")
        ax.add_patch(FancyBboxPatch((x, 0), w, 0.9, boxstyle="round,pad=0,rounding_size=0.08",
                                    color=NAVY if hot else "white", ec=NAVY if hot else FADE, lw=1.2))
        ax.text(x + w / 2, 0.45, name.replace(" / ", "\n/ ").replace(" functions", "\nfunctions"), ha="center",
                va="center", fontsize=8.5, weight="bold", color="white" if hot else INK)
        ax.text(x + w / 2, -0.18, note, ha="center", va="top", fontsize=7.8, color=MUTED)
        if i:
            ax.annotate("", xy=(x - 0.02, 0.45), xytext=(x - gap + 0.02, 0.45),
                        arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=1))
    ax.annotate("WHERE runs before any window exists:\n`WHERE row_number() OVER (...) = 1` is an error",
                xy=(1 * (w + gap) + w / 2, 0.92), xytext=(1 * (w + gap) + w / 2, 1.55), ha="center", fontsize=8,
                color=BAD, arrowprops=dict(arrowstyle="-", color=BAD, lw=0.8))
    ax.annotate("rows removed here were still\nvisible to LEAD / ROW_NUMBER",
                xy=(5 * (w + gap) + w / 2, 0.92), xytext=(5 * (w + gap) + w / 2, 1.55), ha="center", fontsize=8,
                color=NAVY, arrowprops=dict(arrowstyle="-", color=NAVY, lw=0.8))
    ax.set_xlim(-0.1, len(steps) * (w + gap))
    ax.set_ylim(-0.75, 2.0)
    ax.axis("off")
    ax.set_title("Logical evaluation order of a SELECT (what the engine guarantees, not how it executes)")


# ---------------------------------------------------------------------------
# Window functions, animated (Lab 0)
# ---------------------------------------------------------------------------

_ROW_PX, _GAP_PX = 30, 14

_PLAYER_CSS = """
#%%ID%% {font-family: system-ui, sans-serif; color: %%INK%%; background: #fff; border: 1px solid %%GRID%%; border-radius: 8px;
     padding: 12px 14px; max-width: 100%; overflow-x: auto; color-scheme: light}
#%%ID%% code {font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 12.5px; background: #f4f2ec;
          padding: 3px 6px; border-radius: 4px; color: %%NAVY%%; white-space: pre-wrap}
#%%ID%% .wa-bar {display: flex; gap: 6px; align-items: center; margin: 10px 0 6px}
#%%ID%% button {font: inherit; font-size: 13px; padding: 3px 10px; border: 1px solid %%FADE%%; border-radius: 5px;
            background: #fff; color: %%INK%%; cursor: pointer}
#%%ID%% button:hover {border-color: %%NAVY%%}
#%%ID%% .wa-count {font-size: 12px; color: %%MUTED%%; margin-left: 6px; font-variant-numeric: tabular-nums}
#%%ID%% .wa-cap {min-height: 2.8em; font-size: 13.5px; line-height: 1.4; margin: 4px 0 10px}
#%%ID%% .wa-key {font-size: 12px; color: %%MUTED%%; margin-bottom: 6px}
#%%ID%% .wa-key span {display: inline-block; width: 12px; height: 12px; vertical-align: -2px; margin: 0 4px 0 12px;
                  border-radius: 2px}
#%%ID%% .wa-grid {position: relative; font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 13px}
#%%ID%% .wa-head {display: flex; height: 24px; align-items: center; font-weight: bold; color: %%MUTED%%;
              border-bottom: 1px solid %%GRID%%; padding-left: 10px}
#%%ID%% .wa-row {position: absolute; left: 0; height: 26px; display: flex; align-items: center; padding-left: 4px;
             border-left: 6px solid; border-radius: 3px; box-sizing: border-box;
             transition: top .7s ease-in-out, opacity .45s, background-color .35s, box-shadow .25s}
#%%ID%% .wa-row.read {background-color: %%READ%% !important}
#%%ID%% .wa-row.cur {box-shadow: 0 0 0 2px %%INK%% inset}
#%%ID%% .wa-row.faded {opacity: .22}
#%%ID%% .wa-cell {flex: none; padding-left: 6px; white-space: pre}
#%%ID%% .wa-res {font-weight: bold; color: %%NAVY%%; transition: opacity .4s .35s}
#%%ID%% .wa-res.hide {opacity: 0; transition: none}
#%%ID%% .wa-reshead {color: %%NAVY%%}
"""

_PLAYER_JS = """
(function () {
  const root = document.getElementById("%%ID%%"), S = %%STEPS%%;
  const rows = root.querySelectorAll(".wa-row"), res = root.querySelectorAll(".wa-res");
  const cap = root.querySelector(".wa-cap"), count = root.querySelector(".wa-count");
  const play = root.querySelector(".wa-play");
  let k = 0, timer = null;
  function show(i) {
    k = Math.max(0, Math.min(S.length - 1, i));
    const s = S[k];
    rows.forEach((r, j) => {
      r.style.top = s.top[j] + "px";
      r.classList.toggle("cur", s.cur === j);
      r.classList.toggle("read", s.read.includes(j));
      r.classList.toggle("faded", s.faded.includes(j));
    });
    res.forEach((c, j) => c.classList.toggle("hide", !s.shown.includes(j)));
    cap.innerHTML = s.caption;
    count.textContent = "step " + (k + 1) + " / " + S.length;
  }
  function stop() { clearInterval(timer); timer = null; play.textContent = "▶ Play"; }
  root.querySelector(".wa-prev").onclick = () => { stop(); show(k - 1); };
  root.querySelector(".wa-next").onclick = () => { stop(); show(k + 1); };
  root.querySelector(".wa-first").onclick = () => { stop(); show(0); };
  play.onclick = () => {
    if (timer) return stop();
    if (k === S.length - 1) show(0);
    play.textContent = "⏸ Pause";
    timer = setInterval(() => { if (k === S.length - 1) stop(); else show(k + 1); }, %%SPEED%%);
  };
  show(0);
})();
"""


def _tint(hex_color: str, alpha: float = 0.12) -> str:
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    return f"rgba({r},{g},{b},{alpha})"


def animate_window(con, table, expr, show, step_text="reads {rows}", keep=None, keep_label=None, speed_ms=1600):
    """Step through one window function, row by row, as a player (Prev / Play / Next) in the notebook output.

    `expr` is the window function as you would write it in a SELECT, e.g.
    "sum(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running". `show` lists the columns of `table`
    to display; the first one must identify a row (it is also the input order). `keep` is an optional SQL condition
    on the result (e.g. "rn = 1"), shown as a final filtering step.

    Which rows each row "reads" is asked from DuckDB too, not re-implemented: `list(<id>) OVER (<same window>)` returns
    the ids of the rows in each row's frame (for `lead`/`lag`, the next/previous row; for ranking functions, the rows
    up to the current one).
    """
    import re

    m = re.fullmatch(r"\s*(?P<func>.+?)\s+OVER\s*\((?P<window>.*)\)\s+AS\s+(?P<alias>\w+)\s*", expr, re.S | re.I)
    if not m:
        raise ValueError("expected '<function> OVER (<window>) AS <name>'")
    func, window, alias = m["func"], m["window"].strip(), m["alias"]
    part = re.search(r"PARTITION\s+BY\s+(.+?)(?=\s+ORDER\s+BY|\s+ROWS|\s+RANGE|$)", window, re.S | re.I)
    order = re.search(r"ORDER\s+BY\s+(.+?)(?=\s+ROWS|\s+RANGE|$)", window, re.S | re.I)
    order_terms = [t.split() for t in order[1].split(",")] if order else []
    no_frame = re.split(r"\s+(?:ROWS|RANGE)\s+", window, flags=re.I)[0]
    name = func.split("(")[0].strip().lower()
    if name == "lead":
        reads_window = no_frame + " ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING"
    elif name == "lag":
        reads_window = no_frame + " ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING"
    elif name in ("row_number", "rank", "dense_rank"):
        reads_window = no_frame + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    else:
        reads_window = window

    cols = ", ".join(show)
    keep_sql = f", coalesce({keep}, false) AS _keep" if keep else ""
    df = con.sql(f"""
        SELECT *{keep_sql} FROM (
            SELECT {cols}, list({show[0]}) OVER ({reads_window}) AS _reads, {expr} FROM {table}
        ) ORDER BY {show[0]}
    """).df()
    return _player(df, sql=re.sub(r"\s+", " ", expr.strip()), partition_by=part[1].strip() if part else None,
                   order_by=[t[0] for t in order_terms], descending=[len(t) > 1 and t[1].upper() == "DESC"
                                                                    for t in order_terms],
                   reads="_reads", result=alias, id_col=show[0], step_text=step_text,
                   keep="_keep" if keep else None, keep_label=keep_label or (f"Keep {keep}" if keep else None),
                   speed_ms=speed_ms)


def _player(df, sql, partition_by=None, order_by=None, descending=False, reads="reads", result="result",
            id_col=None, step_text="reads {rows}", keep=None, keep_label=None, speed_ms=1600):
    """Build the HTML player. `df` holds the input rows in order: the columns to show, `reads` (the list of `id_col`
    values each row reads) and `result` (the value the engine computed); `keep` is an optional boolean column."""
    import html
    import json
    import uuid

    from IPython.display import HTML

    df = df.reset_index(drop=True)
    id_col = id_col or df.columns[0]
    shown_cols = [c for c in df.columns if c not in (reads, result, keep)]
    n = len(df)
    order_by = [order_by] if isinstance(order_by, str) else list(order_by or [])
    desc = descending if isinstance(descending, list) else [descending] * len(order_by)

    # positions: input order -> grouped by partition (input order kept) -> sorted inside each partition
    def tops(order, groups):
        y, out, prev = 0, [0] * n, None
        for i in order:
            if groups is not None and prev is not None and groups[i] != prev:
                y += _GAP_PX
            out[i] = y
            y += _ROW_PX
            prev = groups[i] if groups is not None else None
        return out

    part = df[partition_by].tolist() if partition_by else None
    pkey = {v: k for k, v in enumerate(sorted(set(part), key=str))} if part else None
    grouped = sorted(range(n), key=lambda i: pkey[part[i]]) if part else list(range(n))
    final = (df.assign(_p=[pkey[v] for v in part] if part else 0, _i=range(n))
               .sort_values(["_p", *order_by, "_i"], ascending=[True, *[not d for d in desc], True])._i.tolist())
    top_input, top_grouped, top_final = tops(range(n), None), tops(grouped, part), tops(final, part)

    colors = _colors(part) if part else {}
    color = [colors[part[i]] if part else NAVY for i in range(n)]
    index_of = {v: i for i, v in enumerate(df[id_col])}
    sql_code = f"<code>{html.escape(sql)}</code>"

    steps = [dict(top=top_input, cur=-1, read=[], shown=[], faded=[],
                  caption=f"The input rows, in the order the table returns them. The query: {sql_code}")]
    if part:
        steps.append(dict(top=top_grouped, cur=-1, read=[], shown=[], faded=[],
                          caption=f"<b>PARTITION BY {html.escape(partition_by)}</b>: rows with the same "
                                  f"<code>{html.escape(partition_by)}</code> form one window ({len(pkey)} windows here). "
                                  "Nothing is copied or moved on disk: the engine only knows which rows belong together."))
    if order_by:
        what = ", ".join(f"{c}{' DESC' if d else ''}" for c, d in zip(order_by, desc))
        steps.append(dict(top=top_final, cur=-1, read=[], shown=[], faded=[],
                          caption=f"<b>ORDER BY {html.escape(what)}</b>: inside each window the rows are put in this "
                                  "order. The function will visit them one at a time."))
    shown = []
    for i in final:
        frame = df[reads].iloc[i]
        rows = [index_of[v] for v in frame] if hasattr(frame, "__iter__") else []    # NULL frame: reads nothing
        names = ", ".join(_fmt(df[id_col].iloc[j]) for j in sorted(rows, key=final.index)) if rows else ""
        rows_text = f"{id_col} {names}" if rows else "no row (there is none in this window)"
        shown = shown + [i]
        steps.append(dict(top=top_final, cur=i, read=rows, shown=list(shown), faded=[],
                          caption=f"Current row: <b>{id_col} {_fmt(df[id_col].iloc[i])}</b>. "
                                  f"The function {html.escape(step_text.format(rows=rows_text))} "
                                  f"→ <b>{html.escape(result)} = {html.escape(_fmt(df[result].iloc[i]))}</b>"))
    if keep:
        gone = [i for i in range(n) if not df[keep].iloc[i]]
        steps.append(dict(top=top_final, cur=-1, read=[], shown=list(shown), faded=gone,
                          caption=f"<b>{html.escape(keep_label or keep)}</b>: the rows where it is false are removed. "
                                  "This runs <b>after</b> every window value was computed, so the removed rows could "
                                  "still be read. The rows left are the result."))
    if not keep:
        steps.append(dict(top=top_final, cur=-1, read=[], shown=list(shown), faded=[],
                          caption=f"Done. Every row kept its own values and got one more column, "
                                  f"<code>{html.escape(result)}</code>: the window's answer for that row."))

    widths = [max(len(str(c)), *(len(_fmt(v)) for v in df[c])) + 2 for c in shown_cols]
    res_w = max(len(result), *(len(_fmt(v)) for v in df[result])) + 2
    row_w = f"calc({sum(widths) + res_w}ch + {6 * (len(widths) + 1) + 16}px)"   # characters + cell padding + border
    head = "".join(f'<span class="wa-cell" style="width:{w}ch">{html.escape(str(c))}</span>'
                   for c, w in zip(shown_cols, widths))
    head += f'<span class="wa-cell wa-reshead" style="width:{res_w}ch">{html.escape(result)}</span>'
    body = []
    for i in range(n):
        cells = "".join(f'<span class="wa-cell" style="width:{w}ch">{html.escape(_fmt(df[c].iloc[i]))}</span>'
                        for c, w in zip(shown_cols, widths))
        cells += f'<span class="wa-cell wa-res hide" style="width:{res_w}ch">{html.escape(_fmt(df[result].iloc[i]))}</span>'
        body.append(f'<div class="wa-row" style="top:{top_input[i]}px; border-left-color:{color[i]}; '
                    f'background-color:{_tint(color[i])}; width:{row_w}">{cells}</div>')
    height = max(top_final + top_input) + _ROW_PX + 4

    uid = "wa-" + uuid.uuid4().hex[:10]
    tokens = {"ID": uid, "INK": INK, "GRID": GRID, "FADE": FADE, "NAVY": NAVY, "MUTED": MUTED, "READ": "#f2dfb4",
              "STEPS": json.dumps(steps), "SPEED": str(speed_ms)}
    css, js = _PLAYER_CSS, _PLAYER_JS
    for k, v in tokens.items():
        css, js = css.replace(f"%%{k}%%", v), js.replace(f"%%{k}%%", v)
    page = f"""
<div id="{uid}">
  <style>{css}</style>
  <div class="wa-bar">
    <button class="wa-first" title="back to the start">⏮</button>
    <button class="wa-prev">◀ Prev</button>
    <button class="wa-play">▶ Play</button>
    <button class="wa-next">Next ▶</button>
    <span class="wa-count"></span>
  </div>
  <div class="wa-cap">{steps[0]["caption"]}</div>
  <div class="wa-key"><span style="background:#fff; box-shadow:0 0 0 2px {INK} inset"></span>current row
    <span style="background:#f2dfb4"></span>rows the function reads
    <span style="background:{NAVY}"></span>new column</div>
  <div class="wa-grid" style="width:{row_w}">
    <div class="wa-head">{head}</div>
    <div style="position:relative; height:{height}px; margin-top:6px">{''.join(body)}</div>
  </div>
  <script>{js}</script>
</div>"""
    return HTML(page)


# ---------------------------------------------------------------------------
# Validity intervals (Lab 0, Lab 1.4)
# ---------------------------------------------------------------------------

@_figure
def timelines(panels, key="customer_id", label="city", start="valid_from", end="valid_to", since=None, until=None):
    """SCD2-style histories: one lane per key, one bar per version, open bars run to `until`.

    `panels` is a list of (title, df). A key whose last version is closed (end not NULL) was deleted: it gets a ✕.
    Versions that start before `since` are drawn from `since`.
    """
    all_df = pd.concat([d for _, d in panels])
    colors = _colors(all_df[label])
    t0 = pd.to_datetime(since) if since is not None else pd.to_datetime(all_df[start]).min()
    t1 = pd.to_datetime(until) if until is not None else max(pd.to_datetime(all_df[start]).max(),
                                                               pd.to_datetime(all_df[end]).max()) + timedelta(days=2)
    keys = sorted(all_df[key].unique(), key=str)
    fig, axes = plt.subplots(len(panels), 1, figsize=(11, (0.48 * len(keys) + 0.9) * len(panels)), squeeze=False)
    for ax, (title, df) in zip(axes[:, 0], panels):
        df = df.assign(_s=pd.to_datetime(df[start]).clip(lower=t0), _e=pd.to_datetime(df[end]))
        for lane, k in enumerate(keys):
            rows = df[df[key] == k].sort_values("_s")
            for _, r in rows.iterrows():
                e = r._e if not pd.isna(r._e) else t1
                if e <= r._s:   # an impossible version: zero length, or ends before it starts
                    what = "zero length" if e == r._s else "ends before it starts"
                    ax.barh(lane - 0.42, (r._s - e) or (t1 - t0) * 0.004, left=e, height=0.22, color="white",
                            edgecolor=BAD, hatch="////", linewidth=1.2, zorder=3)
                    ax.text(max(e, r._s), lane - 0.42, f"  {_fmt(r[label])} {_fmt(r._s)} → {_fmt(e)}: {what}",
                            fontsize=7.8, color=BAD, va="center")
                    continue
                ax.barh(lane, (e - r._s), left=r._s, height=0.56, color=colors.get(r[label], MUTED),
                        alpha=0.9, edgecolor="white", linewidth=2)
                if (e - r._s) > (t1 - t0) * 0.012 * (len(_fmt(r[label])) + 1):   # label only where it fits
                    ax.text(r._s + (e - r._s) / 2, lane, _fmt(r[label]), ha="center", va="center", fontsize=8.5,
                            color="white", weight="bold")
                if pd.isna(r._e):
                    ax.text(t1, lane, " ▶ current", va="center", fontsize=8, color=MUTED)
            if len(rows) and not pd.isna(rows._e.iloc[-1]) and (rows._e.iloc[-1] > rows._s.iloc[-1]):
                ax.plot([rows._e.iloc[-1]], [lane], marker="X", ms=9, color=BAD, mec="white", mew=1.5)
                ax.text(rows._e.iloc[-1], lane + 0.42, " deleted", fontsize=7.5, color=BAD, va="center")
        ax.set_yticks(range(len(keys)), [f"{key} {k}" for k in keys])
        ax.set_ylim(len(keys) - 0.4, -0.8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax.set_xlim(t0 - (t1 - t0) * 0.01, t1 + (t1 - t0) * 0.09)
        ax.grid(axis="x")
        ax.set_axisbelow(True)
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)
        ax.set_title(title)
    handles = [Line2D([], [], color=c, lw=7, label=_fmt(k)) for k, c in colors.items()]
    axes[0, 0].legend(handles=handles, loc="lower right", bbox_to_anchor=(1, 1.0), ncol=len(handles), fontsize=8.5)
    fig.tight_layout()


# ---------------------------------------------------------------------------
# Lab 1.2 · Parquet
# ---------------------------------------------------------------------------

@_figure
def row_group_ranges(ranges: dict, lo, hi, column="created_at"):
    """[min, max] of `column` per row group, per file, against a filter range [lo, hi).

    `ranges` maps a file name to a DataFrame with `row_group_id`, `stats_min`, `stats_max` (from parquet_metadata).
    """
    lo, hi = pd.Timestamp(lo), pd.Timestamp(hi)
    fig, axes = plt.subplots(1, len(ranges), figsize=(5.6 * len(ranges), 4.2), sharex=True, squeeze=False)
    for ax, (name, m) in zip(axes[0], ranges.items()):
        m = m.sort_values("row_group_id")
        mn, mx = pd.to_datetime(m.stats_min), pd.to_datetime(m.stats_max)
        read = (mx >= lo) & (mn < hi)
        ax.axvspan(lo, hi, color=SERIES[3], alpha=0.16, lw=0)
        ax.hlines(m.row_group_id, mn, mx, colors=[NAVY if r else FADE for r in read], lw=1.8)
        for edge in (lo, hi):   # the filter's edges stay visible on top of a solid block of row groups
            ax.axvline(edge, color=SERIES[3], lw=1.5, zorder=3)
        ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=[1, 15]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax.set_ylim(m.row_group_id.max() + 1, -1)
        ax.set_ylabel("row group")
        ax.grid(axis="x")
        ax.set_axisbelow(True)
        ax.set_title(f"{name}: read {int(read.sum())} of {len(m)} row groups")
    axes[0, 0].legend(handles=[Line2D([], [], color=NAVY, lw=3, label="range overlaps the filter: must read"),
                               Line2D([], [], color=FADE, lw=3, label="skipped from the footer alone"),
                               Rectangle((0, 0), 1, 1, color=SERIES[3], alpha=0.25, label=f"filter on {column}")],
                      loc="upper left", bbox_to_anchor=(0, -0.22), ncol=3, fontsize=8.5)
    fig.suptitle(f"Each line is one row group's [min, max] of {column}, read from the footer", x=0.01, ha="left",
                 weight="bold", fontsize=11)
    fig.tight_layout()


@_figure
def bars(df, label, values, titles=None, suptitle=None, fmt=None, ylabel=None):
    """Horizontal bars, one small panel per measure (never two scales on one axis).

    Values are labelled at the bar tips with `fmt`; by default whole numbers get no decimals, others one.
    `df` may also be a DuckDB result, a dict of lists or a list of dicts, so notebooks need no pandas.
    """
    df = df.df() if hasattr(df, "df") else pd.DataFrame(df)
    titles = titles or values
    n = len(df)
    fig, axes = plt.subplots(1, len(values), figsize=(4.6 * len(values) + 0.6, 0.38 * n + 1.3), squeeze=False)
    for ax, v, t in zip(axes[0], values, titles):
        y = range(n)
        ax.barh(y, df[v], height=min(0.62, 0.62), color=NAVY)
        top = df[v].max()
        whole = all(float(x).is_integer() for x in df[v])
        for yi, val in zip(y, df[v]):
            text = (fmt or ("{:,.0f}" if whole else "{:,.1f}")).format(val)
            ax.text(val + top * 0.02, yi, text, va="center", fontsize=8.5, color=INK)
        ax.set_yticks(list(y), [_fmt(x) for x in df[label]])
        ax.set_ylim(n - 0.4, -0.6)
        ax.set_xlim(0, top * 1.22)
        ax.grid(axis="x")
        ax.set_axisbelow(True)
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)
        ax.set_title(t)
    if ylabel:
        axes[0, 0].set_ylabel(ylabel)
    if suptitle:
        fig.suptitle(suptitle, x=0.01, ha="left", weight="bold", fontsize=11)
    fig.tight_layout()


# ---------------------------------------------------------------------------
# Lab 1.4 · CDC
# ---------------------------------------------------------------------------

@_figure
def arrivals(df, first=None, last=None):
    """Log position (`lsn`) against arrival order: out-of-order and redelivered events stand out.

    `df` needs `arrival`, `lsn`, and boolean columns `late` (lsn below an earlier arrival's) and `duplicate`
    (an lsn already delivered).
    """
    d = df[(df.arrival >= (first or df.arrival.min())) & (df.arrival <= (last or df.arrival.max()))]
    d = d.assign(late=d.late.fillna(False).astype(bool), duplicate=d.duplicate.fillna(False).astype(bool))
    fig, ax = plt.subplots(figsize=(11, 4))
    ok = d[~d.late & ~d.duplicate]
    ax.plot(d.arrival, d.lsn.cummax(), color=GRID, lw=2, zorder=1, label="highest lsn seen so far")
    ax.scatter(ok.arrival, ok.lsn, s=22, color=NAVY, zorder=2, ec="white", lw=0.6, label="in order")
    ax.scatter(d[d.late].arrival, d[d.late].lsn, s=44, marker="X", color=BAD, ec="white", lw=0.8, zorder=3,
               label=f"out of order ({int(d.late.sum())})")
    ax.scatter(d[d.duplicate].arrival, d[d.duplicate].lsn, s=60, facecolors="none", edgecolors=SERIES[3], lw=1.6,
               zorder=3, label=f"redelivered ({int(d.duplicate.sum())})")
    ax.ticklabel_format(axis="y", style="plain", useOffset=False)
    ax.set_xlabel("arrival (position in the feed)")
    ax.set_ylabel("lsn (position in the database log)")
    ax.grid(True)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", fontsize=8.5)
    ax.set_title(f"Arrivals {d.arrival.min():,}–{d.arrival.max():,}: anything below the grey line arrived late")
    fig.tight_layout()


# ---------------------------------------------------------------------------
# Lab 3.1 · Iceberg
# ---------------------------------------------------------------------------

@_figure
def iceberg_tree(metadata_location, max_files=40):
    """The metadata tree of an Iceberg table: metadata.json → snapshots (manifest lists) → manifests → data files.

    Reads the given metadata file with PyIceberg; lines from the current snapshot are dark, older snapshots grey.
    """
    from pyiceberg.table import StaticTable

    t = StaticTable.from_metadata(str(metadata_location))
    io, md = t.io, t.metadata
    current = md.current_snapshot_id
    snaps = sorted(md.snapshots, key=lambda s: s.sequence_number or 0)

    manifests, files, edges = {}, {}, []    # path -> label; (from, to, is_current)
    for s in snaps:
        for mf in s.manifests(io):
            name = Path(mf.manifest_path).name
            manifests.setdefault(mf.manifest_path, name[:8] + "…" + name[name.rfind("-m"):] if "-m" in name else name)
            edges.append((("s", s.snapshot_id), ("m", mf.manifest_path), s.snapshot_id == current))
            for e in mf.fetch_manifest_entry(io, discard_deleted=True):
                p = e.data_file.file_path
                if p not in files:
                    files[p] = f"{Path(p).parent.name}/…{Path(p).stem[-6:]}"
                edges.append((("m", mf.manifest_path), ("f", p), s.snapshot_id == current))
    files = dict(list(sorted(files.items(), key=lambda kv: kv[1]))[:max_files])
    live_m = {b[1] for a, b, cur in edges if cur and b[0] == "m"}
    live_f = {b[1] for a, b, cur in edges if cur and b[0] == "f"}

    rows = max(len(snaps), len(manifests), len(files))
    fig, ax = plt.subplots(figsize=(13, 0.3 * rows + 1.6))
    xs = {"c": 0, "j": 1.6, "s": 3.4, "m": 6.0, "f": 8.8}

    def ys(n):
        return [rows * (i + 0.5) / n for i in range(n)]

    pos = {("c", 0): (xs["c"], rows / 2), ("j", 0): (xs["j"], rows / 2)}
    pos.update({("s", s.snapshot_id): (xs["s"], y) for s, y in zip(snaps, ys(len(snaps)))})
    pos.update({("m", p): (xs["m"], y) for p, y in zip(manifests, ys(len(manifests)))})
    pos.update({("f", p): (xs["f"], y) for p, y in zip(files, ys(len(files)))})

    def line(a, b, cur):
        if a in pos and b in pos:
            (x0, y0), (x1, y1) = pos[a], pos[b]
            ax.plot([x0 + 1.1, x1 - 0.05], [y0, y1], color=NAVY if cur else GRID, lw=1.1 if cur else 0.9,
                    zorder=2 if cur else 1)

    line(("c", 0), ("j", 0), True)
    for s in snaps:
        line(("j", 0), ("s", s.snapshot_id), s.snapshot_id == current)
    for a, b, cur in sorted(edges, key=lambda e: e[2]):
        line(a, b, cur)

    def node(key, text, live):
        x, y = pos[key]
        ax.text(x, y, text, va="center", ha="left", fontsize=7.6, family=MONO, color="white" if live else MUTED,
                bbox=dict(boxstyle="round,pad=0.25", fc=NAVY if live else "white", ec=NAVY if live else FADE, lw=0.8),
                zorder=3)

    node(("c", 0), "catalog", True)
    node(("j", 0), Path(str(metadata_location)).name[:14] + "….json", True)
    for i, s in enumerate(snaps, 1):
        node(("s", s.snapshot_id), f"S{i} {s.summary.operation.value}" + (" ★" if s.snapshot_id == current else ""),
             s.snapshot_id == current)
    for p, lab in manifests.items():
        node(("m", p), lab, p in live_m)
    for p, lab in files.items():
        node(("f", p), lab, p in live_f)
    for k, title in [("c", "catalog"), ("j", "metadata.json"), ("s", "snapshots\n(1 manifest list each)"),
                     ("m", "manifests"), ("f", "data files (Parquet)")]:
        ax.text(xs[k], -0.6, title, fontsize=8.5, weight="bold", color=MUTED, va="bottom")
    ax.set_xlim(-0.2, 11.4)
    ax.set_ylim(rows + 0.3, -1.6)
    ax.axis("off")
    ax.set_title(f"{len(snaps)} snapshots share {len(manifests)} manifests and {len(files)} data files\n"
                 f"dark = reachable from the current snapshot ★ · grey = only via time travel")


# ---------------------------------------------------------------------------
# tools/iceberg · Iceberg on a lightboard
# ---------------------------------------------------------------------------
# Step-by-step players in the style of a lightboard talk: a dark board on which every step draws one more piece.
# The Iceberg boards are drawn from the table's own metadata files; the steps only choose what to show, and when.

_LB = {"BG": "#0b1622", "CHALK": "#e8eef5", "SOFT": "#9fb2c8", "JSON": "#b3a6ff", "SNAP": "#ff9a85", "MAN": "#4fd8b8",
       "FILE": "#ffcf5c", "RED": "#ff6b6b", "OK": "#7ee787", "WA": "#7cc4ff", "WB": "#ff7ac6",
       "HAND": "Caveat, 'Segoe Print', 'Bradley Hand', 'Comic Sans MS', cursive"}
# Tags spell out what a highlight means, so colour and glow are never the only cue.
_LB_TAGS = {"new": "new", "reuse": "reused", "skip": "skipped", "read": "opened", "gone": "still on disk",
            "bad": "✗", "ok": "✓", "wa": "writer A", "wb": "writer B", "why": "old or new?"}

_LB_CSS = """
#%%ID%% {background: %%BG%%; color: %%CHALK%%; border-color: #22344a; color-scheme: dark}
#%%ID%% button {background: #13243a; color: %%CHALK%%; border-color: #36506e}
#%%ID%% button:hover {border-color: %%CHALK%%}
#%%ID%% .st-count, #%%ID%% .st-key {color: %%SOFT%%}
#%%ID%% .st-cap {font-size: 14.5px; min-height: 4.4em}
#%%ID%% .st-cap b {color: #fff}
#%%ID%% code {background: #16283d; color: #ffe2a3}
#%%ID%% .lb-title {font-family: %%HAND%%; font-size: 29px; line-height: 1.1; color: #fff}
#%%ID%% .lb-t {border-collapse: collapse; margin-top: 6px; font-size: 12.5px}
#%%ID%% .lb-t td, #%%ID%% .lb-t th {padding: 1px 16px 1px 0; text-align: left;
     font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace}
#%%ID%% .lb-t th {color: %%SOFT%%; font-weight: normal}
#%%ID%% svg {display: block; width: 100%; height: auto; margin-top: 4px}
#%%ID%% svg text {font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 11.5px; fill: %%CHALK%%}
#%%ID%% svg text.hand {font-family: %%HAND%%; font-size: 23px; fill: %%SOFT%%}
#%%ID%% svg text.sub {font-size: 10.5px; fill: %%SOFT%%}
#%%ID%% svg text.big {font-size: 13px; font-weight: bold}
#%%ID%% .nd, #%%ID%% .ed {transition: opacity .45s}
#%%ID%% .hid {opacity: 0}
#%%ID%% .nd.dim {opacity: .3}
#%%ID%% .ed.dim {opacity: .14}
#%%ID%% .box {fill: %%BG%%; stroke: currentColor; stroke-width: 1.7}
#%%ID%% .fold {fill: none; stroke: currentColor; stroke-width: 1.2}
#%%ID%% .frame .box {fill: none}
#%%ID%% .ed {fill: none; stroke: currentColor; stroke-width: 1.4}
#%%ID%% .ghost .box {stroke-dasharray: .02 .015}
#%%ID%% .row .box {stroke-opacity: 0; fill: none}
#%%ID%% .row.cur .box, #%%ID%% .row.new .box, #%%ID%% .row.bad .box {stroke-opacity: 1}
#%%ID%% .new .box, #%%ID%% .ed.new {stroke-width: 2.6; filter: url(#%%ID%%-glow)}
#%%ID%% .cur .box, #%%ID%% .ed.cur {stroke: #fff; stroke-width: 2.6; filter: url(#%%ID%%-glow)}
#%%ID%% .new .box, #%%ID%% .ed.new, #%%ID%% .cur .box, #%%ID%% .ed.cur {stroke-dasharray: 1;
     animation: lb-draw 1.1s ease-out}
#%%ID%% .wa .box, #%%ID%% .ed.wa {stroke: %%WA%%}
#%%ID%% .wb .box, #%%ID%% .ed.wb {stroke: %%WB%%}
#%%ID%% .ok .box {stroke: %%OK%%; stroke-width: 2.6; filter: url(#%%ID%%-glow)}
#%%ID%% .bad .box, #%%ID%% .ed.bad {stroke: %%RED%%; stroke-width: 2.6; filter: url(#%%ID%%-glow)}
#%%ID%% .bad.ghost .box {stroke-dasharray: .02 .015; animation: none}
#%%ID%% .ed.del {stroke: %%RED%%; stroke-dasharray: .025 .02; animation: none}
#%%ID%% .nd.skip, #%%ID%% .nd.gone {opacity: .5}
#%%ID%% .strike {stroke: %%RED%%; stroke-width: 2.2; opacity: 0}
#%%ID%% .skip .strike, #%%ID%% .gone .strike {opacity: 1}
#%%ID%% .ok-t {fill: %%OK%% !important}
#%%ID%% .bad-t {fill: %%RED%% !important}
#%%ID%% .tag {opacity: 0; font-size: 11px !important; font-weight: bold; fill: #ffe2a3 !important; transition: opacity .35s}
#%%ID%% .t-bad, #%%ID%% .t-gone, #%%ID%% .t-skip, #%%ID%% .t-why {fill: %%RED%% !important}
#%%ID%% .t-ok {fill: %%OK%% !important}
#%%ID%% .t-wa {fill: %%WA%% !important}
#%%ID%% .t-wb {fill: %%WB%% !important}
@keyframes lb-draw {from {stroke-dashoffset: 1} to {stroke-dashoffset: 0}}
""" + "".join(f"#%%ID%% .{t} > .t-{t} {{opacity: 1}}\n" for t in _LB_TAGS)


def _lb_player(title, w, h, inner, steps, key="", speed_ms=2800):
    """A lightboard player: an SVG board of `w` x `h` whose elements change class step by step (see `_stepper`)."""
    import html

    css = _LB_CSS
    for k, v in _LB.items():
        css = css.replace(f"%%{k}%%", v)
    svg = (f'<svg viewBox="0 0 {w:.0f} {h:.0f}" style="max-width:{w * 1.15:.0f}px">'
           f'<defs><filter id="%%ID%%-glow" filterUnits="userSpaceOnUse" x="0" y="0" width="{w:.0f}" height="{h:.0f}">'
           '<feGaussianBlur stdDeviation="2.4" result="b"/>'
           '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>'
           f'{inner}</svg>')
    font = '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Caveat:wght@600&display=swap">'
    return _stepper(svg, steps, css=css, key=key, speed_ms=speed_ms,
                    title=f'{font}<div class="lb-title">{html.escape(title)}</div>')


def _lb_node(k, x, y, w, h, color, title, sub="", file=False, tags=(), tag_at="above", cls="", title_cls=""):
    """A card on the board: a box (a Parquet-style dog-eared page if `file`), a title, an optional second line, a
    strike line shown when skipped, and the tags it can show (see _LB_TAGS)."""
    import html

    e = html.escape
    if file:
        shape = (f'<path class="box" pathLength="1" d="M{x:.1f},{y:.1f} H{x + w - 11:.1f} L{x + w:.1f},{y + 11:.1f} '
                 f'V{y + h:.1f} H{x:.1f} Z"/>'
                 f'<path class="fold" d="M{x + w - 11:.1f},{y:.1f} V{y + 11:.1f} H{x + w:.1f}"/>')
    else:
        shape = f'<rect class="box" pathLength="1" x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="7"/>'
    cx = x + w / 2
    text = (f'<text class="{title_cls}" x="{cx:.1f}" y="{y + (h / 2 - 3 if sub else h / 2 + 4):.1f}" '
            f'text-anchor="middle">{e(title)}</text>')
    if sub:
        text += f'<text class="sub" x="{cx:.1f}" y="{y + h / 2 + 12:.1f}" text-anchor="middle">{e(sub)}</text>'
    strike = f'<path class="strike" d="M{x + 5:.1f},{y + h - 5:.1f} L{x + w - 5:.1f},{y + 5:.1f}"/>'
    tx, ty, anchor = {"above": (x + w, y - 6, "end"), "below": (cx, y + h + 15, "middle"),
                      "right": (x + w + 8, y + h / 2 + 4, "start")}[tag_at]
    tg = "".join(f'<text class="tag t-{t}" x="{tx:.1f}" y="{ty:.1f}" text-anchor="{anchor}">{e(_LB_TAGS[t])}</text>'
                 for t in tags)
    return f'<g class="{f"nd {cls}".strip()}" data-k="{k}" style="color:{color}">{shape}{text}{strike}{tg}</g>'


def _lb_edge(k, a, b, color, bend="v"):
    """A line from point a to point b: an S-curve that leaves and arrives vertically (or horizontally, bend="h")."""
    (x1, y1), (x2, y2) = a, b
    if bend == "v":
        m = (y1 + y2) / 2
        d = f"M{x1:.1f},{y1:.1f} C{x1:.1f},{m:.1f} {x2:.1f},{m:.1f} {x2:.1f},{y2:.1f}"
    else:
        m = (x1 + x2) / 2
        d = f"M{x1:.1f},{y1:.1f} C{m:.1f},{y1:.1f} {m:.1f},{y2:.1f} {x2:.1f},{y2:.1f}"
    return f'<path class="ed" data-k="{k}" pathLength="1" style="color:{color}" d="{d}"/>'


def _lb_mark(st, keys, *cls):
    """A copy of the state `st` in which `keys` are shown (not hidden or faded) and get the classes `cls`."""
    out = dict(st)
    for k in keys:
        kept = [c for c in out.get(k, "").split() if c not in ("hid", "dim")]
        out[k] = " ".join(dict.fromkeys(kept + ["on", *cls]))
    return out


def _lb_text(x, y, text, cls="hand", anchor="start", k=None, color=None):
    import html

    attrs = (f' data-k="{k}"' if k else "") + (f' style="fill:{color}"' if color else "")
    tag = f'<text class="{cls}" x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}">{html.escape(text)}</text>'
    return f'<g class="nd"{attrs}>{tag}</g>' if k else tag


def _lb_scene(location):
    """Everything an Iceberg metadata file reaches, plus the metadata files before it (its metadata log): versions
    (one per metadata file), snapshots, manifests and data files, read with PyIceberg."""
    from pyiceberg.manifest import ManifestEntryStatus
    from pyiceberg.table import StaticTable

    t = StaticTable.from_metadata(str(location))
    md, io = t.metadata, t.io
    versions = []
    for p in [e.metadata_file for e in md.metadata_log] + [str(location)]:
        m = md if p == str(location) else StaticTable.from_metadata(p).metadata
        versions.append(dict(path=p, name=Path(p).name, snaps=[s.snapshot_id for s in m.snapshots],
                             current=m.current_snapshot_id, schema=m.current_schema_id, spec=m.default_spec_id))
    snaps, manifests, files = {}, {}, {}
    for n, s in enumerate(sorted(md.snapshots, key=lambda s: s.sequence_number or 0), 1):
        mfs = s.manifests(io)
        snaps[s.snapshot_id] = dict(n=n, snap=s, manifests=[mf.manifest_path for mf in mfs])
        for mf in mfs:
            if mf.manifest_path in manifests:
                continue
            entries = []
            for e in mf.fetch_manifest_entry(io, discard_deleted=False):
                entries.append((e.data_file.file_path, e.status == ManifestEntryStatus.DELETED))
                files.setdefault(e.data_file.file_path, dict(data=e.data_file, order=len(files)))
            manifests[mf.manifest_path] = dict(file=mf, entries=entries, first=n)
    manifests = dict(sorted(manifests.items(), key=lambda kv: (kv[1]["first"], Path(kv[0]).name)))
    for p, f in files.items():
        value = Path(p).parent.name.split("=", 1)[-1]
        f["label"] = value[5:] if len(value) == 10 and value[4] == "-" else value[:10]    # a day: show MM-DD
    files = dict(sorted(files.items(), key=lambda kv: (kv[1]["label"], kv[1]["order"])))
    meta_dir = Path(str(location)).parent
    return dict(table=t, name=f"{meta_dir.parent.parent.name}.{meta_dir.parent.name}", versions=versions, snaps=snaps,
                manifests=manifests, files=files, mi={p: i for i, p in enumerate(manifests)},
                fi={p: i for i, p in enumerate(files)})


def _lb_jname(sc, v, full=False):
    name = sc["versions"][v]["name"]
    return name if full else name[:10] + "…"


def _lb_mname(path):
    name = Path(path).name.removesuffix(".avro")
    return name[:4] + "…" + name[name.rfind("-m"):] if "-m" in name else name[:8] + "…"


def _lb_live(sc, sid):
    """Data files in snapshot `sid` (its manifests' entries that are not marked deleted)."""
    if sid not in sc["snaps"]:
        return set()
    return {fp for p in sc["snaps"][sid]["manifests"] for fp, deleted in sc["manifests"][p]["entries"] if not deleted}


def _lb_rows(sc, files):
    return sum(sc["files"][fp]["data"].record_count for fp in files)


def _lb_layout(sc, engines=()):
    widths = {"x": 150, "c": 236, "j": 112, "s": 112, "m": 104, "f": 84}
    rows = {"x": [f"x:{i}" for i in range(len(engines))]} if engines else {}
    rows.update(c=["c"], j=[f"j:{i}" for i in range(len(sc["versions"]))], s=[f"s:{sid}" for sid in sc["snaps"]],
                m=[f"m:{i}" for i in range(len(sc["manifests"]))], f=[f"f:{i}" for i in range(len(sc["files"]))])
    x0, gap, h, pitch = 178, 14, 44, 96
    cw = max(max(len(keys) * (widths[r] + gap) for r, keys in rows.items()), 640)
    pos, row_y = {}, {}
    for r_i, (r, keys) in enumerate(rows.items()):
        y = 34 + r_i * pitch
        row_y[r] = y
        start = x0 + (cw - len(keys) * (widths[r] + gap) + gap) / 2
        for i, k in enumerate(keys):
            pos[k] = (start + i * (widths[r] + gap), y, widths[r], h)
    return pos, row_y, x0 + cw + 16, 34 + (len(rows) - 1) * pitch + h + 30


def _lb_board(sc, engines=()):
    """The SVG of a scene: one row per layer, every node and edge it can show. `engines` are (name, note, via) with
    via "catalog" or "metadata" (the engine was handed the metadata file path)."""
    pos, row_y, w, h = _lb_layout(sc, engines)
    C = _LB
    vers, snaps, mans, files, mi, fi = (sc[k] for k in ("versions", "snaps", "manifests", "files", "mi", "fi"))

    def top(k):
        x, y, bw, _ = pos[k]
        return x + bw / 2, y

    def bot(k):
        x, y, bw, bh = pos[k]
        return x + bw / 2, y + bh

    labels = {"x": ("engines", ""), "c": ("catalog", "one pointer per table"), "j": ("metadata files", "one per commit"),
              "s": ("manifest lists", "one per snapshot"), "m": ("manifests", "lists of files + stats"),
              "f": ("data files", "Parquet")}
    out = []
    for r, y in row_y.items():
        name, note = labels[r]
        out.append(_lb_text(14, y + 22, name))
        if note:
            out.append(_lb_text(14, y + 40, note, cls="sub"))
    for k, (_, _, via) in enumerate(engines):
        if via == "catalog":
            out.append(_lb_edge(f"xc:{k}", bot(f"x:{k}"), top("c"), C["CHALK"]))
        else:
            out += [_lb_edge(f"xj:{k}:{i}", bot(f"x:{k}"), top(f"j:{i}"), C["CHALK"]) for i in range(len(vers))]
    for i, v in enumerate(vers):
        out.append(_lb_edge(f"cj:{i}", bot("c"), top(f"j:{i}"), C["CHALK"]))
        out += [_lb_edge(f"js:{i}:{sid}", bot(f"j:{i}"), top(f"s:{sid}"), C["JSON"]) for sid in v["snaps"]]
    for sid, s in snaps.items():
        out += [_lb_edge(f"sm:{sid}:{mi[p]}", bot(f"s:{sid}"), top(f"m:{mi[p]}"), C["SNAP"]) for p in s["manifests"]]
    for p, m in mans.items():
        out += [_lb_edge(f"mf:{mi[p]}:{fi[fp]}", bot(f"m:{mi[p]}"), top(f"f:{fi[fp]}"), C["MAN"])
                for fp, _ in m["entries"]]

    for k, (name, note, _) in enumerate(engines):
        out.append(_lb_node(f"x:{k}", *pos[f"x:{k}"], C["CHALK"], name, note, tags=("ok", "bad", "wa", "wb"),
                            title_cls="big"))
    out.append(_lb_node("c", *pos["c"], C["CHALK"], "catalog", sc["name"], title_cls="big"))
    for i, v in enumerate(vers):
        cur = snaps[v["current"]]["n"] if v["current"] in snaps else None
        sub = f"→ S{cur}" if cur else "no snapshot"
        out.append(_lb_node(f"j:{i}", *pos[f"j:{i}"], C["JSON"], _lb_jname(sc, i), sub, tags=("new", "wa", "wb")))
    for sid, s in snaps.items():
        out.append(_lb_node(f"s:{sid}", *pos[f"s:{sid}"], C["SNAP"], f"S{s['n']} {s['snap'].summary.operation.value}",
                            f"snap-{str(sid)[:4]}….avro", tags=("new", "wa", "wb")))
    for p, m in mans.items():
        mf = m["file"]
        counts = [f"+{mf.added_files_count} new" if mf.added_files_count else "",
                  f"{mf.existing_files_count} kept" if mf.existing_files_count else "",
                  f"{mf.deleted_files_count} deleted" if mf.deleted_files_count else ""]
        out.append(_lb_node(f"m:{mi[p]}", *pos[f"m:{mi[p]}"], C["MAN"], _lb_mname(p), " · ".join(c for c in counts if c),
                            tags=("new", "reuse", "skip", "wa", "wb")))
    for fp, f in files.items():
        out.append(_lb_node(f"f:{fi[fp]}", *pos[f"f:{fi[fp]}"], C["FILE"], f["label"],
                            f"{f['data'].record_count:,} rows", file=True,
                            tags=("new", "skip", "read", "gone", "wa", "wb"), tag_at="below"))
    return w, h, "".join(out)


def _lb_view(sc, v, s="current"):
    """Element classes when the catalog points to metadata version `v` (-1: no table yet) and readers use snapshot
    `s` (default: v's current one). Earlier versions, snapshots and files are faded; later ones hidden."""
    vers, snaps, mans, mi, fi = (sc[k] for k in ("versions", "snaps", "manifests", "mi", "fi"))
    s = vers[v]["current"] if s == "current" and v >= 0 else s
    known = set(vers[v]["snaps"]) if v >= 0 else set()
    st = {"c": "on"}
    for i, ver in enumerate(vers):
        st[f"j:{i}"] = "on" if i == v else "dim" if i < v else "hid"
        st[f"cj:{i}"] = "on" if i == v else "hid"
        for sid in ver["snaps"]:
            st[f"js:{i}:{sid}"] = ("on" if sid == s else "dim") if i == v else "hid"
    live_m = set(snaps[s]["manifests"]) if s in snaps else set()
    seen_m = {p for sid in known for p in snaps[sid]["manifests"]}
    for sid, sn in snaps.items():
        cls = "on" if sid == s else "dim" if sid in known else "hid"
        st[f"s:{sid}"] = cls
        st.update({f"sm:{sid}:{mi[p]}": cls for p in sn["manifests"]})
    live_f, seen_f = set(), set()
    for p, m in mans.items():
        cls = "on" if p in live_m else "dim" if p in seen_m else "hid"
        st[f"m:{mi[p]}"] = cls
        for fp, deleted in m["entries"]:
            st[f"mf:{mi[p]}:{fi[fp]}"] = cls + (" del" if deleted else "")
            seen_f |= {fp} if cls != "hid" else set()
            live_f |= {fp} if cls == "on" and not deleted else set()
    st.update({f"f:{fi[fp]}": "on" if fp in live_f else "dim" if fp in seen_f else "hid" for fp in sc["files"]})
    return st


def _lb_tree(sc, v, sid):
    """Keys of everything a reader of snapshot `sid` follows from metadata version `v`, down to the live files."""
    mi, fi, mans = sc["mi"], sc["fi"], sc["manifests"]
    keys = [f"js:{v}:{sid}", f"s:{sid}"]
    for p in sc["snaps"][sid]["manifests"]:
        keys += [f"sm:{sid}:{mi[p]}", f"m:{mi[p]}"]
        keys += [k for fp, d in mans[p]["entries"] if not d for k in (f"mf:{mi[p]}:{fi[fp]}", f"f:{fi[fp]}")]
    return keys


def _lb_diff(sc, v0, v1):
    """What the commits from metadata version v0 to v1 wrote, as board keys per layer, plus the paths involved."""
    vers, snaps, mans, mi, fi = (sc[k] for k in ("versions", "snaps", "manifests", "mi", "fi"))
    old_snaps = set(vers[v0]["snaps"]) if v0 >= 0 else set()
    new_s = [sid for sid in vers[v1]["snaps"] if sid not in old_snaps]
    old_m = {p for sid in old_snaps for p in snaps[sid]["manifests"]}
    new_m = list(dict.fromkeys(p for sid in new_s for p in snaps[sid]["manifests"] if p not in old_m))
    old_f = {fp for p in old_m for fp, _ in mans[p]["entries"]}
    new_f = list(dict.fromkeys(fp for p in new_m for fp, d in mans[p]["entries"] if not d and fp not in old_f))
    s0 = vers[v0]["current"] if v0 >= 0 else None
    s1 = vers[v1]["current"]
    reused = [p for p in snaps[s1]["manifests"] if p in old_m] if s1 in snaps else []
    gone = sorted(_lb_live(sc, s0) - _lb_live(sc, s1), key=lambda fp: fi[fp])
    keys = dict(
        f=[f"f:{fi[fp]}" for fp in new_f],
        m=[k for p in new_m for k in [f"m:{mi[p]}"] + [f"mf:{mi[p]}:{fi[fp]}" for fp, _ in mans[p]["entries"]]],
        s=[k for sid in new_s for k in [f"s:{sid}"] + [f"sm:{sid}:{mi[p]}" for p in snaps[sid]["manifests"]]],
        j=[f"j:{i}" for i in range(v0 + 1, v1 + 1)] + ([f"js:{v1}:{s1}"] if s1 in snaps else []),
        reuse=[f"m:{mi[p]}" for p in reused], gone=[f"f:{fi[fp]}" for fp in gone])
    return dict(keys=keys, new_f=new_f, new_m=new_m, new_s=new_s, reused=reused, gone=gone, s0=s0, s1=s1)


def _lb_version(sc, path):
    if path is None:
        return -1
    names = [v["name"] for v in sc["versions"]]
    return names.index(Path(str(path)).name)


_LB_KEY = ("<b>glow + tag</b> = what this step draws or reads &nbsp;·&nbsp; faded = older, reachable only by time "
           "travel &nbsp;·&nbsp; red dashes = a manifest entry that marks a file deleted")


def iceberg_commit(before, after, title=None, speed_ms=2800):
    """Replay one Iceberg commit on a lightboard, bottom-up: data files, manifests, manifest list (the snapshot), a
    new metadata file, and the atomic swap of the catalog pointer.

    `before` and `after` are `tbl.metadata_location` before and after the write (`before=None`: the table did not
    exist). Several metadata files between them (e.g. create, then set the partition spec) are shown as one step.
    """
    import html

    sc = _lb_scene(after)
    vb, va = _lb_version(sc, before), len(sc["versions"]) - 1
    d = _lb_diff(sc, vb, va)
    K, fi, files, snaps = d["keys"], sc["fi"], sc["files"], sc["snaps"]
    w, h, inner = _lb_board(sc)
    old = f"<code>{_lb_jname(sc, vb)}</code>" if vb >= 0 else None
    new = f"<code>{_lb_jname(sc, va)}</code>"
    steps, num = [], iter(range(1, 10))
    st = _lb_view(sc, vb)
    if vb < 0:
        cap = f"<b>Before.</b> The catalog has no entry for <code>{sc['name']}</code> yet."
    else:
        live = _lb_live(sc, d["s0"])
        cap = (f"<b>Before.</b> The catalog points to {old}. Readers see {len(live)} data files "
               f"({_lb_rows(sc, live):,} rows)." if live else
               f"<b>Before.</b> The catalog points to {old}: a table with a schema and no data yet.")
    steps.append(dict(state=st, caption=cap))
    if d["new_f"]:
        st = _lb_mark(st, K["f"], "new")
        part = Path(d["new_f"][0]).parent.name.split("=")[0]
        listing = ", ".join(f"{files[fp]['label']} ({files[fp]['data'].record_count:,})" for fp in d["new_f"])
        steps.append(dict(state=st, caption=(
            f"<b>{next(num)} · Write the data files.</b> {len(d['new_f'])} Parquet file(s), one per "
            f"<code>{html.escape(part)}</code> partition: {listing} rows. Nothing points to them yet, so every "
            f"reader still sees {old or 'no table'}. If the job died now, these files would just be ignored.")))
    if d["new_m"]:
        st = _lb_mark(st, K["m"], "new")
        roles = []
        for p in d["new_m"]:
            mf = sc["manifests"][p]["file"]
            what = [f"lists {mf.added_files_count} new file(s)" if mf.added_files_count else "",
                    f"re-lists {mf.existing_files_count} unchanged file(s)" if mf.existing_files_count else "",
                    f"marks {mf.deleted_files_count} file(s) deleted (red dashes)" if mf.deleted_files_count else ""]
            roles.append(f"<code>{_lb_mname(p)}</code> {', '.join(x for x in what if x)}")
        steps.append(dict(state=st, caption=(
            f"<b>{next(num)} · Write {len(d['new_m'])} manifest(s).</b> A manifest is a small Avro file listing data "
            "files, each with its partition value, row count and min/max per column. " + "; ".join(roles) + ".")))
    if d["new_s"]:
        st = _lb_mark(_lb_mark(_lb_mark(st, K["s"], "new"), K["reuse"], "reuse"), K["gone"], "gone")
        s1 = snaps[d["s1"]]
        reuse = (f" and reuses {len(d['reused'])} manifest(s) from earlier snapshots as they are (tag <i>reused</i>): "
                 "an old manifest is never edited" if d["reused"] else "")
        gone = (f" The old {', '.join(files[fp]['label'] for fp in d['gone'])} file is no longer in this version, but "
                "it stays on disk for older snapshots." if d["gone"] else "")
        steps.append(dict(state=st, caption=(
            f"<b>{next(num)} · Write a manifest list: snapshot S{s1['n']} ({s1['snap'].summary.operation.value}).</b> "
            f"It names the {len(s1['manifests'])} manifest(s) of this version: "
            f"{len(s1['manifests']) - len(d['reused'])} new{reuse}.{gone}")))
    if not (d["new_f"] or d["new_m"] or d["new_s"]):
        steps.append(dict(state=st, caption=(
            "<b>No data files, no manifests, no snapshot.</b> Nothing in the data layers changes: this commit only "
            "changes the table's definition.")))
    st = _lb_mark(st, K["j"], "new")
    v1 = sc["versions"][va]
    changes = []
    if d["new_s"]:
        changes.append(f"the snapshot list, now with S{snaps[d['s1']]['n']} as current")
    if vb < 0 or sc["versions"][vb]["schema"] != v1["schema"]:
        changes.append(f"schema {v1['schema']}")
    if vb < 0 or sc["versions"][vb]["spec"] != v1["spec"]:
        changes.append(f"partition spec {v1['spec']}")
    many = va - vb > 1
    steps.append(dict(state=st, caption=(
        f"<b>{next(num)} · Write {'new metadata files' if many else 'a new metadata file'}</b> "
        + (", then ".join(f"<code>{_lb_jname(sc, i)}</code>" for i in range(vb + 1, va + 1)) if many else new)
        + f": JSON with the table's schema, partition spec and snapshots. What changed: {', '.join(changes)}. "
        f"The catalog still says {old or 'nothing'}.")))
    st = _lb_mark(_lb_view(sc, va), K["f"] + K["m"] + K["s"] + K["j"] + [f"cj:{va}"], "new")
    st = _lb_mark(_lb_mark(st, K["reuse"], "reuse"), K["gone"], "gone")
    after_swap = (f"A reader that started before still reads {old}; every reader after this instant sees {new}. "
                  "No reader can see half of a commit." if old else
                  f"From this instant the table exists: every reader that asks the catalog gets {new}.")
    steps.append(dict(state=st, caption=(
        f"<b>{next(num)} · Commit: swap the catalog pointer</b> {old or '(none)'} → {new} in one atomic step. "
        + after_swap)))
    return _lb_player(title or "One commit, bottom-up", w, h, inner, steps, key=_LB_KEY, speed_ms=speed_ms)


def iceberg_read(tbl, row_filter, speed_ms=2800):
    """Replay how a reader plans `tbl.scan(row_filter=...)`, top-down: catalog, metadata file, manifest list,
    manifests, and finally the few data files worth opening.

    The decisions are PyIceberg's own: its manifest evaluator (partition ranges in the manifest list), partition
    evaluator and min/max evaluator, and the files `plan_files()` returns.
    """
    import html
    import re

    from pyiceberg.conversions import from_bytes

    sc = _lb_scene(tbl.metadata_location)
    va = len(sc["versions"]) - 1
    sid = sc["versions"][va]["current"]
    mans, files, mi, fi = sc["manifests"], sc["files"], sc["mi"], sc["fi"]
    scan = tbl.scan(row_filter=row_filter)
    planned = {t.file.file_path for t in scan.plan_files()}
    try:
        planner = scan._manifest_planner
        metrics = planner._build_metrics_evaluator()

        def man_ok(mf):
            return planner._build_manifest_evaluator(mf.partition_spec_id)(mf)

        def file_ok(mf, df):
            if not planner._build_partition_evaluator(mf.partition_spec_id)(df):
                return "partition can't match"
            return "read" if metrics(df) else "min/max can't match"
    except AttributeError:      # another PyIceberg version: fall back to the planned files only
        def man_ok(mf):
            return any(fp in planned for fp, _ in mans[mf.manifest_path]["entries"])

        def file_ok(mf, df):
            return "read" if df.file_path in planned else "ruled out"

    schema = sc["table"].metadata.schema()
    col = next((f for f in schema.fields if re.search(rf"\b{f.name}\b", row_filter)), None)

    def value(t, b):
        if b is None:
            return "?"
        v = from_bytes(t, b)
        kind = str(t)
        if kind.startswith("timestamp"):
            return _fmt(pd.Timestamp(v, unit="us"))
        return _fmt(pd.Timestamp(v, unit="D")) if kind == "date" else _fmt(v)

    spec = sc["table"].metadata.spec()
    ptype = spec.partition_type(schema)
    kept, man_rows, file_rows = [], [], []
    for p in sc["snaps"][sid]["manifests"]:
        mf = mans[p]["file"]
        ok = man_ok(mf)
        live = [fp for fp, dl in mans[p]["entries"] if not dl]
        if not live:
            continue
        rng = "?"
        if mf.partitions and ptype.fields:
            ps = mf.partitions[0]
            pt = ptype.fields[0].field_type
            rng = f"{value(pt, ps.lower_bound)} … {value(pt, ps.upper_bound)}"
        man_rows.append((_lb_mname(p), rng, len(live), "open" if ok else "skip"))
        if ok:
            kept.append(p)
            for fp in live:
                df = files[fp]["data"]
                lo = value(col.field_type, (df.lower_bounds or {}).get(col.field_id)) if col else "?"
                hi = value(col.field_type, (df.upper_bounds or {}).get(col.field_id)) if col else "?"
                file_rows.append((fp, lo, hi, file_ok(mf, df)))
    read = {fp for fp, *_, verdict in file_rows if verdict == "read"}
    if read != planned:                       # trust the engine's plan if our replay disagrees
        file_rows = [(fp, lo, hi, "read" if fp in planned else v if v != "read" else "ruled out")
                     for fp, lo, hi, v in file_rows]
        read = planned
    file_rows.sort(key=lambda r: fi[r[0]])
    w, h, inner = _lb_board(sc)
    base = _lb_view(sc, va)
    q = f"<code>{html.escape(row_filter)}</code>"
    n_live = len(_lb_live(sc, sid))
    steps = [dict(state=base, caption=f"A reader asks for {q}. It has not opened a single file yet.")]
    st = _lb_mark(base, ["c", f"cj:{va}", f"j:{va}"], "cur")
    steps.append(dict(state=st, caption=(
        f"<b>1 · Ask the catalog</b> for <code>{sc['name']}</code>: one lookup, one answer, the path of the current "
        f"metadata file <code>{_lb_jname(sc, va)}</code>.")))
    st = _lb_mark(st, [f"js:{va}:{sid}", f"s:{sid}"], "cur")
    steps.append(dict(state=st, caption=(
        f"<b>2 · Read the metadata file:</b> schema, partition spec, {len(sc['versions'][va]['snaps'])} snapshot(s). "
        f"Take the current one, S{sc['snaps'][sid]['n']}, and open its manifest list. The faded snapshots are "
        "ignored.")))
    skipped_m = [p for p in sc["snaps"][sid]["manifests"]
                 if p not in kept and any(not dl for _, dl in mans[p]["entries"])]
    st = _lb_mark(st, [k for p in kept for k in (f"sm:{sid}:{mi[p]}", f"m:{mi[p]}")], "cur")
    st = _lb_mark(st, [f"m:{mi[p]}" for p in skipped_m], "skip")
    # what sits under a skipped manifest is never looked at
    st.update({k: "dim" for p in skipped_m for fp, _ in mans[p]["entries"]
               for k in (f"sm:{sid}:{mi[p]}", f"mf:{mi[p]}:{fi[fp]}", f"f:{fi[fp]}")})
    part_name = ptype.fields[0].name if ptype.fields else "partition"
    table = ('<table class="lb-t"><tr><th>manifest</th><th>' + html.escape(part_name) + ' range</th><th>files</th>'
             '<th></th></tr>' + "".join(f"<tr><td>{a}</td><td>{b}</td><td>{c}</td><td>{d}</td></tr>"
                                        for a, b, c, d in man_rows) + "</table>")
    steps.append(dict(state=st, caption=(
        f"<b>3 · The manifest list</b> stores each manifest's partition range. {len(kept)} of {len(man_rows)} "
        f"manifest(s) can hold matching rows; the others are skipped without being opened.{table}")))
    skip_f = [f"f:{fi[fp]}" for fp, *_, v in file_rows if v != "read"]
    st = _lb_mark(st, [f"mf:{mi[p]}:{fi[fp]}" for p in kept for fp, dl in mans[p]["entries"] if not dl], "cur")
    st = _lb_mark(st, skip_f, "skip")
    col_name = html.escape(col.name) if col else "column"
    table = (f'<table class="lb-t"><tr><th>file</th><th>min {col_name}</th><th>max {col_name}</th><th></th></tr>'
             + "".join(f"<tr><td>{files[fp]['label']}</td><td>{lo}</td><td>{hi}</td><td>{v}</td></tr>"
                       for fp, lo, hi, v in file_rows) + "</table>")
    steps.append(dict(state=st, caption=(
        "<b>4 · Open the manifest(s)</b> (small Avro files). Each lists its data files with partition value and "
        f"min/max per column; the reader checks them against the filter.{table}")))
    st = _lb_mark(st, [f"f:{fi[fp]}" for fp in read], "cur", "read")
    steps.append(dict(state=st, caption=(
        f"<b>5 · Only now open Parquet:</b> {len(read)} of {n_live} data files. Every other file was ruled out from "
        "metadata alone, and the query never mentioned the partition column.")))
    return _lb_player("A reader follows the pointers down", w, h, inner, steps, key=_LB_KEY, speed_ms=speed_ms)


def iceberg_history(tbl, notes=None, speed_ms=2800):
    """Step through every snapshot the current metadata file still lists, highlighting what a reader of that
    snapshot would follow. `notes` maps snapshot ids to an extra sentence (HTML) for that step."""
    sc = _lb_scene(tbl.metadata_location)
    va = len(sc["versions"]) - 1
    cur = sc["versions"][va]["current"]
    w, h, inner = _lb_board(sc)
    notes = notes or {}
    steps = [dict(state=_lb_view(sc, va), caption=(
        f"The current metadata file <code>{_lb_jname(sc, va)}</code> lists {len(sc['snaps'])} snapshots. "
        "Time travel = start from an older one instead of the current one."))]
    for sid, s in sc["snaps"].items():
        props = s["snap"].summary.additional_properties
        change = ", ".join(f"{sign}{int(props[k]):,} {what}" for k, sign, what in [
            ("added-data-files", "+", "file(s)"), ("deleted-data-files", "−", "file(s)"),
            ("added-records", "+", "rows"), ("deleted-records", "−", "rows")] if props.get(k, "0") != "0")
        live = _lb_live(sc, sid)
        st = _lb_mark(_lb_view(sc, va, s=sid), _lb_tree(sc, va, sid), "cur")
        steps.append(dict(state=st, caption=(
            f"<b>S{s['n']} · {s['snap'].summary.operation.value}</b>{' (current)' if sid == cur else ''}: {change}. "
            f"A reader of S{s['n']} follows these lines: {len(live)} files, {_lb_rows(sc, live):,} rows. "
            + notes.get(sid, ""))))
    return _lb_player("Every snapshot is a version you can read", w, h, inner, steps, key=_LB_KEY, speed_ms=speed_ms)


def iceberg_race(base, after_a, after_b, speed_ms=3000):
    """Two writers that both loaded metadata `base`: A commits `after_a`, B's first attempt is refused, B retries
    on top of A and commits `after_b`. Replayed on the board, step by step."""
    sc = _lb_scene(after_b)
    v0, va, vb = _lb_version(sc, base), _lb_version(sc, after_a), len(sc["versions"]) - 1
    A, B = _lb_diff(sc, v0, va)["keys"], _lb_diff(sc, va, vb)["keys"]
    engines = [("writer A", "PyIceberg", "catalog"), ("writer B", "PyIceberg", "catalog")]
    w, h, inner = _lb_board(sc, engines)
    X = ["x:0", "x:1", "xc:0", "xc:1"]
    j0, ja, jb = (f"<code>{_lb_jname(sc, v)}</code>" for v in (v0, va, vb))
    sa, sb = (f"S{sc['snaps'][sc['versions'][v]['current']]['n']}" for v in (va, vb))
    s0 = f"S{sc['snaps'][sc['versions'][v0]['current']]['n']}"
    fa = ", ".join(sc["files"][fp]["label"] for fp in _lb_diff(sc, v0, va)["new_f"])
    fb = ", ".join(sc["files"][fp]["label"] for fp in _lb_diff(sc, va, vb)["new_f"])
    steps = []
    st = _lb_mark(_lb_view(sc, v0), X)
    steps.append(dict(state=st, caption=(
        f"Writers A and B load <code>{sc['name']}</code> at the same moment. The catalog tells both: {j0}. "
        f"Both plan to build on {s0}.")))
    st = _lb_mark(_lb_mark(st, A["f"], "wa", "new"), B["f"], "wb", "new")
    steps.append(dict(state=st, caption=(
        f"<b>Both write their Parquet files at once:</b> A writes {fa}, B writes {fb}. No lock: each writer "
        "gives its files unique names, so data files never collide.")))
    st = _lb_mark(st, A["m"] + A["s"] + A["j"], "wa", "new")
    steps.append(dict(state=st, caption=(
        f"A writes its manifest, its manifest list {sa} (parent {s0}) and a new metadata file {ja}.")))
    st = _lb_mark(_lb_mark(_lb_view(sc, va), X), A["f"] + A["m"] + A["s"] + A["j"], "wa")
    st = _lb_mark(_lb_mark(_lb_mark(st, B["f"], "wb"), [f"cj:{va}"], "new"), ["x:0"], "ok")
    steps.append(dict(state=st, caption=(
        f"<b>A commits:</b> \"swap {j0} → {ja}, if the catalog still says {j0}\". It does, so the swap succeeds ✓. "
        f"B's file is on disk but not in any snapshot.")))
    st = _lb_mark(st, ["x:1"], "bad")
    steps.append(dict(state=st, caption=(
        f"<b>B asks for the same swap</b> from {j0}. The catalog now says {ja}, so the swap is refused ✗: that is "
        "the \"Commit failed due to a concurrent update\" line. Nothing was overwritten and nobody waited.")))
    st = _lb_mark({**st, "x:1": "on"}, B["m"] + B["s"] + B["j"], "wb", "new")
    steps.append(dict(state=st, caption=(
        f"<b>B retries:</b> re-reads {ja}, checks that A only added files (no conflict with its own append), and "
        f"writes a manifest list {sb} whose parent is {sa}: A's manifests plus its own. B keeps its Parquet file.")))
    st = _lb_mark(_lb_mark(_lb_view(sc, vb), X, "ok"), A["f"] + A["m"] + A["s"] + A["j"], "wa")
    st = _lb_mark(_lb_mark(st, B["f"] + B["m"] + B["s"] + B["j"], "wb"), [f"cj:{vb}"], "new")
    steps.append(dict(state=st, caption=(
        f"<b>B commits</b> {ja} → {jb} ✓. Both appends are in the table, one after the other. Only the pointer "
        "swap had to be serialized, and the catalog does that with one compare-and-swap.")))
    return _lb_player("Two writers, one pointer", w, h, inner, steps, key=_LB_KEY, speed_ms=speed_ms)


def iceberg_engines(location, engines, speed_ms=3000):
    """The current table read by several engines. `engines` is a list of (name, note, via), via "catalog" or
    "metadata" (the engine was handed the metadata file path)."""
    sc = _lb_scene(location)
    va = len(sc["versions"]) - 1
    sid = sc["versions"][va]["current"]
    w, h, inner = _lb_board(sc, engines)
    base = _lb_mark(_lb_view(sc, va), [f"x:{k}" for k in range(len(engines))])
    base.update({f"xc:{k}": "hid" for k in range(len(engines))})
    base.update({f"xj:{k}:{i}": "hid" for k in range(len(engines)) for i in range(len(sc["versions"]))})
    tree = ["c", f"cj:{va}", f"j:{va}"] + _lb_tree(sc, va, sid)
    steps = [dict(state=base, caption=(
        f"{len(engines)} engines, one copy of the data. Neither owns <code>{sc['name']}</code>: the files and the "
        "pointers are the table."))]
    for k, (name, note, via) in enumerate(engines):
        path = [f"x:{k}"] + ([f"xc:{k}"] + tree if via == "catalog" else [f"xj:{k}:{va}"] + tree[2:])
        how = ("asks the catalog for the current metadata file, then follows the same pointers down" if via == "catalog"
               else f"was handed the metadata file path (<code>{_lb_jname(sc, va)}</code>) and follows the same "
                    "pointers down. With a REST catalog it would ask the catalog, like any other engine")
        steps.append(dict(state=_lb_mark(base, path, "cur"), caption=(
            f"<b>{name}</b> ({note}) {how}: snapshot S{sc['snaps'][sid]['n']}, "
            f"{len(_lb_live(sc, sid))} data files.")))
    return _lb_player("One table, many engines", w, h, inner, steps, key=_LB_KEY, speed_ms=speed_ms)


def lakehouse_history(speed_ms=3200):
    """How we got to table formats, drawn in six steps: database, data lake, Hive tables, their limits, Iceberg's
    metadata layer, and the layers it adds. A concept board: no data."""
    C = _LB
    out = []

    def n(*args, **kwargs):
        out.append(_lb_node(*args, **kwargs))

    out.append(_lb_text(30, 40, "a database", k="db-t"))
    n("db", 30, 56, 220, 264, C["CHALK"], "", cls="frame")
    n("db-e", 50, 90, 180, 56, C["JSON"], "SQL engine", "parse · plan · run", title_cls="big")
    n("db-s", 50, 226, 180, 56, C["FILE"], "storage", "its own format", title_cls="big")
    out.append(_lb_edge("db-l", (140, 146), (140, 226), C["CHALK"]))
    out.append(_lb_text(140, 196, "knows every table", cls="sub", anchor="middle", k="db-n"))
    engines = ["Spark", "Trino", "Flink", "DuckDB"]
    for i, e in enumerate(engines):
        n(f"en:{i}", 330 + i * 160, 30, 130, 40, C["JSON"], e, title_cls="big")
    n("band", 300, 300, 690, 50, C["SOFT"], "object storage: HDFS, then S3 · ADLS · GCS")
    for i in range(12):
        x = 322 + i * 54 + (24 if i >= 6 else 0)
        n(f"pf:{i}", x, 228, 38, 48, C["FILE"], "", file=True)
    n("pf:bad", 322 + 5 * 54, 228, 38, 48, C["RED"], "", file=True, cls="ghost")
    for i in range(4):
        out.append(_lb_edge(f"en-f:{i}", (395 + i * 160, 70), (480 + i * 120, 226), C["JSON"]))
    out.append(_lb_text(318, 220, "payments/", cls="sub", k="dir-t:0"))
    out.append(_lb_text(666, 220, "orders/", cls="sub", k="dir-t:1"))
    n("dir:0", 312, 206, 330, 80, C["SOFT"], "", cls="ghost frame")
    n("dir:1", 660, 206, 330, 80, C["SOFT"], "", cls="ghost frame")
    n("hms", 330, 108, 290, 52, C["SNAP"], "Hive metastore", "payments → s3://lake/payments/", title_cls="big")
    out.append(_lb_edge("hms-l", (475, 160), (477, 206), C["SNAP"]))
    for i, (x, y, t) in enumerate([(312, 382, "1 · list the folder to find the files"),
                                   (312, 404, "2 · readers see a half-finished load"),
                                   (660, 382, "3 · change 1 row → rewrite whole files"),
                                   (660, 404, "4 · columns matched by name")]):
        out.append(_lb_text(x, y, t, cls="sub bad-t", k=f"pr:{i}"))
    n("ice", 300, 100, 690, 96, C["MAN"], "")
    out.append(_lb_text(312, 122, "Iceberg metadata: exactly which files are in each version", cls="sub", k="ice-t"))
    for i, (t, col) in enumerate([("catalog", C["CHALK"]), ("metadata.json", C["JSON"]), ("manifest list", C["SNAP"]),
                                  ("manifests", C["MAN"])]):
        n(f"lay:{i}", 318 + i * 162, 136, 142, 44, col, t, title_cls="big")
        if i:
            out.append(_lb_edge(f"lay-l:{i}", (318 + i * 162 - 20, 158), (318 + i * 162, 158), col, bend="h"))
    for i in range(4):
        out.append(_lb_edge(f"en-i:{i}", (395 + i * 160, 70), (395 + i * 160, 100), C["JSON"]))
    for i in range(6):
        out.append(_lb_edge(f"ice-f:{i}", (888, 180), (341 + i * 2 * 54 + (24 if i >= 3 else 0), 228), C["MAN"]))

    pf = [f"pf:{i}" for i in range(12)]
    en = [f"en:{i}" for i in range(4)]
    lake = en + pf + ["band"] + [f"en-f:{i}" for i in range(4)]
    hive = ["dir:0", "dir:1", "dir-t:0", "dir-t:1", "hms", "hms-l"]
    probs = [f"pr:{i}" for i in range(4)] + ["pf:bad"]
    ice = ["ice", "ice-t"] + [f"en-i:{i}" for i in range(4)]
    layers = [f"lay:{i}" for i in range(4)] + [f"lay-l:{i}" for i in range(1, 4)] + [f"ice-f:{i}" for i in range(6)]
    db = ["db", "db-t", "db-e", "db-s", "db-l", "db-n"]
    keys = db + lake + hive + probs + ice + layers
    hidden = {k: "hid" for k in keys}

    def at(show, dim=(), **extra):
        st = dict(hidden)
        st.update({k: "on" for k in show})
        st.update({k: "dim" for k in dim})
        for cls, ks in extra.items():
            st = _lb_mark(st, ks, cls)
        return st

    steps = [
        dict(state=at(db, new=["db-e", "db-s"]), caption=(
            "<b>A database</b> keeps compute and storage in one box. Because it owns the storage, it knows what a table "
            "is: which rows, which version, who is writing right now.")),
        dict(state=at(lake, dim=db, new=en + ["band"] + pf), caption=(
            "<b>The data lake</b> pulls them apart. Cheap object storage holds open files (Parquet); many engines come "
            "to the files. Scale and openness, but the files alone don't say which of them form a table.")),
        dict(state=at(lake + hive, dim=db, new=hive), caption=(
            "<b>Hive's answer:</b> a table is a directory, a partition is a sub-directory, and a metastore remembers "
            "where the directory is. It worked for a decade of batch jobs.")),
        dict(state=at(lake + hive + probs, dim=db, bad=["pf:bad"]), caption=(
            "<b>But a directory is a weak table.</b> Readers list folders to find files (slow on S3), a load that dies "
            "halfway leaves its files behind and readers take them as complete, one changed row means rewriting "
            "files, and columns are matched by name. We will see the middle two in a minute.")),
        dict(state=at(lake + ice, dim=db + [f"en-f:{i}" for i in range(4)], new=ice), caption=(
            "<b>Iceberg</b> (Netflix 2017, an Apache top-level project since 2020) keeps the files and adds a metadata "
            "layer: the table is an explicit list of files per version, not whatever sits in a folder. Engines read "
            "that list instead of listing directories.")),
        dict(state=at(lake + ice + layers, dim=db + [f"en-f:{i}" for i in range(4)], new=layers), caption=(
            "<b>The layers</b>, from the top: a catalog pointer → a metadata file → a manifest list per snapshot → "
            "manifests → data files. The rest of this demo builds exactly this, from real files on disk.")),
    ]
    return _lb_player("How we got to table formats", 1010, 424, "".join(out), steps, speed_ms=speed_ms)


def plain_folder(folder, loads, rewrite, speed_ms=3000):
    """A folder of Parquet files used as a table: two loads (the second died halfway), a reader that globs the
    folder, and the rewrite one changed row costs. `loads` lists the file stems each load meant to write (e.g.
    "day=05"); what was really written is read from `folder`."""
    import pyarrow.parquet as pq

    C = _LB
    folder = Path(folder)
    present = {p.stem: pq.read_metadata(p).num_rows for p in folder.glob("*.parquet")}
    expected = [s for load in loads for s in load]
    out, pitch, x0 = [], 112, 52
    out.append(_lb_text(36, 106, f"{folder.name}/", cls="hand"))
    right = x0 + (len(expected) - 1) * pitch + 94 + 22
    out.append(_lb_node("dir", 30, 116, right - 30, 92, C["SOFT"], "", cls="ghost frame"))
    xs = {s: x0 + i * pitch for i, s in enumerate(expected)}
    for s in expected:
        if s in present:
            out.append(_lb_node(f"f:{s}", xs[s], 134, 94, 56, C["FILE"], s, f"{present[s]:,} rows", file=True,
                                tags=("new", "why"), tag_at="below"))
        else:
            out.append(_lb_node(f"f:{s}", xs[s], 134, 94, 56, C["RED"], s, "never written", file=True, cls="ghost",
                                tags=("bad",), tag_at="below"))
    rx = xs[rewrite]
    out.append(_lb_node("f:new", rx, 246, 94, 56, C["FILE"], rewrite, "rewritten", file=True, tags=("why",),
                        tag_at="below"))
    w = right + 20
    reader = (w / 2 - 210, 18, 420, 44)
    for s in expected:
        if s in present:
            out.insert(0, _lb_edge(f"r:{s}", (w / 2, 62), (xs[s] + 47, 134), C["CHALK"]))
    out.insert(0, _lb_edge("r:new", (w / 2, 62), (rx + 47, 246), C["CHALK"]))
    out.append(_lb_node("reader", *reader, C["JSON"], "dashboard", f"SELECT … FROM '{folder.name}/*.parquet'",
                        title_cls="big"))
    first = [f"f:{s}" for s in loads[0] if s in present]
    second = [f"f:{s}" for s in loads[1] if s in present]
    missing = [f"f:{s}" for s in expected if s not in present]
    edges = [f"r:{s}" for s in expected if s in present]
    keys = [f"f:{s}" for s in expected] + edges + ["reader", "r:new", "f:new", "dir"]
    base = {k: "hid" for k in keys}
    base["dir"] = "on"

    def at(prev, show=(), **extra):
        st = _lb_mark(prev, show)
        for cls, ks in extra.items():
            st = _lb_mark(st, ks, cls)
        return st

    names = lambda ks: ", ".join(k[2:] for k in ks)    # noqa: E731
    s0 = at(base, first, new=first)
    s1 = at({**s0, **{k: "on" for k in first}}, second + missing, new=second, bad=missing)
    s2 = at({**s1, **{k: "on" for k in second}}, ["reader"] + edges, cur=edges)
    s2.update({k: "dim" for k in missing})
    s3 = at({**s2, **{k: "on" for k in edges}}, cur=[f"f:{rewrite}"])
    s4 = at(s3, ["f:new", "r:new"], new=["f:new"], why=[f"f:{rewrite}", "f:new"], bad=["r:new", f"r:{rewrite}"])
    steps = [
        dict(state=s0, caption=(f"<b>Load 1</b> writes {names(first)}. Here a table is simply whatever files are in "
                                f"the folder.")),
        dict(state=s1, caption=(f"<b>Load 2</b> should write {names(second + missing)}. It writes {names(second)} "
                                f"and dies 💥. The red slots exist only in this drawing: nothing on disk says they "
                                "were expected.")),
        dict(state=s2, caption=(f"A <b>dashboard</b> globs the folder and reads {len(edges)} files. No error, no "
                                "warning: a confident answer with two days missing.")),
        dict(state=s3, caption=(f"Finance wants <b>one</b> payment marked refunded. A Parquet file can't be edited in "
                                f"place, so the smallest change is a new {rewrite} file: {present[rewrite]:,} rows "
                                "rewritten to change 1.")),
        dict(state=s4, caption=("<b>While the new file replaces the old one</b>, a reader listing the folder can get "
                                "the old file, the new one, both (double count) or neither (a missing day). Nothing "
                                "makes the swap atomic.")),
    ]
    return _lb_player("A folder of Parquet files is not a table", w, 330, "".join(out), steps, speed_ms=speed_ms)


def iceberg_rename(before, after, speed_ms=3000):
    """Why a column rename in Iceberg rewrites no data: Parquet files store a field id per column, the schema maps
    ids to names, and readers match by id. `before`/`after` are metadata locations around the rename."""
    import html

    import pyarrow.parquet as pq
    from pyiceberg.table import StaticTable

    C = _LB
    old, new = (StaticTable.from_metadata(str(p)).metadata for p in (before, after))
    s_old, s_new = old.schema(), new.schema()
    renamed = [f.field_id for f in s_new.fields if f.field_id in {g.field_id for g in s_old.fields}
               and s_old.find_field(f.field_id).name != f.name]
    fid = renamed[0]
    sc = _lb_scene(after)
    live = sorted(_lb_live(sc, sc["versions"][-1]["current"]), key=lambda fp: sc["fi"][fp])
    path = live[0].removeprefix("file://")
    pf_schema = pq.read_schema(path)
    cols = [(int(f.metadata[b"PARQUET:field_id"]), f.name) for f in pf_schema if f.metadata]
    in_file = dict(cols)
    sample = [str(v) for v in pq.read_table(path, columns=[in_file[fid]]).column(0).to_pylist()[:3]]
    label = sc["files"][live[0]]["label"]

    out, y0, rh = [], 92, 27
    panels = {"pf": (30, f"Parquet file {label}", C["FILE"]),
              "ps0": (380, f"schema in {Path(str(before)).name[:10]}…", C["JSON"]),
              "ps1": (380, f"schema in {Path(str(after)).name[:10]}…", C["JSON"])}
    ph = 34 + len(cols) * rh + 10
    for k, (x, t, col) in panels.items():
        out.append(_lb_node(k, x, 50, 270, ph, col, "", file=(k == "pf")))
        out.append(_lb_text(x + 12, 72, t, cls="sub", k=k + "-t"))
    for i, (cid, name) in enumerate(cols):
        y = y0 + i * rh
        out.append(_lb_node(f"fr:{cid}", 42, y - 4, 246, 24, C["FILE"], f"id {cid}  {name}", cls="row"))
        for k, sch in (("ps0", s_old), ("ps1", s_new)):
            j = [f.field_id for f in sch.fields].index(cid)
            out.append(_lb_node(f"{k}r:{cid}", 392, y0 + j * rh - 4, 246, 24, C["JSON"],
                                f"id {cid}  {sch.find_field(cid).name}", cls="row"))
        j = [f.field_id for f in s_new.fields].index(cid)
        out.insert(0, _lb_edge(f"ln:{cid}", (288, y + 8), (392, y0 + j * rh + 8), C["CHALK"], bend="h"))
    new_name, old_name = s_new.find_field(fid).name, s_old.find_field(fid).name
    out.append(_lb_node("rd-id", 730, 50, 250, 150, C["OK"], "", tags=("ok",)))
    out.append(_lb_text(744, 74, "reader of the new schema, by id", cls="sub", k="rd-id-t"))
    out.append(_lb_text(744, 100, f"{new_name} → id {fid}", cls="", k="rd-id-1"))
    for i, v in enumerate(sample):
        out.append(_lb_text(744, 126 + i * 20, v, cls="ok-t", k=f"rd-id-v:{i}"))
    out.append(_lb_node("rd-nm", 730, 50, 250, 150, C["RED"], "", tags=("bad",)))    # same place: never both
    out.append(_lb_text(744, 74, "reader matching by name", cls="sub", k="rd-nm-t"))
    out.append(_lb_text(744, 100, f"'{new_name}' in file? no", cls="", k="rd-nm-1"))
    for i in range(len(sample)):
        out.append(_lb_text(744, 126 + i * 20, "NULL", cls="bad-t", k=f"rd-nm-v:{i}"))
    h = max(50 + ph, 200) + 14

    ids = [c for c, _ in cols]
    rd_id = ["rd-id", "rd-id-t", "rd-id-1"] + [f"rd-id-v:{i}" for i in range(len(sample))]
    rd_nm = ["rd-nm", "rd-nm-t", "rd-nm-1"] + [f"rd-nm-v:{i}" for i in range(len(sample))]
    file_keys = ["pf", "pf-t"] + [f"fr:{c}" for c in ids]
    old_keys = ["ps0", "ps0-t"] + [f"ps0r:{c}" for c in ids]
    new_keys = ["ps1", "ps1-t"] + [f"ps1r:{c}" for c in ids]
    lines = [f"ln:{c}" for c in ids]
    base = {k: "hid" for k in file_keys + old_keys + new_keys + lines + rd_id + rd_nm}
    s0 = _lb_mark(base, file_keys)
    s1 = _lb_mark(s0, old_keys + lines)
    s2 = _lb_mark({**s1, **{k: "hid" for k in old_keys}}, new_keys)
    s2 = _lb_mark(s2, ["ps1", f"ps1r:{fid}"], "new")
    s3 = _lb_mark(s2, rd_id + [f"ln:{fid}", f"fr:{fid}"], "cur")
    s3 = _lb_mark(s3, ["rd-id"], "ok")
    s4 = _lb_mark({**s3, **{k: "hid" for k in rd_id + lines}}, rd_nm)
    s4 = _lb_mark({**s4, f"fr:{fid}": "on"}, ["rd-nm", f"fr:{fid}"], "bad")
    q = lambda s: f"<code>{html.escape(s)}</code>"    # noqa: E731
    steps = [
        dict(state=s0, caption=(f"Inside every Parquet file Iceberg writes, each column carries a <b>field id</b>. "
                                f"Here the {label} file: column {fid} is called {q(old_name)}.")),
        dict(state=s1, caption=(f"The table schema lives in the metadata file and maps ids to names: id {fid} is "
                                f"{q(old_name)}. A reader matches the file's columns to the schema <b>by id</b>.")),
        dict(state=s2, caption=(f"<b>rename_column</b> writes one thing: a new metadata file whose schema calls id "
                                f"{fid} {q(new_name)}. The Parquet file is not touched: it still says {q(old_name)} "
                                "inside.")),
        dict(state=s3, caption=(f"A reader of the new schema wants {q(new_name)} → id {fid} → the file's column "
                                f"{fid}. Old files read correctly: {', '.join(map(html.escape, sample))}, …")),
        dict(state=s4, caption=(f"A reader that matches <b>by name</b> looks for {q(new_name)} in the file, finds no "
                                "such column, and returns NULL for every row written before the rename. That is what "
                                "happens to a plain folder of Parquet.")),
    ]
    return _lb_player("Rename a column: a metadata-only change", 1000, h, "".join(out), steps, speed_ms=speed_ms)


# ---------------------------------------------------------------------------
# Lab 3.3 · Streaming
# ---------------------------------------------------------------------------

@_figure
def watermark(events, allowed_lateness, window=timedelta(minutes=15), start=None, hours=1.0):
    """Event time against arrival time for delivered events, with the watermark and the windows it closes.

    `events` are the lab's dicts with `ts` (event time), `arrived`, `event_type` and `city`, in arrival order.
    `allowed_lateness` is a timedelta, or a list of them for one panel each (same hour, side by side).
    Replays the rule of `stream_windows`: watermark = max event time seen − allowed lateness; an event whose window
    ended at or before the watermark is dropped.
    """
    lateness = allowed_lateness if isinstance(allowed_lateness, (list, tuple)) else [allowed_lateness]
    minutes = window.seconds // 60

    def wstart(ts):
        return ts - timedelta(minutes=ts.minute % minutes, seconds=ts.second, microseconds=ts.microsecond)

    def replay(lag):
        rows, max_ts = [], None
        for e in events:
            if e["event_type"] != "delivered":
                continue
            max_ts = e["ts"] if max_ts is None else max(max_ts, e["ts"])
            wm = max_ts - lag
            rows.append({"arrived": e["arrived"], "ts": e["ts"], "city": e["city"], "wm": wm,
                         "dropped": wstart(e["ts"]) + window <= wm})
        return pd.DataFrame(rows)

    runs = [replay(lag) for lag in lateness]
    if start is None:   # the hour with the most drops under the first setting
        drops = runs[0][runs[0].dropped]
        start = drops.arrived.dt.floor("h").mode().iloc[0] if len(drops) else runs[0].arrived.min().floor("h")
    start = pd.Timestamp(start)
    end = start + timedelta(hours=hours)
    colors = _colors(runs[0].city)

    fig, axes = plt.subplots(1, len(runs), figsize=(6.4 * len(runs), 5.2), sharey=True, squeeze=False)
    for ax, lag, d in zip(axes[0], lateness, runs):
        d = d[(d.arrived >= start) & (d.arrived < end)]
        lo = min(d.ts.min(), start)
        w = pd.Timestamp(lo).floor(f"{minutes}min")
        k = 0
        while w < end:
            if k % 2 == 0:
                ax.axhspan(w, w + window, color=GRID, alpha=0.45, lw=0)
            if ax is axes[0, -1]:
                ax.text(end, w + window / 2, f" window {w.strftime('%H:%M')}", fontsize=7.5, color=MUTED, va="center")
            w += window
            k += 1
        ax.plot(d.arrived, d.wm, color=NAVY, lw=2, drawstyle="steps-post", label="watermark")
        ax.plot([start, end], [start, end], color=FADE, lw=1, ls=(0, (1, 2)), label="arrived = happened")
        for city, c in colors.items():
            ok = d[(d.city == city) & ~d.dropped]
            ax.scatter(ok.arrived, ok.ts, s=16, color=c, label=city, zorder=3, ec="white", lw=0.6)
        bad = d[d.dropped]
        ax.scatter(bad.arrived, bad.ts, s=60, marker="X", color=BAD, ec="white", lw=0.8, zorder=4,
                   label="dropped: window already closed")
        ax.set_xlim(start, end + (end - start) * (0.14 if ax is axes[0, -1] else 0.02))
        ax.set_ylim(lo - timedelta(minutes=2), end)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.yaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_xlabel("arrival time (processing time, UTC)")
        ax.grid(True)
        ax.set_axisbelow(True)
        ax.set_title(f"allowed lateness {int(lag.total_seconds() // 60)} min: {len(bad)} dropped")
    axes[0, 0].set_ylabel("event time (UTC)")
    axes[0, 0].legend(loc="upper left", fontsize=8.2)
    fig.suptitle(f"Delivered events arriving {start.strftime('%d %b %H:%M')}–{end.strftime('%H:%M')} UTC: a window is "
                 f"final once the watermark (navy) passes its end", x=0.01, ha="left", weight="bold", fontsize=11)
    fig.tight_layout()


# ---------------------------------------------------------------------------
# tools/dbt · dbt
# ---------------------------------------------------------------------------

_STEP_CSS = """
#%%ID%% {font-family: system-ui, sans-serif; color: %%INK%%; background: #fff; border: 1px solid %%GRID%%; border-radius: 8px;
     padding: 12px 14px; max-width: 100%; overflow-x: auto; color-scheme: light}
#%%ID%% code {font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 12.5px; background: #f4f2ec;
          padding: 2px 5px; border-radius: 4px; color: %%NAVY%%; white-space: pre-wrap}
#%%ID%% .st-bar {display: flex; gap: 6px; align-items: center; margin: 10px 0 6px}
#%%ID%% button {font: inherit; font-size: 13px; padding: 3px 10px; border: 1px solid %%FADE%%; border-radius: 5px;
            background: #fff; color: %%INK%%; cursor: pointer}
#%%ID%% button:hover {border-color: %%NAVY%%}
#%%ID%% .st-count {font-size: 12px; color: %%MUTED%%; margin-left: 6px; font-variant-numeric: tabular-nums}
#%%ID%% .st-cap {min-height: 4.2em; font-size: 13.5px; line-height: 1.45; margin: 4px 0 10px}
#%%ID%% .st-key {font-size: 12px; color: %%MUTED%%; margin-bottom: 8px}
#%%ID%% .st-key span {display: inline-block; width: 12px; height: 12px; vertical-align: -2px; margin: 0 4px 0 12px;
                  border-radius: 2px; box-sizing: border-box}
"""

_STEP_JS = """
(function () {
  const root = document.getElementById("%%ID%%"), S = %%STEPS%%;
  const els = Array.from(root.querySelectorAll("[data-k]")), base = els.map(e => e.getAttribute("class") || "");
  const cap = root.querySelector(".st-cap"), count = root.querySelector(".st-count");
  const play = root.querySelector(".st-play");
  let k = 0, timer = null;
  function show(i) {
    k = Math.max(0, Math.min(S.length - 1, i));
    const st = S[k].state;
    els.forEach((e, j) => e.setAttribute("class", (base[j] + " " + (st[e.dataset.k] || "")).trim()));
    cap.innerHTML = S[k].caption;
    count.textContent = "step " + (k + 1) + " / " + S.length;
  }
  function stop() { clearInterval(timer); timer = null; play.textContent = "▶ Play"; }
  root.querySelector(".st-prev").onclick = () => { stop(); show(k - 1); };
  root.querySelector(".st-next").onclick = () => { stop(); show(k + 1); };
  root.querySelector(".st-first").onclick = () => { stop(); show(0); };
  play.onclick = () => {
    if (timer) return stop();
    if (k === S.length - 1) show(0);
    play.textContent = "⏸ Pause";
    timer = setInterval(() => { if (k === S.length - 1) stop(); else show(k + 1); }, %%SPEED%%);
  };
  show(0);
})();
"""


def _stepper(body, steps, css="", key="", speed_ms=1800, title=""):
    """A Prev / Play / Next player. `body` is HTML whose elements carry `data-k` ids; each step is a dict with a
    `caption` (HTML) and a `state` mapping ids to the classes they get at that step (on top of their own)."""
    import json
    import uuid

    from IPython.display import HTML

    uid = "st-" + uuid.uuid4().hex[:10]
    tokens = {"ID": uid, "INK": INK, "GRID": GRID, "FADE": FADE, "NAVY": NAVY, "MUTED": MUTED, "BAD": BAD,
              "AMBER": SERIES[3], "TEAL": SERIES[0], "READ": "#f2dfb4",
              "STEPS": json.dumps(steps).replace("</", "<\\/"), "SPEED": str(speed_ms)}
    style, js = _STEP_CSS + css, _STEP_JS
    for k, v in tokens.items():
        style, js = style.replace(f"%%{k}%%", v), js.replace(f"%%{k}%%", v)
    body = body.replace("%%ID%%", uid)
    return HTML(f"""
<div id="{uid}">
  <style>{style}</style>
  {title}
  <div class="st-bar">
    <button class="st-first" title="back to the start">⏮</button>
    <button class="st-prev">◀ Prev</button>
    <button class="st-play">▶ Play</button>
    <button class="st-next">Next ▶</button>
    <span class="st-count"></span>
  </div>
  <div class="st-cap">{steps[0]["caption"]}</div>
  {f'<div class="st-key">{key}</div>' if key else ''}
  {body}
  <script>{js}</script>
</div>""")


def _clean_sql(text: str) -> list[str]:
    """dbt's generated SQL without its blank lines and common indentation."""
    import textwrap

    lines = [l.rstrip() for l in text.replace("\r\n", "\n").split("\n") if l.strip()]
    return textwrap.dedent("\n".join(lines)).split("\n") if lines else []


def sql_steps(panels):
    """Side-by-side code panels, e.g. a model file, its compiled SQL and the SQL dbt ran.

    `panels` is a list of (title, text) or (title, text, note). Jinja (`{{ }}`, `{% %}`) is marked in amber; a line
    that is not in the panel on its left is highlighted, so each panel shows what that step added.
    """
    import html
    import re

    from IPython.display import HTML

    jinja = re.compile(r"(\{\{.*?\}\}|\{%.*?%\})")
    out, prev = [], None
    for i, (title, text, *note) in enumerate(panels):
        lines = _clean_sql(text)
        seen = {l.strip().lower() for l in prev} if prev is not None else None
        body = []
        for l in lines:
            new = seen is not None and l.strip().lower() not in seen
            code = jinja.sub(lambda m: f'<span class="sq-j">{m[1]}</span>', html.escape(l))
            body.append(f'<div class="sq-l{" sq-new" if new else ""}">{code or " "}</div>')
        if i:
            out.append('<div class="sq-arrow">→</div>')
        out.append(f'<div class="sq-p"><div class="sq-t">{html.escape(title)}</div>'
                   + (f'<div class="sq-n">{html.escape(note[0])}</div>' if note else "")
                   + f'<div class="sq-c">{"".join(body)}</div></div>')
        prev = lines
    css = f"""
<style>
.sq-w {{font-family: system-ui, sans-serif; color: {INK}; display: flex; gap: 8px; align-items: stretch; overflow-x: auto;
        color-scheme: light; background: #fff; padding: 4px 0}}
.sq-p {{flex: 1 1 0; min-width: 250px; border: 1px solid {GRID}; border-radius: 8px; padding: 10px 12px; background: #fff}}
.sq-t {{font-weight: bold; font-size: 13px; margin-bottom: 2px}}
.sq-n {{font-size: 12px; color: {MUTED}; margin-bottom: 6px}}
.sq-c {{font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 12px; line-height: 1.5; margin-top: 6px}}
.sq-l {{white-space: pre-wrap; padding: 0 6px; border-left: 3px solid transparent; word-break: break-word}}
.sq-l.sq-new {{background: {_tint(NAVY, 0.08)}; border-left-color: {NAVY}}}
.sq-j {{background: #f2dfb4; color: #6b4a00; border-radius: 3px}}
.sq-arrow {{align-self: center; font-size: 20px; color: {MUTED}}}
.sq-k {{font-family: system-ui, sans-serif; font-size: 12px; color: {MUTED}; margin-top: 6px}}
.sq-k span {{display: inline-block; width: 12px; height: 12px; vertical-align: -2px; margin: 0 4px 0 12px}}
</style>"""
    key = (f'<div class="sq-k"><span style="background:#f2dfb4"></span>Jinja: dbt replaces it'
           f'<span style="background:{_tint(NAVY, 0.15)}; border-left: 3px solid {NAVY}"></span>'
           f'line that is not in the panel on its left</div>')
    return HTML(css + f'<div class="sq-w">{"".join(out)}</div>' + key)


@_figure
def database_map(tables, before=None, title=None):
    """The schemas of a database and the relations in each, from `information_schema.tables`.

    `tables` (and `before`, an earlier snapshot) have `table_schema`, `table_name` and `table_type`. What is in
    `tables` but not in `before` is highlighted as new.
    """
    old = set() if before is None else set(zip(before.table_schema, before.table_name))
    old_schemas = set() if before is None else set(before.table_schema)
    schemas = sorted(tables.table_schema.unique(), key=lambda s: (s != "raw", "marts" in s, s))   # in flow order
    groups = [(s, tables[tables.table_schema == s].sort_values("table_name")) for s in schemas]
    widths = [max(len(s) * 1.15, *(len(n) * 1.1 + 9 for n in g.table_name)) + 2 for s, g in groups]
    rows = max(len(g) for _, g in groups) + 2.6
    gap = 3
    total = sum(widths) + gap * (len(widths) - 1)
    fig, ax = plt.subplots(figsize=(total * CHAR_IN + 0.4, rows * ROW_IN + 0.75))
    x = 0
    for (s, g), w in zip(groups, widths):
        new_schema = before is not None and s not in old_schemas
        ax.add_patch(FancyBboxPatch((x, 0), w, len(g) + 2, boxstyle="round,pad=0,rounding_size=0.4",
                                    fc="white", ec=NAVY if new_schema else FADE, lw=1.6 if new_schema else 1))
        ax.text(x + 1, 0.7, s, family=MONO, fontsize=9, weight="bold", va="center", color=NAVY if new_schema else INK)
        ax.text(x + 1, 1.4, "new schema" if new_schema else "schema", fontsize=7.5, va="center",
                color=NAVY if new_schema else MUTED)
        for i, r in enumerate(g.itertuples()):
            new = before is not None and (s, r.table_name) not in old
            kind = "VIEW" if r.table_type == "VIEW" else "TABLE"
            y = 2.4 + i
            if new:
                ax.add_patch(Rectangle((x + 0.4, y - 0.42), w - 0.8, 0.84, color=NAVY, alpha=0.10, lw=0))
            ax.text(x + 1, y, kind, family=MONO, fontsize=7.5, va="center", color=MUTED,
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=FADE if kind == "VIEW" else MUTED, lw=0.8,
                              ls="--" if kind == "VIEW" else "-"))
            ax.text(x + 8, y, r.table_name, family=MONO, fontsize=8.5, va="center", color=INK,
                    weight="bold" if new else "normal")
        x += w + gap
    ax.set_xlim(-0.3, total + 0.3)
    ax.set_ylim(rows - 0.4, -0.9)
    ax.axis("off")
    ax.set_title(title or "Schemas in the database and the relations in each (TABLE stores rows, VIEW stores a query)"
                 + ("; new ones highlighted" if before is not None else ""))


def _dbt_graph(project_dir):
    """Nodes, tests and a layered layout (in pixels) of a dbt project, read from target/manifest.json."""
    import json

    m = json.loads((Path(project_dir) / "target" / "manifest.json").read_text(encoding="utf-8"))
    nodes = {}
    for uid, s in m["sources"].items():
        nodes[uid] = dict(name=s["name"], kind="source", sub=f"source · {s['schema']}.{s['identifier']}", parents=[])
    for uid, n in m["nodes"].items():
        if n["resource_type"] in ("model", "seed", "snapshot"):
            kind = n["config"].get("materialized", n["resource_type"])
            nodes[uid] = dict(name=n["name"], kind=kind, sub=f"{kind} · {n['schema']}", parents=n["depends_on"]["nodes"])
    for n in nodes.values():
        n["parents"] = [p for p in n["parents"] if p in nodes]

    depth = _depths(nodes)
    tests = {}
    for uid, n in m["nodes"].items():
        if n["resource_type"] != "test":
            continue
        parents = [p for p in n["depends_on"]["nodes"] if p in nodes]
        host = n.get("attached_node") or max(parents, key=lambda p: depth[p])
        meta = n.get("test_metadata") or {}
        tests[uid] = dict(name=n["name"], short=meta.get("name", "custom"), column=n.get("column_name"), host=host)
    return _layered(nodes, tests, depth)


def _depths(nodes):
    """Column of each node: one more than its deepest parent."""
    depth = {}

    def d(uid):
        if uid not in depth:
            depth[uid] = 1 + max((d(p) for p in nodes[uid]["parents"]), default=-1)
        return depth[uid]

    for uid in nodes:
        d(uid)
    return depth


def _layered(nodes, tests, depth):
    """Layered layout (in pixels) of a graph: `nodes` {id: name, sub, parents}, `tests` {id: short, host}."""
    # columns by depth; inside a column, order by the mean position of the parents
    cols, row = [], {}
    for k in range(max(depth.values()) + 1):
        col = [u for u in nodes if depth[u] == k]
        if k:
            col.sort(key=lambda u: (sum(row[p] for p in nodes[u]["parents"]) / len(nodes[u]["parents"]), nodes[u]["name"]))
        row.update({u: i for i, u in enumerate(col)})
        cols.append(col)
    for t in tests.values():
        nodes[t["host"]].setdefault("tests", []).append(t)

    def pill_w(t):
        return len(t["short"]) * 6.4 + 12

    box_h, pitch, col_gap = 40, 82, 56
    x, pos, col_box = 10, {}, []
    tallest = max(len(c) for c in cols)
    for col in cols:
        w = max(max(len(nodes[u]["name"]) * 7.6, len(nodes[u]["sub"]) * 6.3,
                    sum(pill_w(t) + 4 for t in nodes[u].get("tests", []))) + 22 for u in col)
        y0 = 30 + (tallest - len(col)) * pitch / 2
        for i, u in enumerate(col):
            pos[u] = (x, y0 + i * pitch, w)
        col_box.append((x, w))
        x += w + col_gap

    def lanes(k):
        """Free horizontal lanes (y) above, between and below the boxes of column k."""
        ys = sorted(pos[u][1] for u in cols[k])
        return [ys[0] - 14] + [(a + box_h + 21 + b) / 2 for a, b in zip(ys, ys[1:])] + [ys[-1] + box_h + 34]

    # An edge that skips columns runs through a free lane of each column it crosses, never under a box.
    edges = []
    for u, n in nodes.items():
        ux, uy, _ = pos[u]
        for p in n["parents"]:
            px, py, pw = pos[p]
            (x1, y1), (x2, y2) = (px + pw, py + box_h / 2), (ux - 4, uy + box_h / 2)
            pts = [(x1, y1)]
            for k in range(depth[p] + 1, depth[u]):
                cx, cw = col_box[k]
                want = y1 + (y2 - y1) * (cx + cw / 2 - x1) / (x2 - x1)
                lane = min(lanes(k), key=lambda b: abs(b - want))
                pts += [(cx - 8, lane), (cx + cw + 8, lane)]
            edges.append((p, u, pts + [(x2, y2)]))
    height = max([30 + tallest * pitch] + [y + 16 for *_, pts in edges for _, y in pts])
    return dict(nodes=nodes, tests=tests, pos=pos, box_h=box_h, pill_w=pill_w, edges=edges,
                width=x - col_gap + 10, height=height)


def _edge_path(pts):
    """SVG path through the points of a routed edge: curves between columns, straight lines through them."""
    d = f"M{pts[0][0]:.0f},{pts[0][1]:.0f}"
    for i in range(1, len(pts)):
        (x0, y0), (x1, y1) = pts[i - 1], pts[i]
        if i % 2:
            mid = (x0 + x1) / 2
            d += f" C{mid:.0f},{y0:.0f} {mid:.0f},{y1:.0f} {x1:.0f},{y1:.0f}"
        else:
            d += f" L{x1:.0f},{y1:.0f}"
    return d


_DAG_CSS = """
#%%ID%% svg text {font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace}
#%%ID%% .n rect {fill: #fff; stroke: %%FADE%%; stroke-width: 1.3; transition: fill .35s, stroke .35s}
#%%ID%% .n {transition: opacity .35s}
#%%ID%% .n .nm {fill: %%INK%%; font-size: 12.5px; font-weight: bold; transition: fill .35s}
#%%ID%% .n .sb {fill: %%MUTED%%; font-size: 10.5px; transition: fill .35s}
#%%ID%% .n.src rect {fill: #f6f4ee}
#%%ID%% .n.unsel {opacity: .28}
#%%ID%% .n.sel rect {stroke: %%NAVY%%; stroke-width: 2}
#%%ID%% .n.ok rect {fill: %%NAVY%%; stroke: %%NAVY%%}
#%%ID%% .n.ok .nm, #%%ID%% .n.ok .sb, #%%ID%% .n.fail .nm, #%%ID%% .n.fail .sb {fill: #fff}
#%%ID%% .n.fail rect {fill: %%BAD%%; stroke: %%BAD%%}
#%%ID%% .n.skip rect {fill: #f1efe9; stroke: %%MUTED%%; stroke-dasharray: 5 3}
#%%ID%% .n.skip .nm {fill: %%MUTED%%; text-decoration: line-through}
#%%ID%% .n.cur rect {stroke: %%AMBER%%; stroke-width: 4}
#%%ID%% .e {fill: none; stroke: %%FADE%%; stroke-width: 1.4; transition: opacity .35s, stroke .35s}
#%%ID%% .e.unsel {opacity: .15}
#%%ID%% .e.sel {stroke: %%NAVY%%}
#%%ID%% .p rect {fill: #fff; stroke: %%FADE%%; transition: fill .35s, stroke .35s}
#%%ID%% .p {transition: opacity .35s}
#%%ID%% .p text {fill: %%MUTED%%; font-size: 10px}
#%%ID%% .p.unsel {opacity: .25}
#%%ID%% .p.sel rect {stroke: %%NAVY%%}
#%%ID%% .p.pass rect {fill: %%TEAL%%; stroke: %%TEAL%%}
#%%ID%% .p.fail rect {fill: %%BAD%%; stroke: %%BAD%%}
#%%ID%% .p.pass text, #%%ID%% .p.fail text {fill: #fff}
#%%ID%% .p.skip rect {stroke-dasharray: 3 2}
#%%ID%% .p.skip text {text-decoration: line-through}
#%%ID%% .p.cur rect {stroke: %%AMBER%%; stroke-width: 3}
"""


def animate_dbt(project_dir, speed_ms=1500):
    """Replay the last dbt command on the project's graph, node by node, as a player in the notebook output.

    The graph comes from `target/manifest.json` (models, sources, tests, and the `source()` / `ref()` edges); what
    ran, in which order and with which outcome comes from `target/run_results.json`. Call it right after the command.
    """
    import html
    import json

    g = _dbt_graph(project_dir)
    nodes, tests, pos, box_h = g["nodes"], g["tests"], g["pos"], g["box_h"]
    rr = json.loads((Path(project_dir) / "target" / "run_results.json").read_text(encoding="utf-8"))
    args = rr.get("args", {})
    select = args.get("select") or []
    cmd = " ".join(["dbt", args.get("which", "?"), *(["--select", *select] if select else []),
                    *(["--full-refresh"] if args.get("full_refresh") else [])])
    results = [r for r in rr["results"] if r["unique_id"] in nodes or r["unique_id"] in tests]
    ran = {r["unique_id"] for r in results}
    ids = {u: f"n{i}" for i, u in enumerate(nodes)} | {u: f"t{i}" for i, u in enumerate(tests)}

    # --- drawing -----------------------------------------------------------------------------------------------
    svg, edges = [], []
    for p, u, pts in g["edges"]:
        k = f"e{len(edges)}"
        edges.append((k, p, u))
        svg.append(f'<path data-k="{k}" class="e" marker-end="url(#ah-%%ID%%)" d="{_edge_path(pts)}"/>')
    for u, n in nodes.items():
        x, y, w = pos[u]
        svg.append(f'<g data-k="{ids[u]}" class="n{" src" if n["kind"] == "source" else ""}">'
                   f'<rect x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{box_h}" rx="6"/>'
                   f'<text class="nm" x="{x + 10:.0f}" y="{y + 17:.0f}">{html.escape(n["name"])}</text>'
                   f'<text class="sb" x="{x + 10:.0f}" y="{y + 32:.0f}">{html.escape(n["sub"])}</text></g>')
        px = x
        for t in n.get("tests", []):
            tw = g["pill_w"](t)
            uid = next(k for k, v in tests.items() if v is t)
            svg.append(f'<g data-k="{ids[uid]}" class="p"><title>{html.escape(t["name"])}</title>'
                       f'<rect x="{px:.0f}" y="{y + box_h + 5:.0f}" width="{tw:.0f}" height="16" rx="8"/>'
                       f'<text x="{px + tw / 2:.0f}" y="{y + box_h + 16.5:.0f}" text-anchor="middle">'
                       f'{html.escape(t["short"])}</text></g>')
            px += tw + 4
    body = (f'<svg width="{g["width"]:.0f}" height="{g["height"]:.0f}" style="display:block">'
            f'<defs><marker id="ah-%%ID%%" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{MUTED}"/></marker></defs>'
            + "".join(svg) + "</svg>")

    # --- steps -------------------------------------------------------------------------------------------------
    def label(u):
        if u in nodes:
            return f"<b>{html.escape(nodes[u]['name'])}</b>"
        t = tests[u]
        on = nodes[t["host"]]["name"] + (f".{t['column']}" if t["column"] else "")
        return (f"test <b>{html.escape(t['short'])}</b> on <code>{html.escape(on)}</code>" if t["short"] != "custom"
                else f"test <b>{html.escape(t['name'])}</b> (a SQL file in <code>tests/</code>)")

    wrap = {"view": "<code>CREATE VIEW … AS</code>", "table": "<code>CREATE TABLE … AS</code> (the new table "
            "replaces the old one)", "incremental": "<code>CREATE TABLE … AS</code> on the first run and, on later "
            "runs, a <code>DELETE</code> + <code>INSERT</code> of the rows the SELECT returns"}
    selection = {}
    for u in nodes:
        selection[ids[u]] = "" if nodes[u]["kind"] == "source" else ("sel" if u in ran else "unsel")
    for u in tests:
        selection[ids[u]] = "sel" if u in ran else "unsel"
    for k, p, c in edges:
        selection[k] = "sel" if c in ran else "unsel"

    n_models = sum(n["kind"] != "source" for n in nodes.values())
    n_sources = len(nodes) - n_models
    steps = [dict(state={}, caption=f"The project as dbt reads it: {n_models} models, {n_sources} sources and "
                  f"{len(tests)} tests (the pills under the model they check). Every arrow is a <code>source()</code> "
                  "or <code>ref()</code> call: dbt found them by reading the SQL files, before running anything.")]
    how = []
    for s in select:
        name = s.strip("+")
        if s.startswith("+"):
            how.append(f"<code>+</code> before <code>{html.escape(name)}</code> adds all its parents")
        if s.endswith("+"):
            how.append(f"<code>+</code> after <code>{html.escape(name)}</code> adds all its children")
    sel_text = (f"<code>{html.escape(cmd)}</code> selects {len(ran)} of {n_models + len(tests)} nodes (outlined)"
                if select else f"<code>{html.escape(cmd)}</code> has no <code>--select</code>, so every node runs")
    if how:
        sel_text += ": " + "; ".join(how)
    sel_text += ". "
    if args.get("which") == "run" and tests:
        sel_text += "<code>dbt run</code> builds models only: tests run with <code>dbt test</code> or <code>dbt build</code>. "
    sel_text += "A node starts only once all its parents are done, so the order follows the arrows."
    steps.append(dict(state=dict(selection), caption=sel_text))

    done, count, failed_test = {}, {"ok": 0, "fail": 0, "skip": 0}, False
    for i, r in enumerate(results, 1):
        u, status = r["unique_id"], r["status"]
        outcome = {"success": "ok", "pass": "ok", "warn": "ok", "skipped": "skip"}.get(status, "fail")
        count[outcome] += 1
        done[ids[u]] = (selection[ids[u]] + " " + ("pass" if u in tests and outcome == "ok" else outcome)).strip()
        state = selection | done
        state[ids[u]] = done[ids[u]] + " cur"
        prefix = f"{i}/{len(results)} · "
        if u in nodes and outcome == "ok":
            text = (f"{label(u)} ({nodes[u]['kind']}): dbt renders its SELECT, wraps it in "
                    f"{wrap.get(nodes[u]['kind'], 'the SQL of its materialization')}, and DuckDB runs it → "
                    f"<b>success</b> in {r.get('execution_time', 0):.2f} s.")
        elif u in nodes and outcome == "skip":
            why = ("a test on data it reads failed, so dbt does not build on data it has just found to be wrong"
                   if failed_test else "a model it depends on failed")
            text = f"{label(u)} is <b>skipped</b>: {why}. Its table keeps its previous contents."
        elif u in nodes:
            text = f"{label(u)} <b>failed</b>: {html.escape((r.get('message') or '').splitlines()[0][:200])}"
        elif outcome == "ok":
            text = (f"{label(u)}: a test is a SELECT of the rows that break the rule. It returned 0 rows → "
                    f"<b>{'pass' if status == 'pass' else status}</b>.")
        elif outcome == "skip":
            text = f"{label(u)} is <b>skipped</b>: the model it checks was skipped."
        else:
            failed_test = True
            text = (f"{label(u)}: the SELECT of rule-breaking rows returned <b>{r.get('failures') or '?'} row(s)</b> → "
                    f"<b>fail</b>. Note that <code>{html.escape(nodes[tests[u]['host']]['name'])}</code> was already "
                    "built: the bad rows are in the database now. The test stops what comes <b>after</b> it.")
        steps.append(dict(state=state, caption=prefix + text))
    steps.append(dict(state=selection | done, caption=(
        f"Done: {count['ok']} succeeded, {count['fail']} failed, {count['skip']} skipped. Everything in this player "
        "came from two files dbt wrote: <code>target/manifest.json</code> (the graph) and "
        "<code>target/run_results.json</code> (what ran, in which order, with which outcome).")))
    key = (f'<span style="background:#fff; border:1.5px solid {NAVY}"></span>selected'
           f'<span style="background:{NAVY}"></span>built'
           f'<span style="background:{SERIES[0]}"></span>test passed'
           f'<span style="background:{BAD}"></span>failed'
           f'<span style="background:#f1efe9; border:1.5px dashed {MUTED}"></span>skipped'
           f'<span style="background:#fff; border:3px solid {SERIES[3]}"></span>current')
    return _stepper(body, steps, css=_DAG_CSS, key=key, speed_ms=speed_ms)


@_figure
def dbt_graph(project_dir, title=None):
    """The project's graph from target/manifest.json: one box per source and model, coloured by materialization,
    with its tests as pills underneath."""
    from matplotlib.path import Path as MPath
    from matplotlib.patches import PathPatch

    g = _dbt_graph(project_dir)
    nodes, pos, box_h = g["nodes"], g["pos"], g["box_h"]
    fill = {"source": "#f6f4ee", "view": "white", "table": NAVY, "incremental": SERIES[2], "ephemeral": "white"}
    px_in, top_in = 1 / 80, 0.55   # one layout pixel is drawn as 1/80 inch, so the text fits its box
    fig_w, fig_h = g["width"] * px_in, g["height"] * px_in + top_in
    fig = plt.figure(figsize=(fig_w, fig_h))
    ax = fig.add_axes([0, 0, 1, 1 - top_in / fig_h])
    for _, _, pts in g["edges"]:
        verts, codes = [pts[0]], [MPath.MOVETO]
        for i in range(1, len(pts)):
            (x0, y0), (x1, y1) = pts[i - 1], pts[i]
            if i % 2:
                mid = (x0 + x1) / 2
                verts += [(mid, y0), (mid, y1), (x1, y1)]
                codes += [MPath.CURVE4] * 3
            else:
                verts.append((x1, y1))
                codes.append(MPath.LINETO)
        ax.add_patch(PathPatch(MPath(verts, codes), fc="none", ec=FADE, lw=1.2, zorder=1))
        ax.plot([pts[-1][0]], [pts[-1][1]], marker=">", ms=4, color=MUTED, zorder=2)
    for u, n in nodes.items():
        x, y, w = pos[u]
        f = fill.get(n["kind"], "white")
        dark = f in (NAVY, SERIES[2])
        ax.add_patch(FancyBboxPatch((x, y), w, box_h, boxstyle="round,pad=0,rounding_size=6", fc=f,
                                    ec=f if dark else (SERIES[0] if n["kind"] == "view" else FADE),
                                    lw=1.6 if n["kind"] == "view" else 1, ls="--" if n["kind"] == "view" else "-",
                                    zorder=3))
        ax.text(x + 10, y + 15, n["name"], family=MONO, fontsize=9, weight="bold", va="center",
                color="white" if dark else INK, zorder=4)
        ax.text(x + 10, y + 30, n["sub"], family=MONO, fontsize=7.5, va="center",
                color="white" if dark else MUTED, zorder=4)
        tx = x
        for t in n.get("tests", []):
            tw = g["pill_w"](t)
            ax.add_patch(FancyBboxPatch((tx, y + box_h + 5), tw, 16, boxstyle="round,pad=0,rounding_size=8",
                                        fc="white", ec=SERIES[0], lw=1, zorder=3))
            ax.text(tx + tw / 2, y + box_h + 13, t["short"], family=MONO, fontsize=7, ha="center", va="center",
                    color=SERIES[0], zorder=4)
            tx += tw + 4
    ax.set_xlim(0, g["width"])
    ax.set_ylim(g["height"], 0)
    ax.axis("off")
    n_models = sum(n["kind"] != "source" for n in nodes.values())
    fig.text(0.01, 1 - 0.08 / fig_h, ha="left", va="top", fontsize=10.5, weight="bold", s=title or f"The project's graph: {len(nodes) - n_models} sources → {n_models} models, "
                 f"{len(g['tests'])} tests (pills).\nBox = materialization: dashed = view, navy = table, purple = incremental")


# ---------------------------------------------------------------------------
# Dagster · the asset graph, and one run replayed on it
# ---------------------------------------------------------------------------

def _asset_graph(defs, collapse=False):
    """Assets, checks and a layered layout of Dagster definitions (assets, multi-assets and asset checks).

    `collapse=True` draws one pill per asset ("4 checks") instead of one per check, for big static graphs."""
    import dagster as dg

    check_defs = [a for a in defs if isinstance(a, dg.AssetChecksDefinition)]
    asset_defs = [a for a in defs if not isinstance(a, dg.AssetChecksDefinition)]
    needed = {k for a in defs for k in a.required_resource_keys} - {"io_manager"}   # the graph, not the resources
    graph = dg.Definitions(assets=asset_defs, asset_checks=check_defs,
                           resources={k: dg.ResourceDefinition.none_resource() for k in needed}).resolve_asset_graph()
    blocking = {spec.key: spec.blocking for a in defs for spec in a.check_specs}
    nodes, tests = {}, {}
    for key in graph.get_all_asset_keys():
        n = graph.get(key)
        sub = [] if n.group_name == "default" else [n.group_name]
        if n.partitions_def is not None:
            sub.append(type(n.partitions_def).__name__.replace("PartitionsDefinition", "").lower() + " partitions")
        sub += sorted(n.kinds)
        if not n.is_materializable:
            sub = ["external: not built here"]
        nodes[key] = dict(name=key.to_user_string(), kind="asset" if n.is_materializable else "source",
                          sub=" · ".join(sub) or "asset", parents=sorted(n.parent_keys, key=str))
        for ck in sorted(n.check_keys, key=lambda c: c.name):
            cut = ck.name.find("_" + key.path[-1])          # dbt test names: not_null_stg_orders_order_id
            tests[ck] = dict(name=ck.name, short=ck.name[:cut] if cut > 0 else ck.name, column=None, host=key,
                             blocking=blocking.get(ck, False))
    for n in nodes.values():
        n["parents"] = [p for p in n["parents"] if p in nodes]
    counts = dict(n_checks=len(tests), n_block=sum(t["blocking"] for t in tests.values()), collapsed=collapse)
    if collapse:
        hosts = {}
        for t in tests.values():
            hosts.setdefault(t["host"], []).append(t)
        tests = {("checks", h): dict(name=", ".join(t["name"] for t in ts), short=f"{len(ts)} check{'s' * (len(ts) > 1)}",
                                     column=None, host=h, blocking=all(t["blocking"] for t in ts))
                 for h, ts in hosts.items()}
    g = _layered(nodes, tests, _depths(nodes))
    steps = {}                                               # step key (the op's name) -> the assets and checks it makes
    for a in defs:
        steps[a.node_def.name] = set(a.keys) | set(a.check_keys)
    return g | counts | dict(tests=tests, steps=steps, graph=graph)


def asset_run(defs, result=None, selection=None, speed_ms=1500):
    """Dagster assets as a graph and, given the result of `dg.materialize`, that run replayed event by event.

    `defs` is what was passed to `materialize` (assets, multi-assets such as dbt's, asset checks). `selection` is the
    `AssetSelection` or list of assets the run was asked for (default: all of them). Every step of the player is one
    event of the run's log (`result.all_events`): materializations, check results, failures and retries.
    """
    import html

    import dagster as dg

    g = _asset_graph(defs, collapse=result is None and selection is None)
    nodes, tests, pos, box_h = g["nodes"], g["tests"], g["pos"], g["box_h"]
    ids = {u: f"n{i}" for i, u in enumerate(nodes)} | {u: f"t{i}" for i, u in enumerate(tests)}

    svg, edges = [], []
    for p, u, pts in g["edges"]:
        k = f"e{len(edges)}"
        edges.append((k, p, u))
        svg.append(f'<path data-k="{k}" class="e" marker-end="url(#ah-%%ID%%)" d="{_edge_path(pts)}"/>')
    for u, n in nodes.items():
        x, y, w = pos[u]
        svg.append(f'<g data-k="{ids[u]}" class="n{" src" if n["kind"] == "source" else ""}">'
                   f'<rect x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{box_h}" rx="6"/>'
                   f'<text class="nm" x="{x + 10:.0f}" y="{y + 17:.0f}">{html.escape(n["name"])}</text>'
                   f'<text class="sb" x="{x + 10:.0f}" y="{y + 32:.0f}">{html.escape(n["sub"])}</text></g>')
        px = x
        for t in n.get("tests", []):
            tw = g["pill_w"](t)
            uid = next(k for k, v in tests.items() if v is t)
            svg.append(f'<g data-k="{ids[uid]}" class="p{" blk" if t["blocking"] else ""}">'
                       f'<title>{html.escape(t["name"])}{" (blocking)" if t["blocking"] else ""}</title>'
                       f'<rect x="{px:.0f}" y="{y + box_h + 5:.0f}" width="{tw:.0f}" height="16" rx="8"/>'
                       f'<text x="{px + tw / 2:.0f}" y="{y + box_h + 16.5:.0f}" text-anchor="middle">'
                       f'{html.escape(t["short"])}</text></g>')
            px += tw + 4
    body = (f'<svg width="{g["width"]:.0f}" height="{g["height"]:.0f}" style="display:block">'
            f'<defs><marker id="ah-%%ID%%" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{MUTED}"/></marker></defs>'
            + "".join(svg) + "</svg>")
    css = _DAG_CSS + "#%%ID%% .p.blk rect {stroke-width: 2.4}\n"

    def many(n, word):
        return f"{n} {word}{'' if n == 1 else 's'}"

    def meta_text(metadata):
        """Metadata for a caption: ids and dbt's bookkeeping left out, numbers shortened."""
        out = []
        for k, v in metadata.items():
            if k in ("unique_id", "invocation_id") or k.startswith("dagster"):
                continue
            v = v.value
            out.append(f"{k} = " + (f"{v:.2f}" if isinstance(v, float) else f"{v:,}" if isinstance(v, int) else str(v)))
        return html.escape(", ".join(out))

    n_assets = sum(n["kind"] == "asset" for n in nodes.values())
    first = (f"The graph Dagster built from the code: {many(n_assets, 'asset')}"
             + (f", {many(g['n_checks'], 'check')} ("
                + ("one pill per asset, thick when all its checks are blocking; " if g["collapsed"] else "the pills; ")
                + f"{g['n_block']} blocking)" if g["n_checks"] else "")
             + ". Every arrow is a dependency it read from a function's arguments or its <code>deps</code>, "
             "before running anything.")
    steps = [dict(state={}, caption=first)]
    if result is None and selection is None:
        return _stepper(body, steps, css=css, key="", speed_ms=speed_ms)

    if selection is None:
        chosen = {u for u, n in nodes.items() if n["kind"] == "asset"}
    elif isinstance(selection, dg.AssetSelection):
        chosen = set(selection.resolve(g["graph"]))
    else:
        chosen = {k for a in selection for k in a.keys}
    chosen_checks = {c for c, t in tests.items() if t["host"] in chosen}
    sel = {ids[u]: ("sel" if u in chosen else "unsel") for u, n in nodes.items() if n["kind"] == "asset"}
    sel |= {ids[c]: ("sel" if c in chosen_checks else "unsel") for c in tests}
    sel |= {k: ("sel" if c in chosen else "unsel") for k, p, c in edges}
    part = result.dagster_run.tags.get("dagster/partition") if result is not None else None
    steps.append(dict(state=dict(sel), caption=(
        f"This run asks for {len(chosen)} of {many(n_assets, 'asset')} (outlined)"
        + (f", for the partition <b>{html.escape(part)}</b>" if part else "")
        + ". Dagster starts a step once all its parents are done, so the order follows the arrows.")))
    if result is None:
        return _stepper(body, steps, css=css, key="", speed_ms=speed_ms)

    def error_line(err):
        while err is not None and err.cause is not None:
            err = err.cause
        return html.escape(((err.message if err else "") or "").strip().splitlines()[-1][:200])

    done, failed_block, retries = {}, False, {}
    count = {"ok": 0, "fail": 0, "skip": 0}
    events = [e for e in result.all_events if e.event_type_value in
              ("ASSET_MATERIALIZATION", "ASSET_CHECK_EVALUATION", "STEP_FAILURE", "STEP_UP_FOR_RETRY")]
    for e in events:
        t, data = e.event_type_value, e.event_specific_data
        if t == "ASSET_MATERIALIZATION":
            u = e.asset_key
            meta = meta_text(data.materialization.metadata)
            done[ids[u]] = "sel ok"
            count["ok"] += 1
            text = f"<b>{html.escape(nodes[u]['name'])}</b> is materialized" + (f" · metadata: {meta}" if meta else "")
            if u in retries:
                text += f", on attempt {retries[u] + 1}"
            cur = ids[u]
        elif t == "ASSET_CHECK_EVALUATION":
            ck = dg.AssetCheckKey(data.asset_key, data.check_name)
            tst = tests[ck]
            meta = meta_text(data.metadata)
            done[ids[ck]] = "sel " + ("pass" if data.passed else "fail")
            on = html.escape(nodes[tst["host"]]["name"])
            if data.passed:
                text = f"check <b>{html.escape(tst['name'])}</b> on <code>{on}</code> → <b>pass</b>"
            else:
                failed_block |= tst["blocking"]
                text = (f"check <b>{html.escape(tst['name'])}</b> on <code>{on}</code> → <b>fail</b>"
                        + (f" ({meta})" if meta else "") + ". "
                        + ("It is <b>blocking</b>: nothing downstream of it runs in this run."
                           if tst["blocking"] else
                           f"It is not blocking: the failure is recorded, and the assets that read <code>{on}</code> "
                           "still run, on data the check has just found wrong."))
            if meta and data.passed:
                text += f" ({meta})"
            cur = ids[ck]
        elif t == "STEP_UP_FOR_RETRY":
            made = [u for u in g["steps"].get(e.step_key, ()) if u in nodes]
            for u in made:
                retries[u] = retries.get(u, 0) + 1
            n_try = retries.get(made[0], 1) if made else 1
            text = (f"<b>{html.escape(e.step_key)}</b> raised <code>{error_line(data.error)}</code>. Its retry policy "
                    f"allows more attempts: Dagster waits and runs it again (retry {n_try}).")
            cur = ids[made[0]] if made else None
        else:
            made = g["steps"].get(e.step_key, set())
            assets_here = [u for u in made if u in nodes and ids[u] not in done]
            if not assets_here:                              # a blocking check's step: already shown
                continue
            for u in assets_here:
                done[ids[u]] = "sel fail"
                count["fail"] += 1
            text = (f"<b>{html.escape(', '.join(nodes[u]['name'] for u in assets_here))}</b> <b>failed</b>: "
                    f"<code>{error_line(data.error)}</code>")
            cur = ids[assets_here[0]]
        state = sel | done
        if cur:
            state[cur] = state.get(cur, "") + " cur"
        steps.append(dict(state=state, caption=f"{len(steps) - 1}/{len(events)} · {text}"))

    skipped = [u for u in chosen if ids[u] not in done]
    for u in skipped:
        done[ids[u]] = "sel skip"
        count["skip"] += 1
    for c in chosen_checks:
        done.setdefault(ids[c], "sel skip")
    why = ("a blocking check upstream failed" if failed_block else "something upstream of them failed")
    end = (f"Run {'succeeded' if result.success else 'failed'}: {count['ok']} materialized, {count['fail']} failed, "
           f"{count['skip']} skipped")
    if skipped:
        end += (f" ({html.escape(', '.join(nodes[u]['name'] for u in sorted(skipped, key=str)))}: {why}; whatever "
                "they hold is from their last run)")
    end += (". Everything in this player came from the run's event log, <code>result.all_events</code>: the same "
            "record the Dagster UI shows.")
    steps.append(dict(state=sel | done, caption=end))
    key = (f'<span style="background:#fff; border:1.5px solid {NAVY}"></span>asked for'
           f'<span style="background:{NAVY}"></span>materialized'
           f'<span style="background:{SERIES[0]}"></span>check passed'
           f'<span style="background:{BAD}"></span>failed'
           f'<span style="background:#f1efe9; border:1.5px dashed {MUTED}"></span>skipped'
           f'<span style="background:#fff; border:3px solid {SERIES[3]}"></span>current')
    return _stepper(body, steps, css=css, key=key, speed_ms=speed_ms)


def animate_incremental(all_rows, candidates, before, key, cutoff, show=None, filter_col="updated_at", speed_ms=2000):
    """Step through one incremental run with the delete+insert strategy, from the lab's own tables.

    `all_rows` is what the model's SELECT returns without the incremental filter (what a full refresh would load),
    `candidates` what it returned with the filter on this run, `before` the target table before the run. `cutoff` is
    the value the filter compared `filter_col` with.
    """
    import html

    cols = [key] + [c for c in (show or before.columns) if c != key]
    frames = [all_rows, candidates, before]
    widths = [max(len(c), *(len(_fmt(v)) for f in frames for v in f[c])) + 1 for c in cols]
    cand = set(candidates[key])
    old = {r[key]: r for _, r in before.iterrows()}
    matched = [k for k in before[key] if k in cand]

    def row(k, r, changed=()):
        cells = "".join(f'<span class="{"chg" if c in changed else ""}" style="width:{w}ch">{html.escape(_fmt(r[c]))}</span>'
                        for c, w in zip(cols, widths))
        return f'<div data-k="{k}" class="ir">{cells}</div>'

    head = '<div class="ih">' + "".join(f'<span style="width:{w}ch">{html.escape(c)}</span>'
                                        for c, w in zip(cols, widths)) + "</div>"
    all_sorted = all_rows.sort_values(key).reset_index(drop=True)
    left = "".join(row(f"a{i}", r) for i, r in all_sorted.iterrows())
    b_keys = list(before[key])
    right = "".join(row(f"b{i}", r) for i, (_, r) in enumerate(before.iterrows()))
    c_rows = candidates.sort_values(key).reset_index(drop=True)
    right += "".join(row(f"c{i}", r, [c for c in cols if r[key] in old and _fmt(old[r[key]][c]) != _fmt(r[c])])
                     for i, r in c_rows.iterrows())
    body = (f'<div class="ip"><div class="it"><div class="itl">The model\'s SELECT on this run</div>{head}{left}</div>'
            f'<div class="it"><div class="itl">Target table</div>{head}{right}</div></div>')

    hide_left = {f"a{i}": "hide" for i in range(len(all_sorted))}
    hide_new = {f"c{i}": "hide" for i in range(len(c_rows))}
    filt = {f"a{i}": "in" if r[key] in cand else "out" for i, r in all_sorted.iterrows()}
    dele = {f"b{i}": "del" for i, k in enumerate(b_keys) if k in cand}
    gone = {f"b{i}": "gone" for i, k in enumerate(b_keys) if k in cand}
    new = {f"c{i}": "new" for i in range(len(c_rows))}
    untouched = [k for k in b_keys if k not in cand]
    fk = f"<code>{html.escape(key)}</code>"
    steps = [
        dict(state=hide_left | hide_new, caption=f"Before the run, the target holds {len(before)} rows from the "
             "previous run."),
        dict(state=hide_new, caption=f"Without the filter, the model's SELECT would return these {len(all_sorted)} "
             "rows: that is what the first run, or a <code>--full-refresh</code>, loads."),
        dict(state=filt | hide_new, caption=f"The target exists, so <code>is_incremental()</code> is true and the "
             f"filter is in the SQL: keep <code>{html.escape(filter_col)} &gt;= {_fmt(cutoff)}</code> (the latest "
             f"<code>{html.escape(filter_col)}</code> already in the target, minus the overlap). {len(cand)} of "
             f"{len(all_sorted)} rows pass. dbt stores them in a temporary table."),
        dict(state=filt | dele | hide_new, caption=f"<code>delete+insert</code>, part 1: delete every stored row whose "
             f"{fk} is among the selected ones ({len(matched)} rows). The match is on <code>unique_key</code>, so it "
             "only works if that key really is unique."),
        dict(state=filt | gone | new, caption=f"Part 2: insert the {len(cand)} selected rows. Values that changed "
             "are in bold."),
        dict(state=filt | gone | new, caption=f"Result: {len(before) - len(matched) + len(cand)} rows, still one per "
             f"{fk}. " + (f"Rows outside the window ({', '.join(_fmt(k) for k in untouched)}) were not read and "
                          "not touched: if their source had changed, this run would have missed it."
                          if untouched else "Every stored row was inside the window this time.")),
    ]
    css = f"""
#%%ID%% .ip {{display: flex; gap: 28px; flex-wrap: wrap; align-items: flex-start}}
#%%ID%% .it {{font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 12.5px}}
#%%ID%% .itl {{font-family: system-ui, sans-serif; font-weight: bold; font-size: 13px; margin-bottom: 4px}}
#%%ID%% .ih {{display: flex; font-weight: bold; color: {MUTED}; border-bottom: 1px solid {GRID}; padding: 0 0 3px 9px}}
#%%ID%% .ih span, #%%ID%% .ir span {{flex: none; padding-left: 6px; white-space: pre}}
#%%ID%% .ir {{display: flex; align-items: center; height: 24px; max-height: 24px; margin-top: 3px; padding-left: 4px;
          border-left: 5px solid {NAVY}; border-radius: 3px; background-color: {_tint(NAVY, 0.06)}; overflow: hidden;
          box-sizing: border-box; transition: opacity .45s, max-height .45s, margin .45s, background-color .35s}}
#%%ID%% .ir.hide, #%%ID%% .ir.gone {{opacity: 0; max-height: 0; margin-top: 0}}
#%%ID%% .ir.out {{opacity: .25}}
#%%ID%% .ir.in {{background-color: %%READ%%}}
#%%ID%% .ir.del {{background-color: {_tint(BAD, 0.16)}; border-left-color: {BAD}; text-decoration: line-through}}
#%%ID%% .ir.new {{box-shadow: 0 0 0 2px {NAVY} inset}}
#%%ID%% .ir .chg {{font-weight: bold; color: {NAVY}}}
"""
    key_html = (f'<span style="background:%%READ%%"></span>selected by the filter'
                f'<span style="background:{_tint(BAD, 0.3)}"></span>deleted'
                f'<span style="background:#fff; box-shadow: 0 0 0 2px {NAVY} inset"></span>inserted')
    return _stepper(body, steps, css=css, key=key_html.replace("%%READ%%", "#f2dfb4"), speed_ms=speed_ms)


# ---------------------------------------------------------------------------
# tools/spark · Spark
# ---------------------------------------------------------------------------

_SPARK_CSS = """
#%%ID%% svg {display: block; max-width: 100%; height: auto}
#%%ID%% svg text {font-family: system-ui, sans-serif; font-size: 12px; fill: %%INK%%}
#%%ID%% svg .mono {font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 11px}
#%%ID%% svg .mut {fill: %%MUTED%%}
#%%ID%% svg .b {font-weight: bold}
#%%ID%% svg .w {fill: #fff}
#%%ID%% [data-k] {transition: opacity .45s, transform .8s ease-in-out}
#%%ID%% .box {fill: #fff; stroke: %%FADE%%; stroke-width: 1.3; transition: fill .35s, stroke .35s}
#%%ID%% .ln {fill: none; stroke: %%FADE%%; stroke-width: 1.5; transition: opacity .45s, stroke .35s}
#%%ID%% .off {opacity: 0}
#%%ID%% .dim {opacity: .22}
#%%ID%% .ln.on {stroke: %%NAVY%%}
#%%ID%% .ln.hl {stroke: %%AMBER%%}
#%%ID%% .hl > .box {stroke: %%AMBER%%; stroke-width: 3}
#%%ID%% .chip rect {transition: fill .35s}
#%%ID%% .chip.done rect {fill: %%NAVY%%}
"""

_TEAL, _CORAL, _PURPLE, _AMBER = SERIES


def _txt(x, y, s, cls="", anchor="start", size=None):
    import html

    style = f' style="font-size:{size}px"' if size else ""
    return (f'<text x="{x:.0f}" y="{y:.0f}" class="{cls}" text-anchor="{anchor}"{style}>'
            f'{html.escape(str(s))}</text>')


def _svg(w, h, inner):
    """An SVG canvas with arrowheads (`url(#ar-%%ID%%)`, amber: `#arh-%%ID%%`) for the step players."""
    marker = ('<marker id="{}-%%ID%%" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
              'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{}"/></marker>')
    return (f'<svg viewBox="0 0 {w:.0f} {h:.0f}" width="{w:.0f}"><defs>{marker.format("ar", MUTED)}'
            f'{marker.format("arh", _AMBER)}</defs>{inner}</svg>')


def _chip(k, w, h, label, fill, cls="chip"):
    """A labelled block drawn at the origin: classes `at-<place>` move it (see `_places`)."""
    return (f'<g data-k="{k}" class="{cls}"><rect width="{w:.0f}" height="{h:.0f}" rx="4" fill="{fill}"/>'
            + _txt(w / 2, h / 2 + 4, label, "w b", "middle", 11) + "</g>")


def _places(places):
    """CSS that puts element `k` at (x, y) while it has class `at-<name>`; `places` maps (k, name) to (x, y)."""
    return "".join(f'#%%ID%% [data-k="{k}"].at-{n} {{transform: translate({x:.1f}px, {y:.1f}px)}}\n'
                   for (k, n), (x, y) in places.items())


def _frames(start, changes):
    """Steps that build on each other: each (caption, {key: classes}) changes only what it names."""
    cur, out = dict(start), []
    for caption, change in changes:
        cur = cur | change
        out.append({"caption": caption, "state": dict(cur)})
    return out


def spark_cluster(master="local[2]", cores=2, speed_ms=2600):
    """How a Spark job runs on a cluster, from "one machine is too slow" to tasks on executor cores.

    A picture with a fixed 3-worker cluster; the last step shows what `master` / `cores` mean in this notebook.
    """
    W, H = 900, 370
    wx, ww, wh, wy = 630, 250, 104, [14, 133, 252]
    slots = [(wx + 112 + j * 64, y + 52) for y in wy for j in range(2)]
    queue = [(34 + (i % 4) * 50, 214 + (i // 4) * 27) for i in range(8)]
    p = ['<g data-k="single"><rect class="box" x="290" y="120" width="320" height="110" rx="8"/>'
         + _txt(450, 156, "one machine, one core", "b", "middle", 14)
         + _txt(450, 182, "2 TB ÷ 200 MB/s ≈ 10,000 s", "mono", "middle", 12)
         + _txt(450, 206, "≈ 3 hours for one pass", "mut", "middle") + "</g>",
         '<g data-k="drv"><rect class="box" x="20" y="130" width="220" height="140" rx="8" style="stroke:'
         f'{NAVY}; stroke-width:2"/>' + _txt(34, 152, "Driver", "b", size=14)
         + _txt(34, 170, "your program + SparkSession", "mut") + "</g>",
         '<g data-k="dag">' + _txt(34, 198, "plan → job → stages → tasks", "mono") + "</g>",
         '<g data-k="cm"><rect class="box" x="310" y="20" width="240" height="62" rx="8"/>'
         + _txt(324, 44, "Cluster manager", "b", size=14) + _txt(324, 66, "YARN · Kubernetes · Standalone", "mut")
         + "</g>",
         f'<g data-k="a1"><path class="ln on" marker-end="url(#ar-%%ID%%)" d="M240,150 C280,150 280,52 308,52"/>'
         + _txt(266, 112, "1 · ask for", "mut", "end", 11) + _txt(266, 126, "executors", "mut", "end", 11) + "</g>",
         f'<g data-k="a2">' + "".join(f'<path class="ln on" marker-end="url(#ar-%%ID%%)" '
                                      f'd="M550,52 C590,52 590,{y + 40} {wx - 2},{y + 40}"/>' for y in wy)
         + _txt(430, 102, "2 · launch executors", "mut", "middle", 11) + "</g>"]
    for i, y in enumerate(wy):
        p.append(f'<path data-k="a3{i}" class="ln on" marker-end="url(#ar-%%ID%%)" '
                 f'd="M240,240 C440,240 440,{y + 74} {wx - 2},{y + 74}"/>')
    p.append('<g data-k="a3t">' + _txt(400, 300, "3 · tasks out, results back", "mut", "middle", 11) + "</g>")
    for i, y in enumerate(wy):
        p.append(f'<g data-k="w{i}"><rect x="{wx}" y="{y}" width="{ww}" height="{wh}" rx="8" fill="#f6f4ee" '
                 f'stroke="{FADE}" stroke-dasharray="4 3"/>' + _txt(wx + 10, y + 16, f"worker machine {i + 1}", "mut", size=11)
                 + f'<rect class="box" x="{wx + 8}" y="{y + 24}" width="{ww - 16}" height="{wh - 32}" rx="6"/>'
                 + _txt(wx + 18, y + 48, "Executor", "b") + _txt(wx + 18, y + 64, "a JVM process", "mut", size=11)
                 + "".join(f'<rect x="{sx}" y="{sy}" width="58" height="28" rx="4" fill="none" stroke="{FADE}" '
                           f'stroke-dasharray="3 2"/>' + _txt(sx + 29, sy + 42, "core", "mut", "middle", 10)
                           for sx, sy in slots[2 * i: 2 * i + 2]) + "</g>")
    places = {}
    for i in range(8):
        p.append(_chip(f"t{i}", 50, 22, f"task {i}", _TEAL))
        places[(f"t{i}", "q")] = (queue[i][0] - 4, queue[i][1])
        sx, sy = slots[i % 6]
        places[(f"t{i}", "s")] = (sx + 4, sy + 3)
    hidden = ["drv", "dag", "cm", "a1", "a2", "a30", "a31", "a32", "a3t", "w0", "w1", "w2"]
    start = {k: "off" for k in hidden} | {f"t{i}": "off at-q" for i in range(8)}
    waves = {f"t{i}": "chip at-s" for i in range(6)}
    steps = _frames(start, [
        ("<b>The problem.</b> Say Qafila keeps 2 TB of order lines. One core reads Parquet at roughly 200 MB/s, so a "
         "single pass takes about 10,000 seconds: <b>almost 3 hours</b>. Next year there is more data, and the "
         "machine is the same size.", {}),
        ("<b>Spark's answer: split the data, and send the work to many machines.</b> It starts with the "
         "<b>driver</b>: the process that runs your program (here, this notebook). <code>SparkSession</code>, our "
         "<code>spark</code> variable, is your handle on it.", {"single": "off", "drv": ""}),
        ("The driver asks a <b>cluster manager</b> (YARN, Kubernetes, or Spark's own standalone manager) for "
         "resources.", {"cm": "", "a1": ""}),
        ("The cluster manager starts <b>executors</b> on worker machines: processes that do the actual work. Each "
         "has a few <b>cores</b>, and a core runs one <b>task</b> at a time.", {"w0": "", "w1": "", "w2": "", "a2": ""}),
        ("You call an <b>action</b> (<code>count</code>, <code>show</code>, <code>write</code>). The driver turns your "
         "code into a plan, the plan into a <b>job</b>, the job into <b>stages</b> (cut at every shuffle), and each "
         "stage into <b>one task per partition</b> of the data: here, 8 tasks.",
         {"dag": "", "a30": "", "a31": "", "a32": "", "a3t": ""} | {f"t{i}": "chip at-q" for i in range(8)}),
        ("The driver sends the tasks to free cores. <b>One task = one partition on one core.</b> 6 cores run 6 "
         "tasks at once, and the other 2 wait.", waves),
        ("Each core that finishes takes the next task, so 8 tasks on 6 cores run in 2 <b>waves</b>. Results go back "
         "to the driver, or the executors write them straight to storage.",
         {f"t{i}": "chip done off at-s" for i in range(6)} | {"t6": "chip at-s", "t7": "chip at-s"}),
        ("<b>Back to the 2 TB.</b> In 128 MB partitions that is 16,384 tasks. On 400 cores it's 41 waves of under a "
         "second each: <b>under a minute instead of 3 hours</b>, on paper. Real jobs add overhead, and shuffles "
         "(coming up) cost the most.", {"t6": "chip done off at-s", "t7": "chip done off at-s"}),
        (f"<b>In this notebook</b> the master is <code>{master}</code>: the driver and one executor with {cores} cores "
         "run inside a single process on this machine, with no cluster manager. On a real cluster only "
         "<code>.master(...)</code> changes, and the same code runs on 1,000 machines.",
         {"cm": "dim", "a1": "dim", "a2": "dim", "w1": "dim", "w2": "dim", "a31": "dim", "a32": "dim"}),
    ])
    return _stepper(_svg(W, H, "".join(p)), steps, css=_SPARK_CSS + _places(places), speed_ms=speed_ms)


def partitions(parts, cores, speed_ms=1700):
    """One input file, cut into partitions, and the partitions run as tasks on `cores` cores.

    `parts` has one row per partition: `partition` and `count` (rows), e.g. from `groupBy(spark_partition_id())`.
    Tasks go to whichever core is free first, in partition order, and take time in proportion to their rows.
    """
    p = parts.sort_values("partition")
    ids, rows = [int(i) for i in p["partition"]], [int(r) for r in p["count"]]
    total = sum(rows)
    X0, Wd, fy, ch = 90, 790, 40, 32
    s = Wd / total
    lane_y = [118 + j * 46 for j in range(cores)]
    H = lane_y[-1] + 74
    free, sched, x = [0] * cores, [], 0
    for i, r in zip(ids, rows):
        c = min(range(cores), key=lambda j: free[j])
        sched.append((i, r, c, free[c], x))
        free[c] += r
        x += r
    end = max(free)
    out = [_txt(20, fy + 21, "file", "mut")]
    for j, y in enumerate(lane_y):
        out.append(f'<rect x="{X0}" y="{y}" width="{Wd}" height="{ch}" rx="4" fill="#f6f4ee"/>'
                   + _txt(20, y + 21, f"core {j}", "mut"))
    out.append(_txt(X0, lane_y[-1] + ch + 18, "time → (each task takes time in proportion to its rows)", "mut", size=11))
    out.append(f'<rect x="{X0}" y="{fy}" width="{Wd}" height="{ch}" rx="4" fill="none" stroke="{FADE}" '
               'stroke-dasharray="4 3"/>')
    out.append(f'<g data-k="whole"><rect x="{X0}" y="{fy}" width="{Wd}" height="{ch}" rx="4" fill="{FADE}"/>'
               + _txt(X0 + Wd / 2, fy + 21, f"order_items.parquet · {total:,} rows", "b", "middle") + "</g>")
    places = {}
    for i, r, c, t0, x in sched:
        w = r * s
        label = f"P{i} · {r:,} rows" if w > 120 else (f"P{i}" if w > 26 else "")
        out.append(_chip(f"c{i}", max(w - 3, 2), ch, label, _TEAL))
        places[(f"c{i}", "f")] = (X0 + x * s, fy)
        places[(f"c{i}", "l")] = (X0 + t0 * s, lane_y[c])
    ex = X0 + end * s
    out.append(f'<g data-k="end"><line x1="{ex:.0f}" x2="{ex:.0f}" y1="{lane_y[0] - 10}" y2="{lane_y[-1] + ch + 4}" '
               f'stroke="{NAVY}" stroke-width="2" stroke-dasharray="5 3"/>'
               + _txt(ex + 6, lane_y[0] - 2, "stage done", "b", size=11) + "</g>")
    start = {"end": "off"} | {f"c{i}": "chip off at-f" for i in ids}
    changes = [
        (f"One file: <b>order_items.parquet</b>, {total:,} rows. To work on it in parallel, Spark first cuts it "
         "into pieces.", {}),
        (f"Spark cuts it into <b>{len(ids)} partitions</b> (input splits of at most "
         f"<code>maxPartitionBytes</code>): from {min(rows):,} to {max(rows):,} rows each. A partition is the unit "
         "of parallel work.", {"whole": "off"} | {f"c{i}": "chip at-f" for i in ids}),
    ]
    for i, r, c, t0, _ in sched:
        when = "right away" if t0 == 0 else f"as soon as core {c} finishes its previous task"
        changes.append((f"<b>Task {i}</b> reads partition {i} ({r:,} rows) on <b>core {c}</b>, {when}. One partition "
                        "on one core is one task.", {f"c{i}": "chip at-l"}))
    changes.append((f"All {len(ids)} tasks done. The stage takes as long as the busiest core: {end:,} rows of work "
                    f"instead of {total:,} on a single core (<b>{total / end:.1f}× faster</b>). More cores help until "
                    f"every task has its own core, and nothing finishes before the biggest task ({max(rows):,} rows).",
                    {"end": ""}))
    return _stepper(_svg(940, H, "".join(out)), _frames(start, changes), css=_SPARK_CSS + _places(places),
                    speed_ms=speed_ms)


def _explain(df) -> str:
    """The text `df.explain()` prints."""
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    return buf.getvalue()


def lazy(df, transformations, action, jobs_by_transformations, jobs_by_action, result, speed_ms=2400):
    """Transformations add to a plan and run nothing; the action runs it. `df` is the final DataFrame (its plan
    shows the pushed filter); `transformations` are the code of each step, as the notebook wrote them."""
    import html
    import re

    text = _explain(df)
    pushed = re.search(r"PushedFilters: \[([^\]]*)\]", text)
    pushed = [f for f in (pushed[1].split(", ") if pushed else []) if not f.startswith("IsNotNull")]
    cols = re.search(r"FileScan \w+ \[([^\]]*)\]", text)
    cols = re.sub(r"#\d+L?", "", cols[1]).split(",") if cols else []
    names = [(re.findall(r"\.(\w+)\(", t) or ["step"])[0] for t in transformations]
    css = f"""
#%%ID%% .lz {{display: flex; gap: 16px; align-items: stretch; flex-wrap: wrap}}
#%%ID%% .lz-code {{flex: 1 1 360px; font-family: 'DejaVu Sans Mono', Menlo, Consolas, monospace; font-size: 12.5px;
                  border: 1px solid {GRID}; border-radius: 8px; padding: 10px 12px; line-height: 1.9}}
#%%ID%% .lz-l {{white-space: pre; transition: opacity .45s, background .35s; border-left: 3px solid transparent;
                padding-left: 6px}}
#%%ID%% .lz-l.cur {{background: {_tint(_AMBER, 0.15)}; border-left-color: {_AMBER}}}
#%%ID%% .lz-plan {{flex: 0 1 230px; display: flex; flex-direction: column-reverse; gap: 6px; padding: 4px 0}}
#%%ID%% .lz-n {{border: 1.5px dashed {FADE}; border-radius: 6px; padding: 5px 9px; font-size: 12.5px; color: {MUTED};
               transition: all .45s; background: #fff}}
#%%ID%% .lz-n b {{color: {INK}}}
#%%ID%% .lz-n.ran {{border: 1.5px solid {NAVY}; background: {NAVY}; color: #fff}}
#%%ID%% .lz-n.ran b {{color: #fff}}
#%%ID%% .lz-n.ran .lz-s {{display: none}}
#%%ID%% .lz-p {{font-size: 11.5px; transition: opacity .45s}}
#%%ID%% .lz-j {{flex: 0 0 150px; border: 1px solid {GRID}; border-radius: 8px; text-align: center; padding: 10px;
               display: grid; align-content: center}}
#%%ID%% .lz-big {{grid-area: 1 / 1; font-size: 46px; font-weight: bold; color: {NAVY}; transition: opacity .45s}}
#%%ID%% .lz-cap {{grid-area: 2 / 1; font-size: 12px; color: {MUTED}}}
#%%ID%% .off {{opacity: 0}}
"""
    code = ['<div class="lz-l">items = spark.read.parquet(…)</div>']
    code += [f'<div class="lz-l" data-k="l{i}">{html.escape(t)}</div>' for i, t in enumerate(transformations)]
    code.append(f'<div class="lz-l" data-k="la">{html.escape(action)}</div>')
    plan = [f'<div class="lz-n" data-k="scan"><b>read</b> order_items.parquet'
            f'<div class="lz-p" data-k="pp">only {len(cols)} columns · filter pushed into the reader: '
            f'{html.escape(", ".join(pushed) or "none")}</div></div>']
    plan += [f'<div class="lz-n" data-k="p{i}"><b>{html.escape(n)}</b><span class="lz-s"> · planned</span></div>' for i, n in enumerate(names)]
    body = (f'<div class="lz"><div class="lz-code">{"".join(code)}</div><div class="lz-plan">{"".join(plan)}</div>'
            f'<div class="lz-j"><div class="lz-big" data-k="j0">{jobs_by_transformations}</div>'
            f'<div class="lz-big" data-k="j1">{jobs_by_action}</div><div class="lz-cap">Spark jobs run</div></div></div>')
    n = len(transformations)
    start = {f"l{i}": "off" for i in range(n)} | {f"p{i}": "off" for i in range(n)} | {
        "la": "off", "j1": "off", "pp": "off"}
    changes = [("A DataFrame is a <b>recipe</b>, not data. <code>spark.read.parquet</code> only looked at the file's "
                "schema: the plan so far is one step, <i>read the file</i>.", {})]
    for i, name in enumerate(names):
        tail = (f" All {n} transformations ran <b>{jobs_by_transformations} jobs</b>: Spark has only written down the "
                "plan." if i == n - 1 else "")
        changes.append((f"<code>.{html.escape(name)}(…)</code> is a <b>transformation</b>: Spark adds a step to the "
                        "plan and returns at once, without reading a row." + tail,
                        {f"l{j}": "" for j in range(i)} | {f"l{i}": "cur", f"p{i}": ""}))
    changes.append((f"<code>{html.escape(action)}</code> is an <b>action</b>: it needs a real answer, so Spark "
                    f"optimizes the whole plan and runs it: <b>{jobs_by_action} job(s)</b>, result {result:,}.",
                    {f"l{n - 1}": "", "la": "cur", "j0": "off", "j1": "", "scan": "ran"}
                    | {f"p{i}": "ran" for i in range(n)}))
    changes.append(("Why wait? Because Spark sees the <b>whole</b> recipe before it reads anything, its optimizer "
                    f"(Catalyst) reads only the {len(cols)} columns the query uses and hands the filter to the Parquet "
                    "reader, which skips row groups that can't match. You'll see both in the plan below.",
                    {"la": "", "pp": ""}))
    return _stepper(body, _frames(start, changes), css=css, speed_ms=speed_ms)


_PLAN_SAYS = {
    "FileScan": "reads the file(s), only the columns listed. {pushed}Narrow: each task reads its own split.",
    "ColumnarToRow": "Parquet is read in column batches; this turns the batches into rows for the next step.",
    "Filter": "keeps the rows where <code>{detail}</code>. <b>Narrow</b>: each task filters its own partition, no "
              "data moves.",
    "Project": "computes and keeps {detail}. <b>Narrow</b>.",
    "HashAggregate": "{agg}",
    "Exchange": "<b>SHUFFLE</b> (<code>{detail}</code>): every task splits its rows into buckets by the hash of the "
                "key and writes them to local disk; each task of the next stage then fetches its bucket from every "
                "task over the network. <b>Wide</b>, and a <b>stage boundary</b>: the next stage starts when all of "
                "this one is done.",
    "BroadcastExchange": "<b>BROADCAST</b>: the driver collects this whole (small) side and sends a copy to every "
                         "executor.",
    "BroadcastHashJoin": "each task joins its own partition of the big side with the broadcast copy, held as a hash "
                         "table in memory. The big side <b>never moves</b>.",
    "Sort": "sorts each partition {detail}, so the join can merge the two sides.",
    "SortMergeJoin": "both sides arrive shuffled by the join key and sorted: each task walks its two sorted lists "
                     "side by side, like a zipper.",
}


def _plan_tree(text):
    """The physical plan printed by `explain()` as nested dicts: op, detail, children."""
    import re

    lines = text.split("== Physical Plan ==", 1)[-1].strip("\n").split("\n")
    root, stack, adaptive = None, [], False
    for line in lines:
        if not line.strip():
            break
        m = re.match(r"^([ :]*?)([+:]- )(.*)$", line)
        depth, body = (len(m[1]) // 3 + 1, m[3]) if m else (0, line)
        body = re.sub(r"^\*\(\d+\) ", "", body.strip())
        op = re.match(r"[A-Za-z]+", body)[0]
        if op == "AdaptiveSparkPlan":
            adaptive = True
            continue
        depth -= adaptive
        node = {"op": op, "raw": re.sub(r"#\d+L?", "", body[len(op):]).strip(), "children": []}
        while len(stack) > depth:
            stack.pop()
        if stack:
            stack[-1]["children"].append(node)
        else:
            root = node
        stack.append(node)
    return root, adaptive


def _plan_detail(n):
    """(short text for the box, text for the caption) of one plan node."""
    import html
    import re

    e = html.escape
    op, raw = n["op"], n["raw"]
    if op == "FileScan":
        cols = re.search(r"\[([^\]]*)\]", raw)
        cols = cols[1].split(",") if cols else []
        pushed = re.search(r"PushedFilters: \[([^\]]*)\]", raw)
        pushed = [f for f in (pushed[1].split(", ") if pushed and pushed[1] else []) if not f.startswith("IsNotNull")]
        return (f"parquet · {len(cols)} columns" + (" · pushed filter" if pushed else ""),
                f"Pushed to the Parquet reader: <code>{e(', '.join(pushed))}</code>. " if pushed else "")
    if op == "Exchange":
        m = re.match(r"(\w+\([^)]*\))", raw)
        return (m[1] if m else raw), e(m[1] if m else raw)
    if op == "HashAggregate":
        keys = re.search(r"keys=\[([^\]]*)\]", raw)
        fns = re.search(r"functions=\[(.*)\]", raw)
        fns = fns[1] if fns else ""
        k = keys[1] if keys else ""
        if "partial_" in fns:
            say = (f"<b>partial</b> aggregate, before the shuffle: each task pre-computes <code>{e(fns)}</code> per "
                   f"<code>{e(k)}</code> on its own rows, so only one row per key per task has to cross the network.")
        else:
            say = (f"<b>final</b> aggregate, after the shuffle: all rows for one <code>{e(k)}</code> are now in the "
                   f"same task, which combines the partial results: <code>{e(fns)}</code>.")
        return f"by {k} · {fns}", say
    if op in ("BroadcastHashJoin", "SortMergeJoin"):
        keys = re.findall(r"\[([^\]]*)\]", raw)
        kind = re.search(r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti)\b", raw)
        on = f"{keys[0]} = {keys[1]}" if len(keys) >= 2 else ""
        return f"{kind[1] if kind else ''} join on {on}", e(on)
    if op == "Sort":
        by = re.match(r"\[([^\]]*)\]", raw)
        by = re.sub(r" (ASC|DESC) NULLS (FIRST|LAST)", "", by[1]) if by else ""
        return f"by {by}", f"by <code>{e(by)}</code>"
    if op == "Project":
        cols = re.match(r"\[(.*)\]", raw)
        cols = [c.strip() for c in cols[1].split(", ")] if cols else []
        new = [c.split(" AS ")[-1] for c in cols if " AS " in c]
        txt = f"{len(cols)} columns" + (f" · new: {', '.join(new)}" if new else "")
        return txt, e(txt)
    if op == "BroadcastExchange":
        return "copy to every executor", ""
    return raw, e(raw)


def plan(df, speed_ms=2200):
    """The physical plan of `df` (what `df.explain()` prints) as a tree, walked bottom-up one operator at a time.

    Exchanges (shuffles and broadcasts) are stage boundaries: each stage gets a number and a colour.
    """
    import html

    root, adaptive = _plan_tree(_explain(df))
    order, leaves = [], [0]
    BW, BH, GX, GY = 270, 50, 22, 34

    def layout(n, depth, parent=None):
        n["depth"], n["parent"] = depth, parent
        for c in n["children"]:
            layout(c, depth + 1, n)
        if n["children"]:
            n["x"] = sum(c["x"] for c in n["children"]) / len(n["children"])
        else:
            n["x"] = leaves[0] * (BW + GX)
            leaves[0] += 1
        order.append(n)

    layout(root, 0)
    boundary = ("Exchange", "BroadcastExchange")
    stage_roots = [n for n in order if n is root or (n["parent"] and n["parent"]["op"] in boundary)]
    for n in order:
        a = n
        while a not in stage_roots:
            a = a["parent"]
        n["stage"] = None if n["op"] in boundary else stage_roots.index(a) + 1
    depth = max(n["depth"] for n in order)
    W, H = leaves[0] * (BW + GX) - GX + 20, (depth + 1) * (BH + GY) - GY + 20
    y = {n["depth"]: 10 + n["depth"] * (BH + GY) for n in order}
    out = []
    for i, n in enumerate(order):
        n["k"] = f"n{i}"
    for n in order:
        if n["parent"]:
            p = n["parent"]
            x1, y1, x2, y2 = n["x"] + 10 + BW / 2, y[n["depth"]], p["x"] + 10 + BW / 2, y[p["depth"]] + BH
            out.append(f'<path data-k="e{n["k"]}" class="ln" marker-end="url(#ar-%%ID%%)" '
                       f'd="M{x1:.0f},{y1} C{x1:.0f},{y1 - GY / 2:.0f} {x2:.0f},{y2 + GY / 2:.0f} {x2:.0f},{y2 + 1}"/>')
    for n in order:
        x, yy = n["x"] + 10, y[n["depth"]]
        box, _ = _plan_detail(n)
        box = box if len(box) <= 42 else box[:41] + "…"
        if n["op"] in boundary:
            color = _AMBER if n["op"] == "Exchange" else _PURPLE
            tag = "SHUFFLE" if n["op"] == "Exchange" else "BROADCAST"
            out.append(f'<g data-k="{n["k"]}" class="pn"><rect class="box" x="{x}" y="{yy}" width="{BW}" height="{BH}" '
                       f'rx="6" style="fill:{_tint(color, 0.12)}; stroke:{color}; stroke-dasharray:5 3"/>'
                       + _txt(x + 10, yy + 20, n["op"], "b") + _txt(x + BW - 10, yy + 20, tag, "b", "end", 11)
                       + _txt(x + 10, yy + 38, box, "mono mut") + "</g>")
        else:
            c = SERIES[(n["stage"] - 1) % len(SERIES)]
            out.append(f'<g data-k="{n["k"]}" class="pn"><rect class="box" x="{x}" y="{yy}" width="{BW}" height="{BH}" '
                       f'rx="6"/><rect x="{x}" y="{yy}" width="6" height="{BH}" rx="2" fill="{c}"/>'
                       + _txt(x + 16, yy + 20, n["op"], "b") + _txt(x + BW - 10, yy + 20, f"stage {n['stage']}", "", "end", 11)
                       + _txt(x + 16, yy + 38, box, "mono mut") + "</g>")
    css = _SPARK_CSS + f"""
#%%ID%% .pn.done > .box {{stroke: {NAVY}; stroke-width: 2}}
#%%ID%% .pn.cur > .box {{stroke: {_AMBER}; stroke-width: 3.5}}
#%%ID%% .pn.todo {{opacity: .45}}
"""
    for n in order:   # stage colours for the "stage N" labels
        if n["stage"]:
            css += f'#%%ID%% [data-k="{n["k"]}"] text:nth-of-type(2) {{fill: {SERIES[(n["stage"] - 1) % len(SERIES)]}}}\n'
    shuffles = sum(n["op"] == "Exchange" for n in order)
    broadcasts = sum(n["op"] == "BroadcastExchange" for n in order)
    start = {n["k"]: "pn todo" for n in order}
    changes = [("The plan Spark will run, exactly as <code>explain()</code> prints it, drawn as a tree. Data flows "
                "<b>up</b>, so read it from the bottom. Coloured stripes mark the <b>stages</b>.", {})]
    done = {}
    for n in order:
        _, detail = _plan_detail(n)
        says = _PLAN_SAYS.get(n["op"], "<code>{detail}</code>")
        says = says.format(detail=detail, pushed=detail, agg=detail)
        where = "" if n["stage"] is None else f" (stage {n['stage']})"
        changes.append((f"<b>{html.escape(n['op'])}</b>{where}: {says}", done | {n["k"]: "pn cur"}))
        done[n["k"]] = "pn done"
    if shuffles:
        end = (f"{shuffles} shuffle{'s' if shuffles > 1 else ''} → the work is cut into <b>stages</b> at each <code>Exchange</code>. Every task "
               "of a stage runs the operators of that stage back to back on its partition; the shuffle is where data "
               "moves between machines, and usually where the time goes.")
    else:
        end = ("No <code>Exchange</code> anywhere: every operator is <b>narrow</b>, so it is <b>one stage</b> and each "
               "task runs the whole pipeline on its own partition, with no data moving between tasks.")
    if broadcasts:
        end += " The broadcast runs first, as its own small job, so the join can start with the copy in place."
    if adaptive:
        end += (" The whole tree sits inside <code>AdaptiveSparkPlan</code>: with AQE, Spark can still change the "
                "plan while it runs, after it sees the real sizes.")
    changes.append((end, done))
    return _stepper(_svg(W, H, "".join(out)), _frames(start, changes), css=css, speed_ms=speed_ms)


def shuffle(rows, key="product_id", n_out=8, noun="products", speed_ms=2600):
    """A groupBy's shuffle, from the lab's own rows: map tasks pre-aggregate, split their output into `n_out` buckets
    by `pmod(hash(key), n_out)`, and every reducer fetches its bucket from every map task.

    `rows` has one row per (map_task, reducer, key) with `count`: the input rows of that key in that map task.
    """
    maps = sorted(rows.map_task.unique())
    red = list(range(n_out))
    rows_in = rows.groupby("map_task")["count"].sum()
    partial = rows.groupby("map_task").size()
    bucket = rows.groupby(["map_task", "reducer"]).size()
    fetched = rows.groupby("reducer").size().reindex(red, fill_value=0)
    final = rows.groupby("reducer")[key].nunique().reindex(red, fill_value=0)
    RH, top = 44, 46
    H = top + max(len(maps), n_out) * RH + 6
    my = {m: top + i * RH + (n_out - len(maps)) * RH / 2 * (len(maps) < n_out) for i, m in enumerate(maps)}
    ry = {r: top + r * RH + (len(maps) - n_out) * RH / 2 * (n_out < len(maps)) for r in red}
    MX, MW, RX, RW, bh = 20, 300, 600, 280, 36
    vmax = bucket.max()
    out = [_txt(MX, 18, "stage 1 · map tasks", "b"), _txt(RX, 18, "stage 2 · reducers", "b"),
           _txt(460, 18, "network", "mut", "middle"), _txt(460, 34, f"pmod(hash({key}), {n_out})", "mono mut", "middle")]
    for m in maps:
        for r in red:
            v = bucket.get((m, r), 0)
            out.append(f'<path data-k="x{m}_{r}" class="ln" style="stroke-width:{0.6 + 4 * v / vmax:.1f}" '
                       f'd="M{MX + MW},{my[m] + bh / 2:.0f} C460,{my[m] + bh / 2:.0f} 460,{ry[r] + bh / 2:.0f} '
                       f'{RX},{ry[r] + bh / 2:.0f}"/>')
    for m in maps:
        y = my[m]
        out.append(f'<rect class="box" x="{MX}" y="{y:.0f}" width="{MW}" height="{bh}" rx="6"/>'
                   + _txt(MX + 10, y + 15, f"map task {m}", "b")
                   + f'<g data-k="in{m}">' + _txt(MX + 10, y + 30, f"{rows_in[m]:,} rows", "mut", size=11) + "</g>"
                   + f'<g data-k="pa{m}">' + _txt(MX + 10, y + 30, f"{rows_in[m]:,} rows → {partial[m]:,} partial",
                                                  "mut", size=11) + "</g>")
        x = MX + 186
        tot = sum(bucket.get((m, r), 0) for r in red)
        for r in red:
            w = 104 * bucket.get((m, r), 0) / tot
            out.append(f'<rect data-k="sg{m}_{r}" class="sg" x="{x:.1f}" y="{y + 8:.0f}" width="{max(w - 1, 0.5):.1f}" '
                       f'height="20" fill="{FADE if r % 2 else GRID}"/>')
            x += w
    for r in red:
        y = ry[r]
        out.append(f'<g data-k="r{r}"><rect class="box" x="{RX}" y="{y:.0f}" width="{RW}" height="{bh}" rx="6"/>'
                   + _txt(RX + 10, y + 15, f"reducer {r}", "b") + "</g>"
                   + f'<g data-k="rf{r}">' + _txt(RX + 10, y + 30, f"fetches {fetched[r]:,} rows", "mut", size=11) + "</g>"
                   + f'<g data-k="rr{r}">' + _txt(RX + 10, y + 30, f"{fetched[r]:,} rows → {final[r]:,} {noun}",
                                                  "mut", size=11) + "</g>")
    css = _SPARK_CSS + f"#%%ID%% .sg.hl {{fill: {_AMBER}}}\n"
    lines = {f"x{m}_{r}": "ln off" for m in maps for r in red}
    segs = {f"sg{m}_{r}": "sg off" for m in maps for r in red}
    start = lines | segs | {f"pa{m}": "off" for m in maps} | {f"r{r}": "off" for r in red} | {
        f"rf{r}": "off" for r in red} | {f"rr{r}": "off" for r in red}
    r0 = int(fetched.idxmax())
    total_in, total_partial = int(rows_in.sum()), int(partial.sum())
    steps = _frames(start, [
        (f"<b>Stage 1</b>: {len(maps)} map tasks, one per input partition, holding {total_in:,} rows between them. "
         f"Rows for any one {key} are spread over all of them.", {}),
        (f"<b>Partial aggregation</b> first: each task sums its own rows per {key}. {total_in:,} rows shrink to "
         f"{total_partial:,}: one row per {key} per task. (That's the <code>partial_sum</code> in the plan.)",
         {f"in{m}": "off" for m in maps} | {f"pa{m}": "" for m in maps}),
        (f"Each task splits its output into <b>{n_out} buckets</b>, bucket = <code>pmod(hash({key}), {n_out})</code>, "
         "and writes them to its local disk. Same key → same bucket, in every task.", {k: "sg" for k in segs}),
        (f"<b>Stage 2</b> starts only when every map task is done. Reducer {r0} fetches bucket {r0} from "
         f"<b>every</b> map task ({len(maps)} network reads, {fetched[r0]:,} rows).",
         {f"r{r}": "" for r in red} | {f"rf{r}": "" for r in red} | {f"x{m}_{r0}": "ln hl" for m in maps}
         | {f"sg{m}_{r0}": "sg hl" for m in maps}),
        (f"Every reducer does the same: {len(maps)} × {n_out} = <b>{len(maps) * n_out} fetches</b>, everyone to "
         "everyone. That all-to-all copy over the network is the <b>shuffle</b>.",
         {k: "ln on" for k in lines} | {k: "sg" for k in segs}),
        (f"Each reducer now holds every partial row for its keys, and adds them up: one row per {key}, "
         f"{int(final.sum()):,} in total.", {f"rf{r}": "off" for r in red} | {f"rr{r}": "" for r in red}),
        (f"Without the partial sums, all {total_in:,} rows would have crossed the network instead of "
         f"{total_partial:,} (<b>{total_in / total_partial:.0f}× fewer</b>). Spark does this for you on "
         "<code>sum</code>, <code>count</code>, <code>min</code>, <code>max</code>… but not on a plain "
         "<code>repartition</code> or a join.", {k: "ln" for k in lines}),
    ])
    return _stepper(_svg(900, H, "".join(out)), steps, css=css, speed_ms=speed_ms)


def joins(strategy, big_rows, small_rows, big_parts, executors=3, shuffle_partitions=8, big="order_items",
          small="products", key="product_id", speed_ms=2400):
    """A picture of a broadcast join (`strategy="broadcast"`) or a sort-merge join (`"merge"`) on a small cluster,
    with the real table sizes in the captions."""
    merge = strategy == "merge"
    EW, EX = 275, [20 + i * 295 for i in range(executors)]
    H = 380 if merge else 236
    out = ['<g data-k="drv"><rect class="box" x="370" y="8" width="160" height="40" rx="6" style="stroke:'
           f'{NAVY}; stroke-width:2"/>' + _txt(450, 33, "driver", "b", "middle") + "</g>"]
    for i, x in enumerate(EX):
        out.append(f'<rect class="box" x="{x}" y="72" width="{EW}" height="124" rx="8"/>'
                   + _txt(x + 10, 92, f"executor {i + 1}", "b"))
    places = {}
    for p in range(big_parts):
        x = EX[p % executors] + 10 + (p // executors) * 66
        out.append(_chip(f"b{p}", 60, 26, f"P{p}", _TEAL))
        places[(f"b{p}", "h")] = (x, 104)
    out.append(_chip("sm", 74, 26, small, _PURPLE))
    places[("sm", "h")] = (EX[0] + 10, 150)
    places[("sm", "d")] = (413, 15)
    for i in range(executors):
        out.append(_chip(f"bc{i}", 74, 26, small, _PURPLE))
        places[(f"bc{i}", "d")] = (413, 15)
        places[(f"bc{i}", "e")] = (EX[i] + 10, 150)
    if merge:
        rw = (860 - 8 * (shuffle_partitions - 1)) / shuffle_partitions
        for j in range(shuffle_partitions):
            x = 20 + j * (rw + 8)
            for i, ex in enumerate(EX):
                out.append(f'<path data-k="x{i}_{j}" class="ln" d="M{ex + EW / 2:.0f},196 C{ex + EW / 2:.0f},236 '
                           f'{x + rw / 2:.0f},236 {x + rw / 2:.0f},268"/>')
        for j in range(shuffle_partitions):
            x = 20 + j * (rw + 8)
            out.append(f'<g data-k="r{j}"><rect class="box" x="{x:.0f}" y="270" width="{rw:.0f}" height="80" rx="6"/>'
                       + _txt(x + rw / 2, 288, f"reducer {j}", "b", "middle", 11)
                       + f'<rect data-k="rb{j}" x="{x + 8:.0f}" y="298" width="{rw - 16:.0f}" height="20" rx="3" '
                       f'fill="{_TEAL}"/><rect data-k="rs{j}" x="{x + 8:.0f}" y="322" width="{(rw - 16) / 3:.0f}" '
                       f'height="20" rx="3" fill="{_PURPLE}"/></g>')
    out.append(f'<rect x="20" y="{H - 22}" width="12" height="12" rx="2" fill="{_TEAL}"/>'
               + _txt(38, H - 12, f"{big}: {big_rows:,} rows in {big_parts} partitions", "mut", size=11)
               + f'<rect x="330" y="{H - 22}" width="12" height="12" rx="2" fill="{_PURPLE}"/>'
               + _txt(348, H - 12, f"{small}: {small_rows:,} rows", "mut", size=11))
    chips = {f"b{p}": "chip at-h" for p in range(big_parts)}
    start = chips | {"sm": "chip at-h"} | {f"bc{i}": "chip off at-d" for i in range(executors)}
    intro = (f"Join <b>{big}</b> ({big_rows:,} rows in {big_parts} partitions) with <b>{small}</b> ({small_rows:,} "
             f"rows) on <code>{key}</code>, on a cluster of {executors} executors. Matching rows must meet in the "
             "<b>same task</b>. Who moves?")
    if not merge:
        changes = [
            (intro, {}),
            (f"<b>BroadcastExchange</b>, part 1: the driver collects the whole small side, all {small_rows:,} rows.",
             {"sm": "chip at-d"}),
            (f"Part 2: it sends a <b>full copy to every executor</b>: {small_rows:,} × {executors} = "
             f"{small_rows * executors:,} rows over the network, once.",
             {"sm": "chip off at-d"} | {f"bc{i}": "chip at-e" for i in range(executors)}),
            (f"<b>BroadcastHashJoin</b>: each task joins its own partition of {big} with the local copy. Rows of "
             f"{big} that crossed the network: <b>0</b>.", {k: "chip done at-h" for k in chips}),
        ]
    else:
        lines = {f"x{i}_{j}": "ln on" for i in range(executors) for j in range(shuffle_partitions)}
        start |= {k: "ln off" for k in lines} | {f"r{j}": "off" for j in range(shuffle_partitions)} | {
            f"rb{j}": "off" for j in range(shuffle_partitions)} | {f"rs{j}": "off" for j in range(shuffle_partitions)}
        changes = [
            (intro, {}),
            (f"<b>Exchange</b> on {big}: every task sends each row to reducer <code>pmod(hash({key}), "
             f"{shuffle_partitions})</code>, everyone to everyone. <b>All {big_rows:,} rows</b> cross the network.",
             lines | {f"r{j}": "" for j in range(shuffle_partitions)} | {f"rb{j}": "" for j in range(shuffle_partitions)}
             | {k: "chip dim at-h" for k in chips}),
            (f"The same <b>Exchange</b> on {small}: the same hash sends each {key} to the same reducer as its "
             f"matching {big} rows.", {f"rs{j}": "" for j in range(shuffle_partitions)} | {"sm": "chip dim at-h"}),
            (f"<b>Sort</b> both sides inside each reducer by {key}, then <b>SortMergeJoin</b> walks the two sorted "
             "lists together, like a zipper.", {f"r{j}": "hl" for j in range(shuffle_partitions)}),
            (f"Rows over the network: <b>{big_rows + small_rows:,}</b>, against {small_rows * executors:,} with a "
             "broadcast. A sort-merge join is what Spark uses when both sides are too big to copy.",
             {f"r{j}": "" for j in range(shuffle_partitions)} | {k: "ln" for k in lines}),
        ]
    return _stepper(_svg(900, H, "".join(out)), _frames(start, changes), css=_SPARK_CSS + _places(places),
                    speed_ms=speed_ms)


@_figure
def skew(counts, cores, key="city", n=None):
    """Rows per task after a repartition by `key`, and the tasks on `cores` cores with time ∝ rows.

    `counts` has `partition`, `key` and `count`, e.g. from `groupBy(spark_partition_id(), key).count()`.
    """
    n = n or int(counts.partition.max()) + 1
    per = counts.groupby("partition")["count"].sum().reindex(range(n), fill_value=0)
    colors = _colors(counts[key])
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 3.8), gridspec_kw={"width_ratios": [1.3, 1]})
    bottom = pd.Series(0, index=range(n))
    for v, c in colors.items():
        d = counts[counts[key] == v].set_index("partition")["count"].reindex(range(n), fill_value=0)
        a.bar(d.index, d.values, bottom=bottom.values, color=c, width=0.84, label=v)
        for p, cnt in d.items():
            if cnt:
                a.text(p, bottom[p] + cnt / 2, f"{v}\n{cnt:,}", ha="center", va="center", color="white", fontsize=8,
                       weight="bold")
        bottom += d
    for p in range(n):
        if not per[p]:
            a.text(p, per.max() * 0.02, "idle", ha="center", va="bottom", color=MUTED, fontsize=8.5)
    a.set_xticks(range(n), [f"P{p}" for p in range(n)])
    a.set_ylabel("rows")
    a.set_title(f"Rows per task after repartition({n}, '{key}')")
    free, lanes = [0] * cores, []
    for p in range(n):
        c = min(range(cores), key=lambda j: free[j])
        lanes.append((p, c, free[c], per[p]))
        free[c] += per[p]
    for p, c, t0, r in lanes:
        if r:
            b.barh(c, r, left=t0, height=0.55, color=NAVY if r == per.max() else _TEAL)
            b.text(t0 + r / 2, c, f"P{p}", ha="center", va="center", color="white", fontsize=9, weight="bold")
    end, even = max(free), per.sum() / cores
    b.axvline(end, color=BAD, lw=1.6)
    b.text(end, cores - 0.45, "  stage done", color=BAD, fontsize=9, va="center")
    b.axvline(even, color=MUTED, lw=1.2, ls=(0, (4, 3)))
    b.text(even, cores - 0.45, "if the rows were spread evenly  ", color=MUTED, fontsize=9, va="center", ha="right")
    b.set_yticks(range(cores), [f"core {c}" for c in range(cores)])
    b.set_ylim(-0.6, cores - 0.2)
    b.invert_yaxis()
    b.set_xlabel("time → (∝ rows)")
    b.set_title(f"The same {n} tasks on {cores} cores: the stage waits for the biggest task")
    fig.tight_layout()


@_figure
def files(out_dir):
    """Each task of a write produces its own Parquet file: tasks on the left, the files they wrote on the right."""
    import pyarrow.parquet as pq

    paths = sorted(Path(out_dir).glob("*.parquet"))
    rows = [(int(p.name[5:10]), p.name, pq.read_metadata(p).num_rows, p.stat().st_size / 1024) for p in paths]
    fig, ax = plt.subplots(figsize=(9, 0.5 * len(rows) + 0.9))
    for i, (task, name, n, kb) in enumerate(rows):
        ax.add_patch(FancyBboxPatch((0, i - 0.35), 2.2, 0.7, boxstyle="round,pad=0,rounding_size=0.12", fc=_TEAL, lw=0))
        ax.text(1.1, i, f"task {task}", ha="center", va="center", color="white", weight="bold")
        ax.add_patch(FancyArrowPatch((2.3, i), (3.6, i), arrowstyle="-|>", mutation_scale=12, color=MUTED))
        ax.add_patch(FancyBboxPatch((3.7, i - 0.35), 7.6, 0.7, boxstyle="round,pad=0,rounding_size=0.12", fc="white",
                                    ec=FADE))
        ax.text(3.9, i, name[:24] + "…", va="center", family=MONO, fontsize=8.5)
        ax.text(11.1, i, f"{n:,} rows · {kb:,.0f} KB", va="center", ha="right", color=MUTED, fontsize=8.5)
    ax.set_xlim(-0.1, 11.4)
    ax.set_ylim(len(rows) - 0.5, -0.9)
    ax.axis("off")
    ax.set_title(f"{len(rows)} tasks → {len(rows)} files in {Path(out_dir).name}/ (the number in each name is the task)")
