"""The course's sessions, in one list: writes the session tables of index.html and labs/README.md.

    uv run python _tools/sessions.py

Each table sits between two marker comments (`<!-- sessions:begin -->` ... `<!-- sessions:end -->` in index.html,
`<!-- notebooks:begin -->` ... `<!-- notebooks:end -->` in labs/README.md); everything outside them is hand-written.
"""

from __future__ import annotations

import re
from pathlib import Path

LABS = Path(__file__).resolve().parents[1]
ROOT = LABS.parent
COLAB = "https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/"
PAGES = "https://sdaiaacademy.github.io/data-engineering/"

DAYS = {
    1: ("Store & Query", "teal"),
    2: ("Ingest & Clean", "cyan"),
    3: ("Model & Test", "coral"),
    4: ("Scale & Production", "purple"),
}

SESSIONS = [
    dict(id="D1_1", title="The job and the platform", topics="OLTP vs. OLAP, warehouse, lake, lakehouse, ELT, bronze/silver/gold",
         lab="D1_1_platform_tour"),
    dict(id="D1_2", title="Analytical SQL with DuckDB", topics="rows vs. columns, querying files in place, profiling",
         lab="D1_2_duckdb_sql"),
    dict(id="D1_3", title="Window functions", topics="PARTITION BY, ROW_NUMBER and dedupe, LAG/LEAD, running totals",
         lab="D1_3_sql_windows"),
    dict(id="D1_4", title="Files for analytics", topics="CSV vs. Parquet, statistics, sorting to skip, partitions",
         lab="D1_4_files_parquet"),
    dict(id="D2_1", title="Batch ingestion by hand", topics="pagination, retries, full vs. incremental, cursors, idempotency",
         lab="D2_1_api_ingestion"),
    dict(id="D2_2", title="Ingestion with dlt", topics="resources, write dispositions, incremental cursors, new columns",
         lab="D2_2_dlt_pipeline"),
    dict(id="D2_3", title="Changes and history", topics="change feeds, latest per key, MERGE, deletes, SCD Type 2",
         lab="D2_3_cdc_merge_scd"),
    dict(id="D2_4", title="Bronze to silver with Polars", topics="expressions, timestamp formats, duplicates, time zones",
         lab="D2_4_bronze_to_silver"),
    dict(id="D3_1", title="Dimensional modeling", topics="metric definitions, grain, facts and dimensions, star, fan-out",
         lab="D3_1_dimensional_modeling"),
    dict(id="D3_2", title="dbt", topics="sources, ref, staging to marts, views vs. tables, lineage", lab="D3_2_dbt_project"),
    dict(id="D3_3", title="Data quality", topics="dbt tests, warn vs. error, Pandera, quarantine", lab="D3_3_data_quality"),
    dict(id="D3_4", title="Orchestration with Dagster", topics="assets, schedules, blocking checks, freshness and volume",
         lab="D3_4_orchestration_dagster"),
    dict(id="D4_1", title="Lakehouse with Iceberg", topics="atomic commits, snapshots, time travel, many engines",
         lab="D4_1_iceberg_lakehouse"),
    dict(id="D4_2", title="Spark", topics="do you need it? driver and executors, lazy plans, shuffles", lab="D4_2_spark",
         runs_on="Colab"),
    dict(id="D4_3", title="Streaming", topics="logs and offsets, idempotency, event time, windows, watermarks",
         lab="D4_3_streaming_fundamentals"),
    dict(id="D4_4", title="Governance + capstone kickoff", topics="PDPL, NDMO levels, finding PII, pseudonymizing, masked views",
         lab="D4_4_governance_privacy"),
]

# optional reading: (notebook under labs/, what it is, the session it goes deeper than)
WALKTHROUGHS = [
    ("tools/duckdb", "DuckDB, an analytical database in a library", "1.2"),
    ("tools/sql_windows", "SQL window functions, step by step", "1.3"),
    ("tools/dlt", "dlt, from a hand-written loader to an incremental pipeline", "2.2"),
    ("tools/polars", "Polars, one idea at a time", "2.4"),
    ("tools/dbt", "dbt, from an empty folder to a tested report", "3.2, 3.3"),
    ("tools/pandera", "Pandera, checks at the door", "3.3"),
    ("tools/dagster", "Dagster, assets on a schedule", "3.4"),
    ("tools/iceberg", "Iceberg, open table formats on a lightboard", "4.1"),
    ("tools/spark", "Spark, live and visual", "4.2"),
    ("demos/streaming", "A stream you can see: logs, offsets, windows", "4.3"),
    ("demos/stream_to_lakehouse", "Beyond the course: streams into a lakehouse", "4.3"),
]


def esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def a(href: str, text: str) -> str:
    return f'<a href="{href}">{text}</a>'


def html_day(day: int) -> str:
    title, colour = DAYS[day]
    rows = ["      <tr><th>Session</th><th>Lab notebook (opens in Colab)</th></tr>"]
    for s in [s for s in SESSIONS if s["id"].startswith(f"D{day}_")]:
        n = s["id"][-1]
        note = f' ({s["runs_on"]})' if "runs_on" in s else ""
        rows.append(f'      <tr><td><b>{n} · {esc(s["title"])}</b>: {esc(s["topics"])}</td>'
                    f'<td>{a(COLAB + s["lab"] + ".ipynb", s["lab"])}{note}</td></tr>')
    return (f'  <div class="day">\n'
            f'    <div class="day-head" style="--c:var(--{colour})"><div><div class="t">Day {day}</div><h3>{esc(title)}</h3></div>'
            f'<a class="btn" href="slides/day{day}.html">Open slides →</a></div>\n'
            f'    <div class="scroll"><table>\n' + "\n".join(rows) + "\n    </table></div>\n  </div>")


def html_walkthroughs() -> str:
    rows = ["      <tr><th>Walkthrough</th><th>Goes deeper than</th><th>Open</th></tr>"]
    for nb, what, session in WALKTHROUGHS:
        read = f' · {a("labs/tools/html/" + nb.split("/")[1] + ".html", "read")}' if nb.startswith("tools/") else ""
        rows.append(f'      <tr><td>{esc(what)}</td><td>Session {session}</td><td>{a(COLAB + nb + ".ipynb", "Colab")}{read}</td></tr>')
    return '    <div class="scroll"><table>\n' + "\n".join(rows) + "\n    </table></div>"


def markdown_rows() -> str:
    lines = ["| Session | Lab | Slides | Runs on |", "|---|---|---|---|"]
    for s in SESSIONS:
        day = s["id"][1]
        lines.append(f'| {day}.{s["id"][-1]} · {s["title"]} | [`{s["lab"]}.ipynb`]({COLAB}{s["lab"]}.ipynb) | '
                     f'[Day {day}]({PAGES}slides/day{day}.html) | {s.get("runs_on", "CPU")} |')
    return "\n".join(lines)


def replace_between(path: Path, begin: str, end: str, content: str) -> None:
    text = path.read_bytes().decode("utf-8")   # not read_text: it would turn CRLF into LF
    pattern = re.compile(re.escape(begin) + r".*?" + re.escape(end), re.S)
    assert len(pattern.findall(text)) == 1, f"markers not found once in {path}"
    eol = "\r\n" if "\r\n" in text else "\n"
    block = (begin + "\n" + content + "\n" + end).replace("\r\n", "\n").replace("\n", eol)
    path.write_bytes(pattern.sub(lambda _: block, text).encode("utf-8"))
    print("wrote", path.relative_to(ROOT))


if __name__ == "__main__":
    replace_between(ROOT / "index.html", "<!-- sessions:begin -->", "<!-- sessions:end -->",
                    "\n\n".join(html_day(d) for d in DAYS))
    replace_between(ROOT / "index.html", "<!-- walkthroughs:begin -->", "<!-- walkthroughs:end -->", html_walkthroughs())
    replace_between(LABS / "README.md", "<!-- notebooks:begin -->", "<!-- notebooks:end -->", markdown_rows())
