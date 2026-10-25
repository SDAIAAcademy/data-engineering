"""Export the executed tool walkthroughs to read-only HTML pages: solutions/tools/<tool>.ipynb → tools/html/<tool>.html.

    uv run python _tools/export_html.py            # every walkthrough
    uv run python _tools/export_html.py dbt        # one

The pages are for reading without running (or when Colab is slow), so they keep every output and drop the
instructor notes (markdown cells starting with 🎓) and DuckDB's progress bars (widgets with no saved state). Local
paths in the outputs (C:\\Users\\..., /mnt/c/..., /home/...) are shortened to `labs/`. Execute a solution notebook first if its outputs are stale.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import nbformat
from nbconvert import HTMLExporter

LABS = Path(__file__).resolve().parents[1]
SOLUTIONS = LABS / "solutions" / "tools"
OUT = LABS / "tools" / "html"

# any absolute path that ends in .../data-engineering/labs, in Windows (also JSON-escaped) or POSIX form
LOCAL_LABS = re.compile(r"(?:[A-Za-z]:|/mnt/[a-z]|/home/[^/\s\"']+|/Users/[^/\s\"']+|/content)"
                        r"(?:[\\/]{1,4}[^\\/\s\"'<>]+)*?[\\/]{1,4}data-engineering[\\/]{1,4}labs")
LOCAL_LABS_URL = re.compile(r"[A-Za-z]%3A(?:%5C|%2F)(?:[^%\s\"'<>]+(?:%5C|%2F))*?data-engineering(?:%5C|%2F)labs")
HOME = re.compile(r"[A-Za-z]:(?:\\{1,4}|/)Users(?:\\{1,4}|/)[^\\/\s\"'<>]+|/home/[^/\s\"'<>]+")   # anything else: ~


COLAB = "https://colab.research.google.com/github/SDAIAAcademy/data-engineering/blob/main/labs/"


def scrub(text: str) -> str:
    return HOME.sub("~", LOCAL_LABS_URL.sub("labs", LOCAL_LABS.sub("labs", text)))


def colab_links(html: str) -> str:
    """Links like ../D2_1_bronze_to_silver.ipynb are written for tools/; from tools/html/ they open Colab instead."""
    def to_colab(m):
        target = (Path("tools") / m.group(2)).as_posix()
        parts = []
        for part in target.split("/"):
            if part == "..":
                parts.pop()
            elif part != ".":
                parts.append(part)
        return f'{m.group(1)}"{COLAB}{"/".join(parts)}"'
    return re.sub(r'(href=)"(?!https?:|#)([^"]+\.ipynb)"', to_colab, html)


def export(tool: str) -> Path:
    nb = nbformat.read(SOLUTIONS / f"{tool}.ipynb", as_version=4)
    nb.cells = [c for c in nb.cells if not (c.cell_type == "markdown" and c.source.lstrip().startswith("🎓"))]
    nb.metadata.pop("widgets", None)
    for c in nb.cells:
        if c.cell_type == "code":
            c.outputs = [o for o in c.outputs if "application/vnd.jupyter.widget-view+json" not in o.get("data", {})]
    exporter = HTMLExporter(template_name="lab")
    body, _ = exporter.from_notebook_node(nb, resources={"metadata": {"name": f"{tool} walkthrough"}})
    OUT.mkdir(parents=True, exist_ok=True)
    out = OUT / f"{tool}.html"
    out.write_text(colab_links(scrub(body)), encoding="utf-8", newline="\n")
    print(f"wrote {out.relative_to(LABS)} ({out.stat().st_size / 1e6:.1f} MB)")
    return out


if __name__ == "__main__":
    tools = sys.argv[1:] or sorted(p.stem for p in SOLUTIONS.glob("*.ipynb"))
    for t in tools:
        export(t)
