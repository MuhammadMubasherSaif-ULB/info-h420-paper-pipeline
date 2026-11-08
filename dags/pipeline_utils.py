"""Helper functions for the paper pipeline.

All the real logic lives here so it can be unit-tested without Airflow.
The DAG (paper_pipeline.py) only wires these functions into tasks.

Configuration is read from environment variables (see .env.example).
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CATEGORIES = [
    "Data Engineering",
    "Machine Learning",
    "Responsible AI",
    "Process Modeling",
    "Databases",
    "Natural Language Processing",
    "Computer Vision",
    "Software Engineering",
    "Other",
]

# Keep the prompt small enough for local models with modest context windows.
MAX_MARKDOWN_CHARS = 12_000


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def get_config() -> dict:
    """Read configuration from the environment (evaluated at call time)."""
    library = Path(
        os.environ.get("RESEARCH_LIBRARY_DIR", "~/Google Drive/ResearchLibrary")
    ).expanduser()
    return {
        "papers_dir": Path(os.environ.get("PAPERS_DIR", library / "papers")).expanduser(),
        "output_dir": Path(os.environ.get("OUTPUT_DIR", library / "output")).expanduser(),
        # DB and Markdown cache stay LOCAL, outside the Google Drive folder.
        "db_path": Path(os.environ.get("DB_PATH", PROJECT_ROOT / "data" / "papers.db")).expanduser(),
        "markdown_dir": Path(
            os.environ.get("MARKDOWN_DIR", PROJECT_ROOT / "data" / "markdown")
        ).expanduser(),
        "llm_base_url": os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1"),
        "llm_api_key": os.environ.get("LLM_API_KEY", "ollama"),
        "llm_model": os.environ.get("LLM_MODEL", "llama3.2"),
        "discord_webhook_url": os.environ.get("DISCORD_WEBHOOK_URL", ""),
    }


# --------------------------------------------------------------------------- #
# SQLite
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE TABLE IF NOT EXISTS papers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT NOT NULL UNIQUE,
    title        TEXT NOT NULL,
    summary      TEXT NOT NULL,
    keywords     TEXT NOT NULL,          -- JSON-encoded list of strings
    category     TEXT NOT NULL,
    processed_at TEXT NOT NULL           -- ISO-8601 UTC timestamp
);
"""


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    db_path = Path(db_path or get_config()["db_path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA)
    return conn


def known_filenames(db_path: Path | None = None) -> set[str]:
    with get_connection(db_path) as conn:
        return {r["filename"] for r in conn.execute("SELECT filename FROM papers")}


def save_paper(meta: dict, db_path: Path | None = None) -> None:
    """Insert (or update) one paper. `filename` is the unique key."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO papers (filename, title, summary, keywords, category, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(filename) DO UPDATE SET
                title=excluded.title,
                summary=excluded.summary,
                keywords=excluded.keywords,
                category=excluded.category,
                processed_at=excluded.processed_at
            """,
            (
                meta["filename"],
                meta["title"],
                meta["summary"],
                json.dumps(meta["keywords"]),
                meta["category"],
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ),
        )


def load_papers(db_path: Path | None = None) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute("SELECT * FROM papers ORDER BY title COLLATE NOCASE").fetchall()
    papers = []
    for r in rows:
        d = dict(r)
        d["keywords"] = json.loads(d["keywords"])
        papers.append(d)
    return papers


# --------------------------------------------------------------------------- #
# PDF discovery and conversion
# --------------------------------------------------------------------------- #
def find_new_pdfs(papers_dir: Path | None = None, db_path: Path | None = None) -> list[str]:
    """Return filenames of PDFs in papers/ that are not yet in the database."""
    papers_dir = Path(papers_dir or get_config()["papers_dir"])
    if not papers_dir.is_dir():
        raise FileNotFoundError(
            f"Papers folder not found: {papers_dir}. Set RESEARCH_LIBRARY_DIR or PAPERS_DIR."
        )
    on_disk = sorted(p.name for p in papers_dir.iterdir() if p.suffix.lower() == ".pdf")
    done = known_filenames(db_path)
    return [f for f in on_disk if f not in done]


def convert_pdf_to_markdown(filename: str) -> str:
    """Convert one PDF to Markdown with PyMuPDF4LLM; return path of the .md file."""
    import pymupdf4llm  # imported lazily so unit tests do not need it

    cfg = get_config()
    pdf_path = cfg["papers_dir"] / filename
    md_text = pymupdf4llm.to_markdown(str(pdf_path))
    if not md_text.strip():
        raise ValueError(f"No text could be extracted from {filename} (scanned PDF?)")
    cfg["markdown_dir"].mkdir(parents=True, exist_ok=True)
    md_path = cfg["markdown_dir"] / (Path(filename).stem + ".md")
    md_path.write_text(md_text, encoding="utf-8")
    return str(md_path)


# --------------------------------------------------------------------------- #
# LLM
# --------------------------------------------------------------------------- #
def build_messages(markdown: str) -> list[dict]:
    system = (
        "You are a research librarian. Read the paper and reply with ONE JSON object "
        "and nothing else. The object must have exactly these keys:\n"
        '  "title": string, the paper title,\n'
        '  "summary": string, 2-4 sentences,\n'
        '  "keywords": array of 3-8 short strings,\n'
        '  "category": string, one of: ' + ", ".join(CATEGORIES) + "."
    )
    user = "Paper (Markdown, possibly truncated):\n\n" + markdown[:MAX_MARKDOWN_CHARS]
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def parse_llm_json(text: str) -> dict:
    """json.loads, tolerating a ```json fenced block around the answer."""
    text = text.strip()
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    return json.loads(text)


def validate_metadata(data: object) -> dict:
    """Check required fields and basic types (kept deliberately light)."""
    if not isinstance(data, dict):
        raise ValueError("LLM output is not a JSON object")
    for key in ("title", "summary", "category"):
        if not isinstance(data.get(key), str) or not data[key].strip():
            raise ValueError(f"Field '{key}' missing or not a non-empty string")
    kw = data.get("keywords")
    if not isinstance(kw, list) or not all(isinstance(k, str) for k in kw):
        raise ValueError("Field 'keywords' missing or not a list of strings")
    return {
        "title": data["title"].strip(),
        "summary": data["summary"].strip(),
        "keywords": [k.strip() for k in kw if k.strip()],
        "category": data["category"].strip(),
    }


def _complete(client, model: str, messages: list[dict]) -> str:
    """Call the chat endpoint; prefer JSON mode, fall back if the server rejects it."""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
    except Exception as exc:  # server/model without JSON-mode support
        log.warning("JSON mode failed (%s); retrying without response_format", exc)
        resp = client.chat.completions.create(model=model, messages=messages, temperature=0)
    return resp.choices[0].message.content or ""


def generate_metadata(markdown: str, client=None, attempts: int = 2) -> dict:
    """Ask the local LLM for title/summary/keywords/category. Retry once on bad JSON."""
    cfg = get_config()
    if client is None:
        from openai import OpenAI

        client = OpenAI(base_url=cfg["llm_base_url"], api_key=cfg["llm_api_key"])

    messages = build_messages(markdown)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        raw = _complete(client, cfg["llm_model"], messages)
        try:
            return validate_metadata(parse_llm_json(raw))
        except (ValueError, json.JSONDecodeError) as exc:  # JSONDecodeError is a ValueError
            last_error = exc
            log.warning("Attempt %d/%d: invalid LLM output (%s)", attempt, attempts, exc)
    raise ValueError(f"LLM did not return valid JSON after {attempts} attempts: {last_error}")


# --------------------------------------------------------------------------- #
# Static HTML
# --------------------------------------------------------------------------- #
HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }
  h1 { margin-bottom: .2rem; }
  .meta { color: #666; font-size: .85rem; margin-bottom: 1rem; }
  .controls { display: flex; gap: .5rem; margin-bottom: 1rem; }
  input, select { padding: .5rem; font-size: 1rem; border: 1px solid #bbb; border-radius: 6px; }
  input { flex: 1; }
  .card { border: 1px solid #ddd; border-radius: 8px; padding: 1rem; margin-bottom: .8rem; }
  .card h2 { margin: 0 0 .3rem; font-size: 1.1rem; }
  .card a { color: #0b57d0; text-decoration: none; }
  .tag { display: inline-block; background: #eef2ff; border-radius: 12px; padding: 1px 9px; margin: 2px 4px 0 0; font-size: .8rem; }
  .cat { background: #e6f4ea; }
  @media (prefers-color-scheme: dark) {
    body { background: #121212; color: #eee; } .card { border-color: #333; }
    .tag { background: #26304f; } .cat { background: #1e3a2a; } .card a { color: #8ab4f8; }
    input, select { background: #1e1e1e; color: #eee; border-color: #444; } .meta { color: #aaa; }
  }
</style>
</head>
<body>
<h1>Research Library</h1>
<div class="meta" id="meta"></div>
<div class="controls">
  <input id="q" type="search" placeholder="Search title, summary, keywords...">
  <select id="cat"><option value="">All categories</option></select>
</div>
<div id="list"></div>
<script>
const PAPERS = __DATA__;
const UPDATED = "__UPDATED__";
const q = document.getElementById("q"), cat = document.getElementById("cat");
const list = document.getElementById("list"), meta = document.getElementById("meta");

[...new Set(PAPERS.map(p => p.category))].sort().forEach(c => {
  const o = document.createElement("option"); o.value = c; o.textContent = c; cat.appendChild(o);
});

function tag(text, extra) {
  const s = document.createElement("span"); s.className = "tag" + (extra ? " " + extra : ""); s.textContent = text; return s;
}

function render() {
  const term = q.value.trim().toLowerCase(), c = cat.value;
  const hits = PAPERS.filter(p =>
    (!c || p.category === c) &&
    (!term || (p.title + " " + p.summary + " " + p.keywords.join(" ")).toLowerCase().includes(term)));
  meta.textContent = hits.length + " of " + PAPERS.length + " papers - generated " + UPDATED;
  list.replaceChildren(...hits.map(p => {
    const card = document.createElement("div"); card.className = "card";
    const h = document.createElement("h2"), a = document.createElement("a");
    a.href = "../papers/" + encodeURIComponent(p.filename); a.textContent = p.title; h.appendChild(a);
    const s = document.createElement("p"); s.textContent = p.summary;
    const t = document.createElement("div"); t.appendChild(tag(p.category, "cat"));
    p.keywords.forEach(k => t.appendChild(tag(k)));
    card.append(h, s, t); return card;
  }));
}
q.addEventListener("input", render); cat.addEventListener("change", render); render();
</script>
</body>
</html>
"""


def render_library_html(db_path: Path | None = None, output_dir: Path | None = None) -> str:
    """Regenerate output/index.html from the database; return its path."""
    cfg = get_config()
    output_dir = Path(output_dir or cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    data = json.dumps(load_papers(db_path), ensure_ascii=False).replace("</", "<\\/")
    updated = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = HTML_TEMPLATE.replace("__DATA__", data).replace("__UPDATED__", updated)

    target = output_dir / "index.html"
    tmp = target.with_suffix(".html.tmp")  # write-then-rename so Drive never syncs a half file
    tmp.write_text(html, encoding="utf-8")
    tmp.replace(target)
    return str(target)


# --------------------------------------------------------------------------- #
# Discord
# --------------------------------------------------------------------------- #
def send_discord(meta: dict, webhook_url: str | None = None) -> bool:
    """Post a short message to a Discord webhook. Returns False if no webhook is set."""
    url = webhook_url if webhook_url is not None else get_config()["discord_webhook_url"]
    if not url:
        log.warning("DISCORD_WEBHOOK_URL not set; skipping notification")
        return False

    import requests  # installed with Airflow; imported lazily

    summary = meta["summary"]
    if len(summary) > 500:
        summary = summary[:497] + "..."
    content = (
        f"**New paper processed:** {meta['title']}\n"
        f"*Category:* {meta['category']} | *Keywords:* {', '.join(meta['keywords'])}\n"
        f"{summary}\n`{meta['filename']}`"
    )
    resp = requests.post(url, json={"content": content[:1900]}, timeout=15)
    resp.raise_for_status()
    return True
