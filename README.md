# Research Paper Library Pipeline (INFO-H420, Assignment 3)

**Authors:** Muhammad Mubasher Saif, Derrick Foko

**Tested with:** Apache Airflow 3.4.0 (`airflow standalone`), Ollama 0.40.15 serving `llama3.2` (3.2B, Q4_K_M), PyMuPDF4LLM, Python `openai` client.

An Apache Airflow DAG that turns research PDFs in a Google Drive-synced folder into a
searchable study library, using a **locally served LLM**.

```
papers/*.pdf --> Markdown --> local LLM (JSON) --> SQLite --> static index.html --> Discord
```

## Architecture

```
scan_new_pdfs -> convert_pdf (xN) -> extract_metadata (xN) -> save_to_db (xN) -+-> notify_discord (xN)
                                                                               +-> render_html (x1)
```

| Task | What it does |
|---|---|
| `scan_new_pdfs` | Lists `papers/*.pdf` and keeps those whose filename is not yet in SQLite |
| `convert_pdf` | PDF -> Markdown with PyMuPDF4LLM; `.md` saved locally, only its path is passed on |
| `extract_metadata` | Calls the local LLM (OpenAI-compatible API, JSON mode) for title, summary, keywords, category. `json.loads`, check fields/types, **retry once** on bad output |
| `save_to_db` | Upserts into SQLite (`filename` UNIQUE, keywords stored as a JSON string) |
| `notify_discord` | Posts one webhook message per new paper |
| `render_html` | Regenerates `output/index.html` (search box + category filter) from the DB |

`convert_pdf`, `extract_metadata`, `save_to_db` and `notify_discord` use **dynamic task mapping**
(one task instance per new PDF), so one broken PDF does not stop the others; it is simply
retried on the next scheduled run because it is still not in the database.
`render_html` uses `trigger_rule="all_done"` so the page is rebuilt even if nothing was new.
The DAG runs every 10 minutes (`*/10 * * * *`) with `max_active_runs=1`.

Code layout: `dags/paper_pipeline.py` (DAG wiring) and `dags/pipeline_utils.py` (all logic, unit-tested).

## Folder layout

```
Google Drive/ResearchLibrary/        <- synced by Google Drive
├── papers/   paper.pdf ...          <- you drop PDFs here
└── output/   index.html             <- generated
This repository (local, NOT synced)
├── dags/paper_pipeline.py, pipeline_utils.py
├── data/papers.db                   <- SQLite lives here, outside Google Drive
└── tests/
```

## Run instructions

**Prerequisites:** a Python version supported by your Airflow release (Linux/macOS/WSL2), Google Drive for desktop, and either
[Ollama](https://ollama.com) or [LM Studio](https://lmstudio.ai).

1. **Install** (Airflow needs its constraints file; adjust the versions to yours):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   AIRFLOW_VERSION=3.4.0; PY=$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
   pip install "apache-airflow==${AIRFLOW_VERSION}" \
     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
   pip install pymupdf4llm openai requests pytest
   ```
   The DAG was developed and tested on Airflow 3.x (it imports from `airflow.sdk`). For Airflow 2.x it falls back to `airflow.decorators`, but that path was not tested.
2. **Start the local LLM**, e.g. with Ollama: `ollama pull llama3.2 && ollama serve`
   (LM Studio: load a model, then *Developer > Start Server*).
3. **Create the folders** `ResearchLibrary/papers` and `ResearchLibrary/output` in Google Drive, and put a few PDFs in `papers/`.
4. **Configure**: `cp .env.example .env`, edit it (library path, model, Discord webhook), then
   `set -a; source .env; set +a`.
5. **Start Airflow**: `airflow standalone`. The admin password is printed in the terminal
   (or in `airflow_home/simple_auth_manager_passwords.json.generated` on Airflow 3).
6. Open <http://localhost:8080>, unpause **paper_pipeline**, and trigger it (or wait for the schedule).
7. Open `ResearchLibrary/output/index.html` in a browser. Drop another PDF into `papers/`
   and the next run adds it, updates the page, and pings Discord.

Run the unit tests (no Airflow, LLM or network needed): `pytest -q`

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `RESEARCH_LIBRARY_DIR` | `~/Google Drive/ResearchLibrary` | Synced folder holding `papers/` and `output/` |
| `LLM_BASE_URL` | `http://localhost:11434/v1` | OpenAI-compatible endpoint (LM Studio: `http://localhost:1234/v1`) |
| `LLM_MODEL` | `llama3.2` | Model name as served locally |
| `LLM_API_KEY` | `ollama` | Any non-empty string for local servers |
| `DISCORD_WEBHOOK_URL` | empty | If empty, notifications are skipped with a warning |
| `DB_PATH` | `./data/papers.db` | SQLite file (keep it outside Google Drive) |

## Evidence of execution

Real run results (Airflow task states, Ollama model metadata, SQLite rows, screenshots) are in
[`docs/evidence/`](docs/evidence/EVIDENCE.md) and `docs/shot_*.png`. The report is `docs/report.pdf`.

## Notes and limitations

- Only the first ~12,000 characters of each paper are sent to the LLM (small local context windows).
- If the server rejects `response_format=json_object`, the code falls back to a plain request and relies on the JSON parser/retry.
- Scanned PDFs without a text layer fail at conversion (no OCR).
- Filenames are assumed unique (they are the database key).
- The webhook URL is a secret: keep it in `.env` (git-ignored), never commit it.
