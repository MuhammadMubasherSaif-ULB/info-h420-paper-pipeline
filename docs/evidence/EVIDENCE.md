# Evidence of execution (8 November 2026)

All files in this folder come from real runs on the team's machine.
Times in the Airflow metadata are UTC; the Airflow UI and screenshots show Europe/Paris (UTC+1).

## Environment

- Apache Airflow 3.4.0, `airflow standalone`
- Ollama 0.40.15, model `llama3.2:latest` (3.2B parameters, Q4_K_M, GGUF); full `/api/show` response in `ollama_model_metadata.json`
- PyMuPDF4LLM for PDF to Markdown; Python `openai` client against the local OpenAI-compatible endpoint

## Runs

The first two runs used a local demo copy of the library folder (`local_demo_library`); the third run used the Google Drive-synced folder.

| Run | Purpose | Observed result |
|---|---|---|
| `manual__codex_live_20261108T135055Z` | Duplicate-run check | Success. `convert_pdf`, `extract_metadata`, `save_to_db`, `notify_discord` skipped (nothing new); `scan_new_pdfs` and `render_html` succeeded; database still held 1 known record. |
| `manual__codex_newfile_20261108T135438Z` | New-file check | Success, about 65 s. A second PDF was converted, described by the LLM (about 54 s), saved, and the page regenerated; database went from 1 to 2 records. |
| `manual__codex_drivesync_20261108T142500Z` | Run with three further PDFs in the Google Drive-synced `ResearchLibrary/papers` folder (https://drive.google.com/drive/u/0/folders/1w574K7Nj-hlWkswR0RV8XLOhHTexfpDn; private, access on request) | Success, 4 min 18 s. All six tasks green; `convert_pdf`, `extract_metadata`, `save_to_db` each mapped over 3 papers (see `../shot_graph.png`). Database then held 5 records. |

## Files

| File | Content |
|---|---|
| `live_run_evidence.json` | Task states and timestamps for the first two runs |
| `ollama_model_metadata.json` | Raw Ollama response for the selected model |
| `sqlite_records_evidence.json` | All 5 rows read from the SQLite database |
| `live_evidence_preview.html` | Viewer for the model, the first two database rows and the first two runs (open together with the two JSON files above) |
| `../shot_graph.png` | Airflow graph view of the Drive run, all tasks green |
| `../shot_discord_tasks.png` | `notify_discord` task instances (mapped, state Success) |

## Known limits of this evidence

- A green `notify_discord` task shows that Airflow ran the task. Delivery of the message is shown only by the Discord channel screenshot, if included in the report.
- `live_evidence_preview.html` and `live_run_evidence.json` were produced after the first two runs, so they list 2 papers; `sqlite_records_evidence.json` is the later state with 5.
- Papers 3 to 5 (environmental modelling) were assigned the category "Data Engineering" because the prompt's category list had no environmental option. Paper 5's summary contains a literal `<sup>` tag from the PDF text; the HTML page escapes it, so it is displayed as text.
