"""Airflow DAG: research PDFs -> Markdown -> local LLM metadata -> SQLite -> HTML + Discord.

Task graph (convert/extract/save/notify are *dynamically mapped*: one task
instance per new PDF, so a failing paper does not block the others):

    scan_new_pdfs
         |
    convert_pdf  (x N)
         |
    extract_metadata  (x N, one at a time so the local LLM is not overloaded)
         |
    save_to_db  (x N)
        /    \\
 notify_discord  render_html
     (x N)       (once, even if there was nothing new)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

try:  # Airflow 3.x
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

import pipeline_utils as pu  # lives next to this file in dags/

log = logging.getLogger(__name__)


@dag(
    dag_id="paper_pipeline",
    description="Turn research PDFs into a searchable study library",
    schedule="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never process the same PDF in two overlapping runs
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["info-h420", "llm"],
)
def paper_pipeline():
    @task
    def scan_new_pdfs() -> list[str]:
        """List PDFs in papers/ that are not yet in the SQLite database."""
        new = pu.find_new_pdfs()
        log.info("Found %d new PDF(s): %s", len(new), new)
        return new

    @task
    def convert_pdf(filename: str) -> dict:
        """PDF -> Markdown (PyMuPDF4LLM). The .md goes to disk; only its path is passed on."""
        return {"filename": filename, "md_path": pu.convert_pdf_to_markdown(filename)}

    @task(retries=2, max_active_tis_per_dag=1)
    def extract_metadata(item: dict) -> dict:
        """Ask the local LLM for title/summary/keywords/category (JSON, retried once)."""
        with open(item["md_path"], encoding="utf-8") as fh:
            markdown = fh.read()
        meta = pu.generate_metadata(markdown)
        meta["filename"] = item["filename"]
        return meta

    @task
    def save_to_db(meta: dict) -> dict:
        """Upsert the paper's metadata into the local SQLite database."""
        pu.save_paper(meta)
        log.info("Saved %s", meta["filename"])
        return meta

    @task
    def notify_discord(meta: dict) -> None:
        """Send one Discord message per processed paper."""
        pu.send_discord(meta)

    # all_done: regenerate the page even when there was nothing new or one paper failed.
    @task(trigger_rule="all_done")
    def render_html() -> str:
        """Rebuild output/index.html (inside the Google Drive folder) from the DB."""
        path = pu.render_library_html()
        log.info("Wrote %s", path)
        return path

    new_files = scan_new_pdfs()
    converted = convert_pdf.expand(filename=new_files)
    described = extract_metadata.expand(item=converted)
    saved = save_to_db.expand(meta=described)

    notify = notify_discord.expand(meta=saved)
    html = render_html()
    saved >> [notify, html]


paper_pipeline()
