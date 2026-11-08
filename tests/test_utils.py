"""Run with:  pytest -q   (from the project root). No Airflow, LLM or network needed."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
import pipeline_utils as pu  # noqa: E402

GOOD = {
    "title": "A Paper",
    "summary": "It does things.",
    "keywords": ["a", "b"],
    "category": "Data Engineering",
}


class FakeClient:
    """Mimics openai.OpenAI().chat.completions.create, returning queued replies."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = 0
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls += 1
        content = self.replies.pop(0)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


def test_parse_handles_code_fence():
    assert pu.parse_llm_json("```json\n" + json.dumps(GOOD) + "\n```") == GOOD


def test_validate_rejects_bad_types():
    with pytest.raises(ValueError):
        pu.validate_metadata({**GOOD, "keywords": "not a list"})
    with pytest.raises(ValueError):
        pu.validate_metadata({k: v for k, v in GOOD.items() if k != "title"})


def test_generate_metadata_retries_once_then_succeeds():
    client = FakeClient(["not json at all", json.dumps(GOOD)])
    assert pu.generate_metadata("text", client=client)["title"] == "A Paper"
    assert client.calls == 2


def test_generate_metadata_gives_up_after_two_attempts():
    client = FakeClient(["nope", "still nope"])
    with pytest.raises(ValueError):
        pu.generate_metadata("text", client=client)
    assert client.calls == 2


def test_db_roundtrip_and_new_pdf_detection(tmp_path, monkeypatch):
    papers = tmp_path / "papers"
    papers.mkdir()
    (papers / "a.pdf").write_bytes(b"%PDF-1.4")
    (papers / "b.PDF").write_bytes(b"%PDF-1.4")
    (papers / "notes.txt").write_text("ignore me")
    db = tmp_path / "db" / "papers.db"

    assert pu.find_new_pdfs(papers, db) == ["a.pdf", "b.PDF"]
    pu.save_paper({**GOOD, "filename": "a.pdf"}, db)
    assert pu.find_new_pdfs(papers, db) == ["b.PDF"]

    # saving the same filename again updates instead of duplicating
    pu.save_paper({**GOOD, "filename": "a.pdf", "title": "Renamed"}, db)
    rows = pu.load_papers(db)
    assert len(rows) == 1 and rows[0]["title"] == "Renamed" and rows[0]["keywords"] == ["a", "b"]


def test_render_html_embeds_data_and_escapes_script_end(tmp_path):
    db = tmp_path / "papers.db"
    pu.save_paper({**GOOD, "filename": "x.pdf", "title": "</script><b>evil</b>"}, db)
    out = Path(pu.render_library_html(db, tmp_path / "output"))
    html = out.read_text(encoding="utf-8")
    assert out.name == "index.html"
    assert "x.pdf" in html and "Data Engineering" in html
    assert "</script><b>" not in html  # cannot break out of the inline <script>


def test_discord_skipped_without_webhook(monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert pu.send_discord({**GOOD, "filename": "x.pdf"}) is False
