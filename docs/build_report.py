"""Builds docs/report.pdf. Edit the TEAM / GITHUB constants, add screenshots, re-run:
    python docs/build_report.py
"""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

TEAM = "[Name 1], [Name 2], [Name 3]  (Group [N])"
DRIVE = "https://drive.google.com/drive/u/0/folders/1w574K7Nj-hlWkswR0RV8XLOhHTexfpDn"
GITHUB = "https://github.com/[your-username]/[your-repo]"
SHOTS = {  # PNGs in docs/. Missing files show a red placeholder in the report.
    "Figure 1. Airflow graph view of the run on the Drive library: all six tasks succeeded; "
    "convert_pdf, extract_metadata and save_to_db were mapped over 3 papers (run duration 4 min 18 s).": "docs/shot_graph.png",
    "Figure 2. notify_discord task instances (one per paper, state Success).": "docs/shot_discord_tasks.png",
    "Figure 3. Discord channel showing the received notifications.": "docs/shot_discord.png",
    "Figure 4. Generated index.html filtered with search \"Autoencoder\" and category \"Machine Learning\".": "docs/shot_html.png",
    "Figure 5. Google Drive folder (ResearchLibrary) with papers/ and output/index.html.": "docs/shot_drive.png",
}

ss = getSampleStyleSheet()
B = ParagraphStyle("B", parent=ss["BodyText"], fontSize=10, leading=13.5, spaceAfter=5)
H1 = ParagraphStyle("H1", parent=ss["Heading1"], fontSize=13, spaceBefore=10, spaceAfter=4)
T = ParagraphStyle("T", parent=ss["Title"], fontSize=18, spaceAfter=2)
CELL = ParagraphStyle("CELL", parent=B, fontSize=8.5, leading=11, spaceAfter=0)
PH = ParagraphStyle("PH", parent=B, textColor=colors.HexColor("#b00020"), alignment=1)
GREY = colors.HexColor("#444444")


def box(d, x, y, w, h, title, sub, fill):
    d.add(Rect(x, y, w, h, rx=4, ry=4, fillColor=fill, strokeColor=GREY, strokeWidth=0.8))
    d.add(String(x + w / 2, y + h / 2 + 3, title, textAnchor="middle", fontSize=8, fontName="Helvetica-Bold"))
    d.add(String(x + w / 2, y + h / 2 - 8, sub, textAnchor="middle", fontSize=7, fontName="Helvetica"))


def arrow(d, x1, y1, x2, y2):
    d.add(Line(x1, y1, x2, y2, strokeColor=GREY, strokeWidth=1))
    d.add(Polygon([x2, y2, x2 - 5, y2 + 3, x2 - 5, y2 - 3], fillColor=GREY, strokeColor=None))


def dag_drawing():
    d = Drawing(480, 120)
    blue, green, orange = colors.HexColor("#dbe7ff"), colors.HexColor("#dff3e3"), colors.HexColor("#ffe9cc")
    y, w, h = 52, 84, 36
    stages = [
        (0, "scan_new_pdfs", "1 task", blue),
        (96, "convert_pdf", "mapped x N", green),
        (192, "extract_metadata", "mapped x N (LLM)", orange),
        (288, "save_to_db", "mapped x N", green),
    ]
    for x, t, s, f in stages:
        box(d, x, y, w, h, t, s, f)
    for (x1, *_), (x2, *_) in zip(stages[:-1], stages[1:]):
        arrow(d, x1 + w, y + h / 2, x2, y + h / 2)
    # fan-out from save_to_db to the two final tasks
    fx = 288 + w + 12
    d.add(Line(288 + w, y + h / 2, fx, y + h / 2, strokeColor=GREY, strokeWidth=1))
    d.add(Line(fx, 92, fx, 28, strokeColor=GREY, strokeWidth=1))
    arrow(d, fx, 92, 392, 92)
    arrow(d, fx, 28, 392, 28)
    box(d, 392, 76, 88, 32, "notify_discord", "mapped x N", blue)
    box(d, 392, 12, 88, 32, "render_html", "1 task, all_done", blue)
    return d


def table(rows, widths):
    data = [[Paragraph(c, CELL) for c in r] for r in rows]
    t = Table(data, colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eaf6")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#bbbbbb")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def p(text, style=B):
    return Paragraph(text, style)


story = [
    p("Assignment 3: Workflows with Airflow", T),
    p("<b>INFO-H420 Process Modeling, Data Pipelines, and Responsible AI</b>", B),
    p(f"<b>Team:</b> {TEAM}<br/><b>Code repository:</b> {GITHUB}", B),

    p("1. Goal and approach", H1),
    p("We built one scheduled Airflow DAG, <font face='Courier'>paper_pipeline</font>, that turns research PDFs "
      "dropped into a Google Drive-synced <font face='Courier'>papers/</font> folder into a searchable study library. "
      "For each new PDF the DAG converts it to Markdown, asks a locally served LLM for a title, summary, keywords "
      "and category as JSON, stores the result in a local SQLite database, regenerates a static "
      "<font face='Courier'>index.html</font>, and sends a Discord notification. Airflow runs locally with "
      "<font face='Courier'>airflow standalone</font>; the SQLite file stays on the local machine, outside the synced folder.", B),

    p("2. DAG design and task dependencies", H1),
    dag_drawing(),
    p("We used <b>dynamic task mapping</b>: <font face='Courier'>scan_new_pdfs</font> returns the list of new "
      "filenames and the next tasks are expanded once per PDF. A paper that fails (for example a scanned PDF "
      "or an LLM error) does not block the others, and because it is never written to the database it is picked up "
      "again on the next run. <font face='Courier'>extract_metadata</font> is limited to one running instance "
      "(<font face='Courier'>max_active_tis_per_dag=1</font>) so the local LLM is not overloaded, and "
      "<font face='Courier'>render_html</font> uses <font face='Courier'>trigger_rule=\"all_done\"</font> so the page "
      "is rebuilt even when nothing was new. The DAG runs every 10 minutes with <font face='Courier'>catchup=False</font> "
      "and <font face='Courier'>max_active_runs=1</font> to avoid processing a file twice.", B),

    p("3. Implementation", H1),
    table([
        ["Step", "Implementation"],
        ["Find new PDFs", "List <font face='Courier'>papers/*.pdf</font>, subtract filenames already in SQLite (filename is the unique key)."],
        ["PDF to Markdown", "PyMuPDF4LLM <font face='Courier'>to_markdown()</font>. The .md file is written locally and only its path is passed via XCom (keeps XComs small)."],
        ["Local LLM", "Python <font face='Courier'>openai</font> client pointed at Ollama (<font face='Courier'>localhost:11434/v1</font>) or LM Studio. JSON mode (<font face='Courier'>response_format=json_object</font>) is used, with a fallback to a plain request if the server rejects it. Input is truncated to about 12,000 characters. The category is chosen from a fixed list in the prompt."],
        ["JSON handling", "<font face='Courier'>json.loads</font> (tolerating a Markdown code fence), then a light check that title/summary/category are non-empty strings and keywords is a list of strings. On failure the call is <b>retried once</b>; after the second failure the task fails (and Airflow retries the task)."],
        ["SQLite", "Built-in <font face='Courier'>sqlite3</font>, table <font face='Courier'>papers</font> (filename UNIQUE, title, summary, keywords as JSON string, category, processed_at). Upsert on filename."],
        ["Static HTML", "Single self-contained <font face='Courier'>index.html</font> with the data embedded as JSON, a search box and a category dropdown (plain JavaScript, text inserted via <font face='Courier'>textContent</font>). Written to a temp file then renamed so Drive never syncs a half-written page."],
        ["Discord", "One webhook POST per new paper (title, category, keywords, short summary). The URL comes from an environment variable and is never committed."],
    ], [3.2 * cm, 13.8 * cm]),
    Spacer(1, 6),
    p("All logic lives in <font face='Courier'>dags/pipeline_utils.py</font> and the DAG file only wires it into tasks, "
      "so the logic (JSON retry, validation, new-file detection, DB upsert, HTML generation) is covered by unit tests "
      "that run without Airflow, an LLM or network access.", B),

    p("4. Evidence that the workflow runs", H1),
    p("The pipeline was run locally with Airflow 3.4.0 (<font face='Courier'>airflow standalone</font>), Ollama 0.40.15 serving "
      "<font face='Courier'>llama3.2:latest</font> (3.2B parameters, Q4_K_M) and PyMuPDF4LLM. The raw model metadata, database rows "
      "and run states are in <font face='Courier'>docs/evidence/</font> of the repository.", B),
    table([
        ["Run / check", "Observed result"],
        ["Duplicate run (nothing new)", "First tests used a local demo copy of the library folder. Run succeeded. <font face='Courier'>scan_new_pdfs</font> found no new file, so convert, extract, save and notify were <b>skipped</b>; "
         "<font face='Courier'>render_html</font> still succeeded. The database kept its 1 known record."],
        ["New file added", "A second PDF was converted, described by the LLM, saved and rendered in about 65 s (LLM step about 54 s). The database went from 1 to 2 records."],
        ["Run on the Drive library", "Three further PDFs, kept in the Google Drive-synced <font face='Courier'>ResearchLibrary/papers</font> folder "
         f"(<font face='Courier'>{DRIVE}</font>), were processed in one run (4 min 18 s). All six tasks succeeded and the three per-paper tasks were mapped x3 (Figure 1). "
         "The database then held 5 records, and <font face='Courier'>output/index.html</font> was written back into the synced folder."],
        ["JSON from the model", "All 5 papers returned valid JSON with title, summary, keywords and category; no task failed. The records are in <font face='Courier'>sqlite_records_evidence.json</font>."],
        ["Discord", "Three <font face='Courier'>notify_discord</font> tasks (one per paper) completed successfully (Figure 2). Delivery in the channel is shown in Figure 3."],
        ["Search and filter", "The generated page filtered by search \"Autoencoder\" and category \"Machine Learning\" returned exactly one paper (Figure 4)."],
        ["Time zones", "Timestamps in the database and run metadata are UTC; the Airflow UI shows Europe/Paris (UTC+1)."],
    ], [4.0 * cm, 13.0 * cm]),
    Spacer(1, 6),
    p("<b>Example records produced by the pipeline</b>", B),
    table([
        ["File", "Title (LLM)", "Category"],
        ["2410.05889.pdf", "Deep Learning-Based Fault Identification in Condition Monitoring", "Machine Learning"],
        ["2101.11539.pdf", "Autoencoder-based Condition Monitoring and Anomaly Detection Method for Rotating Machines", "Machine Learning"],
        ["A-generic-framework-...pdf", "A generic framework for scenario exercises using models applied to water-resource management", "Data Engineering"],
        ["A-GIS-tool-...pdf", "A GIS tool for modeling anthropogenic noise propagation in natural ecosystems", "Data Engineering"],
        ["A-dynamic-model-...pdf", "A dynamic model for optimally phasing in CO2 capture and storage infrastructure", "Data Engineering"],
    ], [3.6 * cm, 10.4 * cm, 3.0 * cm]),
    Spacer(1, 8),
]

for caption, path in SHOTS.items():
    if os.path.exists(path):
        img = Image(path)
        img.drawHeight = img.drawHeight * min(1, 15 * cm / img.drawWidth)
        img.drawWidth = min(img.drawWidth, 15 * cm)
        story += [img, p(f"<i>{caption}</i>", ParagraphStyle("cap", parent=B, fontSize=8.5, alignment=1))]
    else:
        story += [p(f"[INSERT SCREENSHOT: {caption}  (save as {path})]", PH)]

story += [
    p("5. Limitations", H1),
    p("Only the start of each paper is sent to the LLM, so summaries of long papers reflect the introduction and early sections. "
      "The category list in the prompt is generic: the three environmental-modelling papers were all labelled \"Data Engineering\" "
      "because no better option existed, which shows how model output depends on the prompt and on the 3B model's size. "
      "One summary contains a literal <font face='Courier'>&lt;sup&gt;</font> tag copied from the PDF text; the page escapes it, so it appears as text. "
      "Scanned PDFs without a text layer are not supported (no OCR), and filenames are assumed to be unique. "
      "Seven unit tests (<font face='Courier'>tests/test_utils.py</font>) cover JSON validation and retry, new-file detection, the database upsert, "
      "HTML escaping and the missing-webhook case; the LLM and Discord calls are tested only through the live runs above.", B),
    p("6. Reproducing", H1),
    p(f"Install steps, configuration and run instructions are in the README of the repository: {GITHUB}", B),
]

os.makedirs("docs", exist_ok=True)
SimpleDocTemplate("docs/report.pdf", pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                  topMargin=1.8 * cm, bottomMargin=1.8 * cm,
                  title="INFO-H420 Assignment 3 Report").build(story)
print("wrote docs/report.pdf")
