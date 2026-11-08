"""Pipeline: PDF -> Markdown -> LLM metadata -> SQLite -> static HTML -> Discord."""
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

import pymupdf4llm
import requests
from airflow.sdk import dag, task
from openai import OpenAI

# ---------- Configuration ----------
LIBRARY_DIR = Path(os.environ.get("LIBRARY_DIR", "/mnt/g/Mon Drive/ResearchLibrary"))
PAPERS_DIR = LIBRARY_DIR / "papers"
OUTPUT_DIR = LIBRARY_DIR / "output"

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"          # local, outside the synced folder
MARKDOWN_DIR = DATA_DIR / "markdown"
DB_PATH = DATA_DIR / "papers.db"

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2:3b")
MAX_CHARS = 6000  # part of the paper sent to the LLM

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;padding:0 1rem;background:#f5f6f8;color:#222}
.bar{display:flex;gap:.5rem;margin-bottom:1rem}
input,select{padding:.6rem;font-size:1rem;border:1px solid #bbb;border-radius:6px}
input{flex:1}
.card{background:#fff;border-radius:8px;padding:1rem 1.2rem;margin-bottom:1rem;box-shadow:0 1px 3px #0002}
.card h2{margin:.2rem 0;font-size:1.2rem}
.meta{color:#666;font-size:.85rem}
.tag{display:inline-block;background:#e6edff;color:#2447a8;border-radius:12px;padding:.1rem .6rem;margin:.2rem .2rem 0 0;font-size:.8rem}
.cat{background:#e3f5e8;color:#1d6b35}
</style></head><body>
<h1>Research Library</h1>
<div class="bar">
  <input id="q" placeholder="Search title, summary, keywords...">
  <select id="cat"><option value="">All categories</option></select>
</div>
<p id="count" class="meta"></p>
<div id="list"></div>
<script>
const papers = __DATA__;
const q = document.getElementById("q"), cat = document.getElementById("cat");
const list = document.getElementById("list"), count = document.getElementById("count");
[...new Set(papers.map(p => p.category))].sort().forEach(c => {
  const o = document.createElement("option"); o.value = o.textContent = c; cat.appendChild(o);
});
function el(tag, text, cls) {
  const e = document.createElement(tag); e.textContent = text;
  if (cls) e.className = cls; return e;
}
function render() {
  const t = q.value.toLowerCase();
  list.replaceChildren(); let n = 0;
  papers.forEach(p => {
    const hay = (p.title + " " + p.summary + " " + p.keywords.join(" ")).toLowerCase();
    if ((cat.value && p.category !== cat.value) || !hay.includes(t)) return;
    n++;
    const card = document.createElement("div"); card.className = "card";
    card.appendChild(el("h2", p.title));
    card.appendChild(el("div", p.filename, "meta"));
    card.appendChild(el("p", p.summary));
    card.appendChild(el("span", p.category, "tag cat"));
    p.keywords.forEach(k => card.appendChild(el("span", k, "tag")));
    list.appendChild(card);
  });
  count.textContent = n + " paper(s)";
}
q.addEventListener("input", render); cat.addEventListener("change", render);
render();
</script></body></html>
"""


# ---------- Helpers ----------
def get_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE NOT NULL,
            title TEXT, summary TEXT, keywords TEXT, category TEXT)"""
    )
    return conn


def llm_text(client, prompt):
    """Plain-text LLM call (no JSON), used for chunk summaries."""
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()


def condense(client, text):
    """Shrink a long text by summarising it chunk by chunk, until it fits in MAX_CHARS."""
    for _ in range(5):  # safety: at most 5 passes
        if len(text) <= MAX_CHARS:
            break
        chunks = [text[i:i + MAX_CHARS] for i in range(0, len(text), MAX_CHARS)]
        print(f"Condensing {len(chunks)} chunk(s)")
        summaries = [
            llm_text(client, "Summarise this passage of a document in 3-4 sentences:\n\n" + c)
            for c in chunks
        ]
        text = "\n\n".join(summaries)
    return text[:MAX_CHARS]


def check_metadata(data):
    """Basic check: required fields exist with the expected types."""
    if not isinstance(data, dict):
        raise ValueError("JSON root is not an object")
    for field in ("title", "summary", "category"):
        if not isinstance(data.get(field), str) or not data[field].strip():
            raise ValueError(f"Missing or invalid field: {field}")
    kw = data.get("keywords")
    if not isinstance(kw, list) or not all(isinstance(k, str) for k in kw):
        raise ValueError("Missing or invalid field: keywords")


def known_categories():
    """Categories already used in the database."""
    with closing(get_db()) as conn:
        return sorted({r[0] for r in conn.execute("SELECT DISTINCT category FROM papers")})


# ---------- Tasks ----------
@task
def find_new_pdfs():
    """Return PDFs in papers/ that are not yet in the database."""
    with closing(get_db()) as conn:
        known = {row[0] for row in conn.execute("SELECT filename FROM papers")}
    new = sorted(p.name for p in PAPERS_DIR.glob("*.pdf") if p.name not in known)
    print(f"{len(new)} new PDF(s): {new}")
    return new


@task
def convert_to_markdown(filename):
    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    md = pymupdf4llm.to_markdown(str(PAPERS_DIR / filename))
    md_path = MARKDOWN_DIR / (Path(filename).stem + ".md")
    md_path.write_text(md, encoding="utf-8")
    print(f"{filename}: {len(md)} characters of Markdown")
    return {"filename": filename, "markdown_path": str(md_path)}


@task
def extract_metadata(item):
    """Ask the local LLM for title/summary/keywords/category as JSON (1 retry)."""
    full_text = Path(item["markdown_path"]).read_text(encoding="utf-8")
    client = OpenAI(base_url=LLM_BASE_URL, api_key="ollama")
    text = condense(client, full_text)
    existing = known_categories()
    messages = [
        {"role": "system", "content": (
            "You analyse research papers. Reply ONLY with a JSON object with exactly "
            'these keys: "title" (string), "summary" (string, 2-3 sentences), '
            '"keywords" (list of 3-6 short strings), "category" (string, a short '
            "topic label of 1 to 3 words, Title Case). Write all fields in English."
            + (f"Reuse one of these existing categories if one clearly fits: "
               f"{', '.join(existing)}. Otherwise create a new one." if existing else ""))},
        {"role": "user", "content": "Paper content:\n\n" + text},
    ]
    last_error = None
    for attempt in (1, 2):
        try:
            resp = client.chat.completions.create(
                model=LLM_MODEL, messages=messages,
                response_format={"type": "json_object"}, temperature=0.2,
            )
            data = json.loads(resp.choices[0].message.content)
            check_metadata(data)
            return {
                "filename": item["filename"],
                "title": data["title"].strip(),
                "summary": data["summary"].strip(),
                "keywords": data["keywords"],
                "category": data["category"].strip(),
            }
        except (json.JSONDecodeError, ValueError) as e:
            last_error = e
            print(f"Attempt {attempt} failed for {item['filename']}: {e}")
    raise RuntimeError(f"LLM output invalid after retry: {last_error}")


@task
def save_to_db(meta):
    with closing(get_db()) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO papers (filename, title, summary, keywords, category) "
            "VALUES (?, ?, ?, ?, ?)",
            (meta["filename"], meta["title"], meta["summary"],
             json.dumps(meta["keywords"]), meta["category"]),
        )
        conn.commit()
    return meta


@task
def generate_html(saved):
    """Regenerate the static page from the whole database."""
    with closing(get_db()) as conn:
        rows = conn.execute(
            "SELECT filename, title, summary, keywords, category FROM papers ORDER BY title"
        ).fetchall()
    papers = [
        {"filename": r[0], "title": r[1], "summary": r[2],
         "keywords": json.loads(r[3]), "category": r[4]}
        for r in rows
    ]
    data_js = json.dumps(papers).replace("</", "<\\/")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / "index.html"
    out.write_text(HTML_TEMPLATE.replace("__DATA__", data_js), encoding="utf-8")
    print(f"Wrote {out} ({len(papers)} papers)")
    return str(out)


@task
def notify_discord(item):
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        print("DISCORD_WEBHOOK_URL not set, skipping notification")
        return
    content = (
        f"📄 **New paper processed:** {item['title']}\n"
        f"**Category:** {item['category']}\n"
        f"**Keywords:** {', '.join(item['keywords'])}\n"
        f"{item['summary']}"
    )[:1900]
    r = requests.post(url, json={"content": content}, timeout=10)
    r.raise_for_status()


# ---------- DAG ----------
@dag(
    dag_id="paper_pipeline",
    schedule="*/10 * * * *",          # every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["research", "llm"],
)
def paper_pipeline():
    new_files = find_new_pdfs()
    converted = convert_to_markdown.expand(filename=new_files)
    metadata = extract_metadata.expand(item=converted)
    saved = save_to_db.expand(meta=metadata)
    html = generate_html(saved)
    notify = notify_discord.expand(item=saved)
    html >> notify


paper_pipeline()
