# Research Paper Library: Airflow + local LLM

Airflow pipeline that turns research PDFs into a searchable study library.

PDF -> Markdown (PyMuPDF4LLM) -> metadata JSON from a local LLM (Ollama) -> SQLite -> static HTML page -> Discord notification.

## Pipeline (DAG `paper_pipeline`, runs every 10 minutes)

1. `find_new_pdfs`: lists PDFs in `papers/` that are not yet in the database.
2. `convert_to_markdown`: one mapped task per new PDF.
3. `extract_metadata`: asks the local LLM for title, summary, keywords and category as JSON (`json.loads`, basic field/type check, one retry).
4. `save_to_db`: stores one row per paper in SQLite (`filename` is unique).
5. `generate_html`: regenerates `output/index.html` (search box + category filter).
6. `notify_discord`: sends a webhook message for each newly processed paper.

The SQLite database is kept locally in `data/papers.db`, outside the Google Drive synced folder.

## Requirements

- Linux or WSL2 (Airflow does not run natively on Windows), Python 3.12
- [Ollama](https://ollama.com) with a model, e.g. `ollama pull llama3.2:3b`
- A Google Drive synced folder `ResearchLibrary/` with `papers/` and `output/`
- A Discord webhook URL

## Setup

```bash
git clone https://github.com/Aminnbxl/paper-pipeline.git
cd paper-pipeline
uv venv --python 3.12 && source .venv/bin/activate
uv pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt"
```

If the constraints install fails, simply run `uv pip install -r requirements.txt`.

## Run

1. Start Ollama and check the model: `ollama list`
2. (WSL only) mount the Drive letter: `sudo mount -t drvfs G: /mnt/g`
3. Copy `start.example.sh` to `start.sh`, put your Discord webhook URL in it, then run `bash start.sh`.
   It starts `airflow standalone` with:
   - `AIRFLOW__CORE__DAGS_FOLDER` pointing to this repo's `dags/`
   - `LIBRARY_DIR` (optional) to change the library path (default `/mnt/g/Mon Drive/ResearchLibrary`)
   - `LLM_BASE_URL` / `LLM_MODEL` (optional, default Ollama `http://localhost:11434/v1` and `llama3.2:3b`)
4. Open http://localhost:8080 and log in (the generated password is printed in the terminal at startup).
5. Un-pause `paper_pipeline`, drop a PDF in `papers/` and click **Trigger**.
6. Open `output/index.html` and check your Discord channel.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `LIBRARY_DIR` | Synced library folder | `/mnt/g/Mon Drive/ResearchLibrary` |
| `LLM_BASE_URL` | OpenAI-compatible endpoint | `http://localhost:11434/v1` |
| `LLM_MODEL` | Model name | `llama3.2:3b` |
| `DISCORD_WEBHOOK_URL` | Webhook URL (never commit it) | none (notification skipped) |
