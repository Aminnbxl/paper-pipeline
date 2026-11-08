# Research Paper Library: Airflow + local LLM

Airflow pipeline that turns research PDFs into a searchable study library.

PDF -> Markdown (PyMuPDF4LLM) -> metadata JSON from a local LLM (Ollama) -> SQLite -> static HTML page -> Discord notification.

## Pipeline (DAG `paper_pipeline`, runs every 10 minutes)

1. `find_new_pdfs`: lists PDFs in `papers/` that are not yet in the database.
2. `convert_to_markdown`: one mapped task per new PDF.
3. `extract_metadata`: one mapped task per PDF, in two steps:
   - **Condense**: the Markdown is split into chunks of `MAX_CHARS` (6000) characters, each chunk is summarised by the LLM and the summaries are joined. This is repeated until the text fits in one chunk, so the LLM sees the whole document and not only its beginning.
   - **Extract**: the LLM returns title, summary, keywords and category as JSON (`json.loads`, basic field/type check, one retry).

   The category is free: the model reuses a category already stored in the database when one fits, otherwise it creates a new short one. There is no fixed category list.
4. `save_to_db`: stores one row per paper in SQLite (`filename` is unique).
5. `generate_html`: regenerates `output/index.html` (search box + category filter).
6. `notify_discord`: sends a webhook message for each newly processed paper.

The SQLite database is kept locally in `data/papers.db`, outside the Google Drive synced folder. Markdown conversions are cached in `data/markdown/`.

## Requirements

- Linux, WSL2 or macOS (Airflow does not run natively on Windows), Python 3.12, [uv](https://docs.astral.sh/uv/)
- [Ollama](https://ollama.com) with a model, e.g. `ollama pull llama3.2:3b`
- [Google Drive for desktop](https://www.google.com/drive/download/) with a synced folder `ResearchLibrary/` containing `papers/` and `output/`
  - macOS: right-click `ResearchLibrary` in Finder, then **Offline access** > **Available offline** (or use "Mirror files"), otherwise the PDFs may not be on disk
- A Discord webhook URL (channel settings > Integrations > Webhooks)

## Setup

```bash
git clone https://github.com/Aminnbxl/paper-pipeline.git
cd paper-pipeline
uv venv --python 3.12 && source .venv/bin/activate
uv pip install -r requirements.txt
```

Do not add Airflow's official `--constraint` file: it pins a different `openai` version than `requirements.txt` and the install fails with "No solution found".

## Run

1. Start Ollama (the app on macOS, `ollama serve` on Linux/WSL) and check the model: `ollama list`
2. Create `ResearchLibrary/papers/` and `ResearchLibrary/output/` in your synced Google Drive folder and put some PDFs in `papers/`.
3. Copy `start.example.sh` to `start.sh`, then edit it:
   - keep the `LIBRARY_DIR` line that matches your system (WSL2 or macOS) and fix the path if needed
   - put your Discord webhook URL in `DISCORD_WEBHOOK_URL`

   Then run `bash start.sh`. It starts `airflow standalone` with `AIRFLOW_HOME` and the DAGs folder set to this repo.
4. Open http://localhost:8080 and log in as `admin`. The generated password is printed in the terminal at startup, and stored in `airflow/simple_auth_manager_passwords.json.generated`.
5. Un-pause `paper_pipeline` and click **Trigger** (it also runs on its own every 10 minutes).
6. Follow the run in the **Graph** view. When all tasks are green, open `output/index.html` and check your Discord channel.

On macOS, allow the Terminal to access Google Drive if the system asks.

A long PDF takes a few minutes: `extract_metadata` makes one LLM call per chunk.

## Reset (to test from scratch)

Stop Airflow (`Ctrl + C`), then from the repo folder:

```bash
sqlite3 data/papers.db "DELETE FROM papers;"
rm -rf data/markdown
rm -f "$LIBRARY_DIR/output/index.html"
```

The PDFs stay in `papers/` and are processed again at the next run.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `LIBRARY_DIR` | Synced library folder (contains `papers/` and `output/`) | `/mnt/g/Mon Drive/ResearchLibrary` |
| `LLM_BASE_URL` | OpenAI-compatible endpoint | `http://localhost:11434/v1` |
| `LLM_MODEL` | Model name | `llama3.2:3b` |
| `DISCORD_WEBHOOK_URL` | Webhook URL (never commit it) | none (notification skipped) |

`MAX_CHARS` (chunk size) is a constant at the top of `dags/paper_pipeline.py`.

## Troubleshooting

- **`No solution found ... openai` during install**: run `uv pip install -r requirements.txt` without `--constraint`.
- **The DAG does not appear in Airflow**: wait about 30 seconds and refresh. Import errors are shown in a banner on the Dags page.
- **`Connection refused` in `extract_metadata`**: Ollama is not running.
- **`LLM output invalid after retry`**: the model did not return valid JSON twice. Re-run the task or try a larger model with `LLM_MODEL`.
- **Task fails to read a PDF on macOS**: the file is probably not downloaded locally. Set the Drive folder to "Available offline".
