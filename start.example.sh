#!/bin/bash
# Copy this file to start.sh, then set LIBRARY_DIR and your Discord webhook URL.
# start.sh is ignored by git: never commit your webhook URL.
cd "$(dirname "$0")" || exit 1
PROJECT_DIR="$PWD"

# WSL2 only: mount the Google Drive letter
if grep -qi microsoft /proc/version 2>/dev/null; then
  sudo mount -t drvfs G: /mnt/g 2>/dev/null
fi

source .venv/bin/activate
export AIRFLOW_HOME="$PROJECT_DIR/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES   # macOS only, harmless elsewhere

# Synced Google Drive folder (must contain papers/ and output/). Keep ONE of these lines:
export LIBRARY_DIR="/mnt/g/Mon Drive/ResearchLibrary"                                                  # WSL2
# export LIBRARY_DIR="$HOME/Library/CloudStorage/GoogleDrive-you@gmail.com/My Drive/ResearchLibrary"  # macOS

# Optional: local LLM (defaults: Ollama on localhost, llama3.2:3b)
# export LLM_BASE_URL="http://localhost:11434/v1"
# export LLM_MODEL="llama3.2:3b"

export DISCORD_WEBHOOK_URL='your_webhook_url_here'
airflow standalone
