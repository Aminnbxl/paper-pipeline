#!/bin/bash
# Copy this file to start.sh and put your own Discord webhook URL below.
sudo mount -t drvfs G: /mnt/g 2>/dev/null   # WSL only: mount the Google Drive letter
cd ~/paper-pipeline
source .venv/bin/activate
export AIRFLOW_HOME=~/paper-pipeline/airflow
export AIRFLOW__CORE__DAGS_FOLDER=~/paper-pipeline/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export DISCORD_WEBHOOK_URL='your_webhook_url_here'
airflow standalone
