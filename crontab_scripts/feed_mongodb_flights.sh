#!/bin/bash
VENV_PATH="/home/gil/projects/DST-Airlines/venv/venv_main/bin/activate"
PYTHON_SCRIPT_PATH="/home/gil/projects/DST-Airlines/crontab_scripts/feed_mongodb_flights.py"

source "$VENV_PATH"
python3 "$PYTHON_SCRIPT_PATH"
deactivate