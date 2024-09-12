#!/bin/bash
PYTHON_SCRIPT_PATH="/home/gil/projects/DST-Airlines/crontab_scripts/weather_from_flights.py"
VENV_PATH="/home/gil/projects/DST-Airlines/venv/venv_main/bin/activate"

source "$VENV_PATH"
python3 "$PYTHON_SCRIPT_PATH"
deactivate