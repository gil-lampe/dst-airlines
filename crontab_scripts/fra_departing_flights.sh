#!/bin/bash
CONDA_ENV_NAME="dst_airlines"
PYTHON_SCRIPT_PATH="/home/gil/projects/DST-Airlines/crontab_scripts/fra_departing_flights.py"
MINICONDA_PATH="/home/gil/miniconda3/etc/profile.d/conda.sh"

source "$MINICONDA_PATH"
conda activate "$CONDA_ENV_NAME"
python3 "$PYTHON_SCRIPT_PATH"
conda deactivate