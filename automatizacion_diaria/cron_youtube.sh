#!/bin/bash
# Wrapper para ejecutar el pipeline YouTube.
# Usado por launchd (y opcionalmente cron).

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export LANG="en_US.UTF-8"
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

REPO="/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE"
VENV="$REPO/reto_ml/bin/python"
SCRIPT="$REPO/Clases/RETO/automatizacion_diaria/run_pipeline_youtube.py"

cd "$REPO"
PYTHON_BIN="$VENV" "$VENV" "$SCRIPT"
