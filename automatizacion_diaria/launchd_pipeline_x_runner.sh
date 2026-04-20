#!/usr/bin/env bash
# Runner para launchd: ejecuta el pipeline X (wrapper) con un entorno estable.
#
# Motivo: en algunos macOS, invocar directamente el Python del venv desde launchd
# provoca: "OSError: [Errno 11] Resource deadlock avoided" al importar el módulo site.
# Este script fija HOME/PATH y limpia variables que suelen interferir.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# automatizacion_diaria -> RETO -> Clases -> raíz del workspace (MASTER DATA SCIENCE)
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

VENV_PY="$REPO_ROOT/Clases/RETO/X_Mensajes/venv/bin/python3"
WRAPPER_PY="$SCRIPT_DIR/run_pipeline_wrapper.py"

export PATH="${PATH:-/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}"
export HOME="${HOME:-$(eval echo "~$(id -un)")}"
unset PYTHONHOME

# Evita interacción rara site/venv bajo launchd (Python 3.11+)
export PYTHONSAFEPATH="${PYTHONSAFEPATH:-1}"

cd "$REPO_ROOT"

if [[ ! -x "$VENV_PY" ]]; then
  echo "launchd_pipeline_x_runner: no existe o no es ejecutable: $VENV_PY" >&2
  exit 78
fi

exec "$VENV_PY" -u "$WRAPPER_PY" --catch-up
