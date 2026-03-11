#!/bin/bash
# Script helper para ejecutar etiquetar_local.py con el entorno virtual correcto

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/../../../X_Mensajes/venv/bin/python3"

cd "$SCRIPT_DIR"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: No se encontró el entorno virtual en: $VENV_PYTHON"
    echo "   Asegúrate de que el venv existe en X_Mensajes/venv"
    exit 1
fi

echo "🔧 Usando Python: $VENV_PYTHON"
echo "📁 Directorio: $SCRIPT_DIR"
echo ""

"$VENV_PYTHON" etiquetar_local.py "$@"

