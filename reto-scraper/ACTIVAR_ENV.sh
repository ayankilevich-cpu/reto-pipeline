#!/bin/bash
# Script para activar el entorno virtual de reto-scraper

# Cambiar al directorio del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Activar entorno virtual
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "✓ Entorno virtual 'reto-scraper' activado"
    echo "  Directorio: $SCRIPT_DIR"
    echo "  Python: $(which python)"
    echo "  Pip: $(which pip)"
else
    echo "❌ Error: No se encontró el entorno virtual en $SCRIPT_DIR/venv"
    echo "   Crea el entorno virtual con: python3 -m venv venv"
    exit 1
fi
















