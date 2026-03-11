#!/bin/bash
# Script wrapper para ejecutar la extracción diaria de YouTube
# Ejecuta youtube_extract_hate.py automáticamente

# Cambiar al directorio del script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Archivo de log con fecha
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/youtube_extract_daily_$(date +%Y%m%d_%H%M%S).log"

echo "==========================================" >> "$LOG_FILE"
echo "Ejecución diaria de YouTube Extract (youtube_extract_hate.py)" >> "$LOG_FILE"
echo "Fecha: $(date)" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Activar entorno virtual si existe (prioridad: venv local, luego reto-scraper)
if [ -d "$SCRIPT_DIR/venv" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
    echo "Entorno virtual activado: $SCRIPT_DIR/venv" >> "$LOG_FILE"
elif [ -d "$SCRIPT_DIR/.venv" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
    echo "Entorno virtual activado: $SCRIPT_DIR/.venv" >> "$LOG_FILE"
elif [ -d "$SCRIPT_DIR/../reto-scraper/venv" ]; then
    source "$SCRIPT_DIR/../reto-scraper/venv/bin/activate"
    echo "Entorno virtual activado: $SCRIPT_DIR/../reto-scraper/venv" >> "$LOG_FILE"
elif [ -d "$SCRIPT_DIR/../reto-scraper/.venv" ]; then
    source "$SCRIPT_DIR/../reto-scraper/.venv/bin/activate"
    echo "Entorno virtual activado: $SCRIPT_DIR/../reto-scraper/.venv" >> "$LOG_FILE"
else
    echo "⚠️ No se encontró entorno virtual, usando Python del sistema" >> "$LOG_FILE"
fi

# Ejecutar el script de extracción
echo "Ejecutando youtube_extract_hate.py..." >> "$LOG_FILE"
# Usar python3 con ruta absoluta si está disponible, sino usar python
if command -v python3 &> /dev/null; then
    PYTHON_CMD=$(which python3)
elif command -v python &> /dev/null; then
    PYTHON_CMD=$(which python)
else
    PYTHON_CMD="python3"  # Fallback
fi

echo "Usando Python: $PYTHON_CMD" >> "$LOG_FILE"
"$PYTHON_CMD" youtube_extract_hate.py 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo "" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "Finalizado: $(date)" >> "$LOG_FILE"
echo "Código de salida: $EXIT_CODE" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

exit $EXIT_CODE



