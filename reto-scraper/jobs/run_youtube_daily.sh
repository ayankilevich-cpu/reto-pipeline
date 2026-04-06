#!/bin/bash
# Script wrapper para ejecutar el scraper diario de YouTube
# Ejecuta la generación de jobs y luego procesa los pendientes

# Cambiar al directorio del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR" || exit 1

# Configurar variables de entorno si es necesario
export POSTGRES_DSN="dbname=reto_scraper user=reto_writer password=Ale211083 host=localhost"
export RUN_YT_LIMIT=100
export RUN_YT_SLEEP=2
export RUN_YT_HOURS_BACK=24

# Archivo de log con fecha
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/youtube_daily_$(date +%Y%m%d_%H%M%S).log"

echo "==========================================" >> "$LOG_FILE"
echo "Ejecución diaria de YouTube scraper" >> "$LOG_FILE"
echo "Fecha: $(date)" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Activar entorno virtual si existe (ajusta la ruta según tu configuración)
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# 1. Generar jobs para las últimas 24 horas
echo "1. Generando jobs..." >> "$LOG_FILE"
python -m jobs.generate_jobs 2>&1 | tee -a "$LOG_FILE"

# 2. Ejecutar jobs pendientes
echo "" >> "$LOG_FILE"
echo "2. Ejecutando jobs pendientes..." >> "$LOG_FILE"
python -m jobs.run_pending_youtube 2>&1 | tee -a "$LOG_FILE"

# 3. Exportar a CSV para limpieza con stopwords
echo "" >> "$LOG_FILE"
echo "3. Exportando mensajes a CSV..." >> "$LOG_FILE"
python -m jobs.export_to_csv --hours-back 24 2>&1 | tee -a "$LOG_FILE"

echo "" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "Finalizado: $(date)" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

