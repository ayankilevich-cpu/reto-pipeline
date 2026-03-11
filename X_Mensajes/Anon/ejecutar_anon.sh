#!/bin/bash
# Script helper para ejecutar filter_and_anonymize_x.py con RETO_SALT configurado

# Cargar SALT desde secrets.yaml si existe
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="$SCRIPT_DIR/../../reto-scraper/config/secrets.yaml"

if [ -f "$SECRETS_FILE" ]; then
    # Extraer el SALT del archivo YAML (búsqueda simple)
    SALT=$(grep -E "^anonym_salt:" "$SECRETS_FILE" | sed 's/^anonym_salt: *"\(.*\)".*/\1/' | head -1)
    if [ -n "$SALT" ]; then
        export RETO_SALT="$SALT"
        echo "✅ RETO_SALT cargado desde secrets.yaml"
    fi
fi

# Si no se encontró en secrets.yaml, verificar si ya está configurado
if [ -z "$RETO_SALT" ]; then
    echo "⚠️  RETO_SALT no está configurado."
    echo "   Configúralo con: export RETO_SALT='tu_salt_aqui'"
    echo "   O edita este script para agregarlo."
    exit 1
fi

# Cambiar al directorio del script
cd "$SCRIPT_DIR"

# Activar entorno virtual si existe
if [ -f "../venv/bin/activate" ]; then
    source ../venv/bin/activate
fi

# Ejecutar el script con los argumentos pasados
python filter_and_anonymize_x.py "$@"




