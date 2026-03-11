#!/bin/bash
# Script helper para ejecutar sync_drive_csvs.py con la configuración predeterminada

# Ruta al archivo de credenciales
CREDENTIALS="/Users/alejandroyankilevich/Library/Containers/com.apple.Notes/Data/Library/CoreData/Attachments/67E6CA2F-84C7-4511-BF9A-0DD973132D12/gen-lang-client-0962946576-cb7a6b166a7b.json"

# Verificar que el archivo existe
if [ ! -f "$CREDENTIALS" ]; then
    echo "❌ Error: No se encontró el archivo de credenciales en:"
    echo "   $CREDENTIALS"
    echo ""
    echo "Por favor, verifica la ruta o proporciona una ruta alternativa."
    exit 1
fi

# Directorio del script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Verificar que el script Python existe
if [ ! -f "sync_drive_csvs.py" ]; then
    echo "❌ Error: No se encontró sync_drive_csvs.py en $SCRIPT_DIR"
    exit 1
fi

# Si no se proporcionan argumentos, mostrar ayuda
if [ $# -eq 0 ]; then
    echo "📋 Uso del script:"
    echo ""
    echo "  ./ejecutar_sync.sh --folder-id \"TU_FOLDER_ID\" [--out-dir \"directorio\"] [otras opciones]"
    echo ""
    echo "Ejemplo:"
    echo "  ./ejecutar_sync.sh --folder-id \"1aBCdEfGhijkLmnOpQrStUvWxYzZ\" --out-dir \"data/raw\""
    echo ""
    echo "Opciones disponibles:"
    python3 sync_drive_csvs.py --help
    exit 0
fi

# Ejecutar el script con las credenciales predeterminadas
echo "🔐 Usando credenciales: $CREDENTIALS"
echo ""

python3 sync_drive_csvs.py --credentials "$CREDENTIALS" "$@"




