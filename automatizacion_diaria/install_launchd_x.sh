#!/bin/bash
# install_launchd_x.sh - Instala / recarga el agente launchd del pipeline X.
#
# Que hace:
#   - Copia com.reto.pipeline_x.plist a ~/Library/LaunchAgents/
#   - Descarga la version previa si existe (bootout)
#   - Carga la nueva (bootstrap)
#   - Muestra estado
#
# Uso:
#   bash install_launchd_x.sh           # instalar / recargar
#   bash install_launchd_x.sh --status  # solo consultar estado
#   bash install_launchd_x.sh --unload  # desinstalar

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SRC="$SCRIPT_DIR/com.reto.pipeline_x.plist"
PLIST_NAME="com.reto.pipeline_x.plist"
LAUNCH_DIR="$HOME/Library/LaunchAgents"
PLIST_DST="$LAUNCH_DIR/$PLIST_NAME"
LABEL="com.reto.pipeline_x"
UID_NUM="$(id -u)"

status() {
    echo "==> Estado actual:"
    if launchctl print "gui/$UID_NUM/$LABEL" >/dev/null 2>&1; then
        launchctl print "gui/$UID_NUM/$LABEL" | grep -E "state|last exit|path|program|run count" || true
    else
        echo "   (no esta cargado)"
    fi
}

if [[ "${1:-}" == "--status" ]]; then
    status
    exit 0
fi

if [[ "${1:-}" == "--unload" ]]; then
    echo "==> Desinstalando $LABEL..."
    launchctl bootout "gui/$UID_NUM/$LABEL" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "    OK (removido de $PLIST_DST)"
    exit 0
fi

if [[ ! -f "$PLIST_SRC" ]]; then
    echo "ERROR: no existe $PLIST_SRC" >&2
    exit 1
fi

mkdir -p "$LAUNCH_DIR"

echo "==> Copiando plist a $PLIST_DST"
cp "$PLIST_SRC" "$PLIST_DST"
chmod 644 "$PLIST_DST"

echo "==> Descargando agente previo (si existe)..."
launchctl bootout "gui/$UID_NUM/$LABEL" 2>/dev/null || true

echo "==> Cargando agente nuevo..."
launchctl bootstrap "gui/$UID_NUM" "$PLIST_DST"

echo "==> Habilitando en caso de estar disabled..."
launchctl enable "gui/$UID_NUM/$LABEL" || true

sleep 1
status

echo ""
echo "Comandos utiles:"
echo "  Ver estado:     bash $0 --status"
echo "  Forzar corrida: launchctl kickstart -k gui/$UID_NUM/$LABEL"
echo "  Logs launchd:   tail -f '$SCRIPT_DIR/logs/launchd_x_stdout.log'"
echo "  Logs wrapper:   tail -f \"\$(ls -t '$SCRIPT_DIR/logs/wrapper_'*.log | head -1)\""
echo "  Desinstalar:    bash $0 --unload"
