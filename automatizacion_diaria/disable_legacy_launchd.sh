#!/usr/bin/env bash
# Desactiva launchd legacy de ReTo en la Mac local.
# Uso:
#   bash disable_legacy_launchd.sh

set -euo pipefail

UID_NUM="$(id -u)"
LABELS=(
  "com.reto.pipeline_x"
  "com.reto.pipeline_youtube"
  "com.reto.tag_youtube_hate_auto"
  "com.reto.youtube_extract_hate"
  "com.retoscraper.youtube_extract"
  "com.retoscraper.youtube"
)

for label in "${LABELS[@]}"; do
  echo "Desactivando $label ..."
  launchctl bootout "gui/$UID_NUM/$label" 2>/dev/null || true
done

echo
echo "Estado residual:"
launchctl list | grep -E "reto|youtube|pipeline" || true
