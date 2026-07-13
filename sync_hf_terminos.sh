#!/bin/bash
# sync_hf_terminos.sh — Sincroniza lista de exclusión de términos al Space de Hugging Face
# Dirección: repo principal → hf-space-temp → push (NUNCA al revés)
# Uso: ./sync_hf_terminos.sh "mensaje de commit opcional"

set -e
SRC_DIR="$(dirname "$0")/automatizacion_diaria"
HF_DIR="$(dirname "$0")/hf-space-temp"
MSG="${1:-sync: actualizar lista de exclusión de términos en HF Space}"

echo "→ Copiando terminos_exclusion_oficial.py y JSON a HF Space..."
cp "$SRC_DIR/terminos_exclusion_oficial.py" "$HF_DIR/automatizacion_diaria/terminos_exclusion_oficial.py"
cp "$SRC_DIR/terminos_excluidos_visualizacion.json" "$HF_DIR/automatizacion_diaria/terminos_excluidos_visualizacion.json"
cp "$SRC_DIR/terminos_exclusion_oficial.py" "$HF_DIR/terminos_exclusion_oficial.py"

echo "→ Commiteando en hf-space-temp..."
cd "$HF_DIR"
git add automatizacion_diaria/terminos_exclusion_oficial.py automatizacion_diaria/terminos_excluidos_visualizacion.json terminos_exclusion_oficial.py
git commit -m "$MSG" || echo "(sin cambios)"

echo "→ Push a HuggingFace..."
GIT_LFS_SKIP_PUSH=1 git push

echo "✅ Sync completo → https://huggingface.co/spaces/aleyanki13/proyectoreto"
