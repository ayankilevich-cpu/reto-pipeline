#!/bin/bash
# sync_hf.sh — Sincroniza el dashboard al Space de Hugging Face
# Uso: ./sync_hf.sh "mensaje de commit opcional"

set -e

SRC="$(dirname "$0")/automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"
HF_DIR="$(dirname "$0")/hf-space-temp"
MSG="${1:-sync: actualizar dashboard desde repo principal}"

echo "→ Copiando dashboard_v3.py a HF Space..."
cp "$SRC" "$HF_DIR/automatizacion_diaria/dashboard.py"
cp "$SRC" "$HF_DIR/automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"

echo "→ Commiteando en hf-space-temp..."
cd "$HF_DIR"
git add automatizacion_diaria/dashboard.py
git add "automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"
git commit -m "$MSG" || echo "(sin cambios)"

echo "→ Push a HuggingFace..."
GIT_LFS_SKIP_PUSH=1 git push

echo "✅ Sync completo → https://huggingface.co/spaces/aleyanki13/proyectoreto"
