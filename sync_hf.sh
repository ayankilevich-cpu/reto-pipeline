#!/bin/bash
# sync_hf.sh — Sincroniza el dashboard al Space de Hugging Face
# Uso: ./sync_hf.sh "mensaje de commit opcional"

set -e

SRC_DIR="$(dirname "$0")/automatizacion_diaria/Diseñador Web Reto"
SRC="$SRC_DIR/dashboard_v3.py"
I18N="$SRC_DIR/i18n.py"
HF_DIR="$(dirname "$0")/hf-space-temp"
MSG="${1:-sync: actualizar dashboard desde repo principal}"

echo "→ Copiando dashboard_v3.py a HF Space..."
cp "$SRC" "$HF_DIR/automatizacion_diaria/dashboard.py"
cp "$SRC" "$HF_DIR/automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"

# El monolito hace `from i18n import t`; Streamlit resuelve módulos relativos
# al directorio del entry point (automatizacion_diaria/), no a Diseñador Web Reto/.
echo "→ Copiando i18n.py (requerido por el monolito)..."
cp "$I18N" "$HF_DIR/automatizacion_diaria/i18n.py"
cp "$I18N" "$HF_DIR/automatizacion_diaria/Diseñador Web Reto/i18n.py"

echo "→ Commiteando en hf-space-temp..."
cd "$HF_DIR"
git add automatizacion_diaria/dashboard.py
git add "automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"
git add automatizacion_diaria/i18n.py
git add "automatizacion_diaria/Diseñador Web Reto/i18n.py"
git commit -m "$MSG" || echo "(sin cambios)"

echo "→ Push a HuggingFace..."
GIT_LFS_SKIP_PUSH=1 git push

echo "✅ Sync completo → https://huggingface.co/spaces/aleyanki13/proyectoreto"
