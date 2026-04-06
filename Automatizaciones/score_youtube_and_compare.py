from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix

# =========================
# Archivos
# =========================
BASE_DIR = Path(__file__).resolve().parent

INPUT_CSV = BASE_DIR / "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Automatizaciones/SCORINGS - YT_STAGING.csv"
OUTPUT_CSV = BASE_DIR / "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Automatizaciones/SCORINGS - YT_STAGING_scored.csv"

# Columnas (las que me pasaste)
TEXT_COL = "content_original"
LABEL_COL = "odio_flag"
ID_COL = "message_uuid"

# =========================
# Cargar modelo
# =========================
# Usar el modelo MIXTO (X + YT)
model = joblib.load(BASE_DIR / "modelo_odio_mixed_v1.pkl")
vectorizer = joblib.load(BASE_DIR / "vectorizer_tfidf_mixed_v1.pkl")

# Threshold específico para YouTube (según evaluación en YT)
TH = 0.30

# =========================
# Cargar datos
# =========================
df = pd.read_csv(INPUT_CSV)

# Validaciones mínimas
for c in [ID_COL, TEXT_COL]:
    if c not in df.columns:
        raise ValueError(f"Falta la columna {c}. Columnas disponibles: {list(df.columns)}")

# Texto
text = df[TEXT_COL].fillna("").astype(str)

# =========================
# Scoring
# =========================
X_vec = vectorizer.transform(text)
df["proba_odio"] = model.predict_proba(X_vec)[:, 1]
df["pred_odio"] = (df["proba_odio"] >= TH).astype(int)

def bucket(p: float) -> str:
    if p >= 0.80:
        return "P1_ALTA"
    if p >= 0.50:
        return "P2_MEDIA"
    if p >= TH:
        return "P3_BAJA"
    return "P4_NO_PRIORITARIO"

df["priority"] = df["proba_odio"].apply(bucket)

# =========================
# Salida informativa (siempre)
# =========================
print("\n=== Scoring completado ===")
print(f"Filas totales scoreadas: {len(df)}")
print("\nDistribución pred_odio (0/1):")
print(df["pred_odio"].value_counts(dropna=False))
print("\nDistribución por prioridad:")
print(df["priority"].value_counts(dropna=False))

preview = df[[ID_COL, "proba_odio", "pred_odio", "priority", TEXT_COL]].copy()
preview[TEXT_COL] = preview[TEXT_COL].astype(str).str.slice(0, 80)
preview = preview.sort_values("proba_odio", ascending=False).head(5)
print("\nTop 5 por probabilidad de ODIO:")
print(preview.to_string(index=False))

# =========================
# Evaluación vs manual (si hay etiquetas)
# =========================
def normalize_manual(v):
    """Normaliza la etiqueta manual a 0/1.

    Acepta: Si/Sí/No, ODIO/NO_ODIO, 1/0, true/false. Devuelve None para dudoso o vacío.
    """
    if pd.isna(v):
        return None

    s = str(v).strip().lower()

    # Vacío / dudoso
    if s in ("", "dudoso", "duda", "na", "nan", "none", "null"):
        return None

    # Formatos de texto
    if s in ("si", "sí", "odio", "od", "odioso"):
        return 1
    if s in ("no", "no_odio", "no odio"):
        return 0

    # Formatos booleanos
    if s in ("true", "t"):
        return 1
    if s in ("false", "f"):
        return 0

    # Formatos numéricos (0/1)
    try:
        n = int(float(s))
        if n in (0, 1):
            return n
    except Exception:
        pass

    return None

if LABEL_COL in df.columns:
    # Debug: ver valores reales de odio_flag en el CSV
    raw_vals = (
        df[LABEL_COL]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
    )
    raw_vals = raw_vals[raw_vals != ""]

    print("\nValores distintos en odio_flag (no vacíos), sample:")
    print(raw_vals.value_counts().head(20))

    df["_y_manual"] = df[LABEL_COL].apply(normalize_manual)
    eval_df = df[df["_y_manual"].notna()].copy()

    if len(eval_df) > 0:
        y_true = eval_df["_y_manual"].astype(int)
        y_pred = eval_df["pred_odio"].astype(int)

        print("\n=== Evaluación vs manual (solo filas con odio_flag Si/No) ===")
        print(f"Filas evaluadas: {len(eval_df)} / {len(df)}")
        print("Threshold:", TH)

        print("\nMatriz de confusión [ [TN FP] [FN TP] ]:")
        print(confusion_matrix(y_true, y_pred))

        print("\nReporte:")
        print(classification_report(y_true, y_pred))
    else:
        print("\nNo hay filas con odio_flag etiquetado (Si/No/ODIO/NO_ODIO/0/1). Esto es normal si el CSV aún no fue etiquetado. Igual se generó pred_odio/proba_odio/priority para comparar luego.")
else:
    print("\nNo encontré columna odio_flag. Solo hice scoring.")

# =========================
# Export
# =========================
# Formato compatible con Google Sheets (coma decimal)
df["proba_odio"] = df["proba_odio"].map(lambda x: f"{x:.6f}".replace(".", ","))
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8", sep=";")
print(f"\nOK: generado {OUTPUT_CSV}")