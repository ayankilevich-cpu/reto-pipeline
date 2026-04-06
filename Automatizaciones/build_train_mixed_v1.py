import pandas as pd
from pathlib import Path

# =========================
# Paths (mismos directorios)
# =========================
BASE_DIR = Path(__file__).resolve().parent
X_PATH = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Automatizaciones/reto_train_x_v1.csv"
YT_PATH = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Automatizaciones/Reto_Youtube_Para_Etiquetar.csv"
OUT_PATH = str(BASE_DIR / "reto_train_mixed_v1.csv")

# =========================
# 1) Cargar datasets
# =========================
df_x = pd.read_csv(X_PATH)               # columnas esperadas: message_id, message_text, y_odio
# El CSV de YouTube viene con separador ';' y a veces BOM/columnas vacías al final.
df_yt = pd.read_csv(
    YT_PATH,
    sep=";",
    encoding="utf-8-sig",
    engine="python",  # tolera saltos de línea dentro de comillas
)
# Quitar columnas basura tipo "Unnamed: X"
df_yt = df_yt.loc[:, ~df_yt.columns.astype(str).str.startswith("Unnamed")]

# =========================
# 2) Normalizar YouTube -> y_odio binario
# =========================
def yt_flag_to_y(v):
    s = str(v).strip().lower()
    if s in ("odio", "si", "sí", "1", "true"):
        return 1
    if s in ("no_odio", "no", "0", "false"):
        return 0
    return None

df_yt["y_odio"] = df_yt["odio_flag"].apply(yt_flag_to_y)
df_yt = df_yt.dropna(subset=["y_odio"]).copy()
df_yt["y_odio"] = df_yt["y_odio"].astype(int)

# Renombrar texto YT a message_text para unificar
df_yt = df_yt.rename(columns={"content_original": "message_text"})

# Source
df_yt["source"] = "YT"

# =========================
# 3) Normalizar X
# =========================
# df_x ya trae y_odio; aseguramos tipos
df_x = df_x.dropna(subset=["message_text", "y_odio"]).copy()
df_x["message_text"] = df_x["message_text"].astype(str)
df_x["y_odio"] = df_x["y_odio"].astype(int)
df_x["source"] = "X"

# =========================
# 4) Selección de columnas comunes
# =========================
df_x_final = df_x[["message_text", "y_odio", "source"]].copy()
df_yt_final = df_yt[["message_text", "y_odio", "source"]].copy()

# =========================
# 5) Unir y exportar
# =========================
df_mixed = pd.concat([df_x_final, df_yt_final], ignore_index=True)

# Limpieza mínima (quitar vacíos)
df_mixed["message_text"] = df_mixed["message_text"].str.strip()
df_mixed = df_mixed[df_mixed["message_text"] != ""]

# Shuffle reproducible
df_mixed = df_mixed.sample(frac=1, random_state=42).reset_index(drop=True)

print("=== Dataset MIXTO generado ===")
print("Total filas:", len(df_mixed))
print("\nDistribución por source:")
print(df_mixed["source"].value_counts())
print("\nDistribución por clase y_odio:")
print(df_mixed["y_odio"].value_counts())

df_mixed.to_csv(OUT_PATH, index=False, encoding="utf-8")
print(f"\nOK: exportado {OUT_PATH}")