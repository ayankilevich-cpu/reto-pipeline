"""
Script para validar mensajes de X/Twitter y detectar términos de odio.
Lee un CSV con mensajes y genera uno nuevo con términos de odio detectados.
"""

import os
import pandas as pd
import re
import unicodedata
from pathlib import Path

# =====================================
# CONFIGURACIÓN
# =====================================

SCRIPT_DIR = Path(__file__).parent

# Archivos de entrada
INPUT_CSV = Path(os.getenv("X_TERMS_INPUT_CSV", str(SCRIPT_DIR.parent / "X_Mensajes" / "Anon" / "reto_x_master_anon.csv")))
HATE_DICT_CSV = SCRIPT_DIR / "hate_terms_clean.csv"
HATE_GENERAL_CSV = SCRIPT_DIR / "hate_general_terms.csv"
STOPWORDS_FILE = SCRIPT_DIR / "stopwords_extras.txt"

# Archivo de salida
OUTPUT_CSV = SCRIPT_DIR / "x_manual_label_for_sheets_tagged.csv"


# =====================================
# FUNCIONES AUXILIARES
# =====================================

def normalize_text(text: str) -> str:
    """Pasa a minúsculas, quita acentos y normaliza espacios."""
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text: str):
    """Tokeniza en palabras (letras/números)."""
    return re.findall(r"\w+", text)


def load_extra_stopwords(path: Path):
    """Carga stopwords extra desde un txt (ignora líneas vacías y comentarios con #)."""
    stop = set()
    if not path.exists():
        return stop
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().lower()
            if not line or line.startswith("#"):
                continue
            stop.add(normalize_text(line))
    return stop


# =====================================
# CARGA STOPWORDS
# =====================================

print("Cargando stopwords extra...")
EXTRA_STOP_TERMS = load_extra_stopwords(STOPWORDS_FILE)

# Stopwords base del español (mismas que en limpiar_csv_stopwords.py)
SPANISH_STOPWORDS_BASE = {
    "es", "que", "por", "pro", "de", "la", "el", "un", "una", "y", "a", "en", "lo", "no",
    "con", "sin", "para", "del", "al", "le", "da", "se", "te", "me", "los", "las", "nos",
    "son", "han", "está", "están", "ser", "será", "si", "ya", "más", "muy", "tan", "cómo",
    "cuando", "donde", "quien", "como", "todo", "toda", "todos", "todas", "mismo", "misma",
    "bien", "mal", "mala", "malo", "malos", "bueno", "buena", "buenos", "buenas",
    "poco", "poca", "pocos", "pocas", "mucho", "mucha", "muchos", "muchas",
    "todo", "toda", "todos", "todas", "nada", "nadie", "nunca", "siempre",
    "aquí", "allí", "ahí", "allá", "ahora", "antes", "después", "entonces",
    "pero", "mas", "sino", "aunque", "también", "tampoco", "solo", "sola",
    "grave", "graves", "odio", "odios",
    "español", "española", "españoles", "españolas",
    "gobierno", "gobiernos",
    "políticos", "político", "política", "politicos", "politico", "politica",
    "radical", "radicales",
    "problema", "problemas",
    "pueblo", "pueblos",
    "país", "países",
    "día", "días",
    "año", "años",
    "vez", "veces",
    "hora", "horas",
    "hombre", "hombres",
    "mujer", "mujeres",
    "gente",
    "vida", "vidas",
    "trabajo", "trabajos",
    "caso", "casos",
    "parte", "partes",
    "tiempo", "tiempos",
    "forma", "formas",
    "momento", "momentos",
    "lugar", "lugares",
    "manera", "maneras",
    "hecho", "hechos",
    "dicho", "dichos",
    "hecha", "hechas",
}

# Combinar stopwords (normalizadas)
ALL_STOPWORDS = {normalize_text(sw) for sw in SPANISH_STOPWORDS_BASE}
ALL_STOPWORDS.update(EXTRA_STOP_TERMS)
print(f"Stopwords totales cargadas: {len(ALL_STOPWORDS)} términos.")


# =====================================
# CARGA DICCIONARIOS DE ODIO
# =====================================

print("\nCargando diccionarios de términos de odio...")

# Cargar hate_terms_clean.csv (columna "Lemas")
hate_df_clean = pd.read_csv(HATE_DICT_CSV)
if "Lemas" not in hate_df_clean.columns:
    raise ValueError(f"El archivo {HATE_DICT_CSV} debe tener una columna llamada 'Lemas'.")

hate_df_clean["lemma_norm"] = (
    hate_df_clean["Lemas"]
    .astype(str)
    .str.lower()
    .str.strip()
)
hate_df_clean = hate_df_clean[hate_df_clean["lemma_norm"] != ""]
print(f"  - Términos cargados de hate_terms_clean.csv: {len(hate_df_clean)}")

# Cargar hate_general_terms.csv (columna "term")
hate_df_general = pd.read_csv(HATE_GENERAL_CSV)
if "term" not in hate_df_general.columns:
    raise ValueError(f"El archivo {HATE_GENERAL_CSV} debe tener una columna llamada 'term'.")

hate_df_general["lemma_norm"] = (
    hate_df_general["term"]
    .astype(str)
    .str.lower()
    .str.strip()
)
hate_df_general = hate_df_general[hate_df_general["lemma_norm"] != ""]
print(f"  - Términos cargados de hate_general_terms.csv: {len(hate_df_general)}")

# Combinar ambos DataFrames
hate_df_clean["source"] = "hate_terms_clean"
hate_df_general["source"] = "hate_general_terms"

# Seleccionar columnas comunes
hate_df_clean_subset = hate_df_clean[["lemma_norm", "source"]].copy()
hate_df_general_subset = hate_df_general[["lemma_norm", "source"]].copy()

# Combinar ambos DataFrames
hate_df = pd.concat([hate_df_clean_subset, hate_df_general_subset], ignore_index=True)

# Eliminar duplicados (mantener el primero que aparece)
hate_df = hate_df.drop_duplicates(subset=["lemma_norm"], keep="first")

print(f"  - Términos únicos después de combinar: {len(hate_df)}")

# Excluir stopwords extra del diccionario
hate_df = hate_df[~hate_df["lemma_norm"].isin(EXTRA_STOP_TERMS)]
print(f"  - Términos después de filtrar stopwords: {len(hate_df)}")

# Separamos lemas de una palabra y multi-palabra
single_word_lemmas = set(
    h for h in hate_df["lemma_norm"] if " " not in h and h != ""
)
multi_word_lemmas = [h for h in hate_df["lemma_norm"] if " " in h]

print(f"  - Términos de una palabra: {len(single_word_lemmas)}")
print(f"  - Términos multi-palabra: {len(multi_word_lemmas)}")


# =====================================
# CARGA CSV DE MENSAJES
# =====================================

print(f"\nLeyendo mensajes de X desde {INPUT_CSV}...")
df = pd.read_csv(INPUT_CSV)

# Verificar columnas requeridas
required_cols = ["message_uuid", "content_original"]
missing = set(required_cols) - set(df.columns)
if missing:
    raise ValueError(f"Faltan columnas requeridas: {missing}")

print(f"  - Total de mensajes: {len(df)}")

# Normalizar texto
df["content_norm"] = df["content_original"].apply(normalize_text)


# =====================================
# DETECCIÓN DE TÉRMINOS DE ODIO
# =====================================

def detect_hate_terms(text_norm: str):
    """
    Devuelve:
    - matched_lemmas (str): términos encontrados separados por punto y coma
    - match_count (int): número de términos únicos encontrados
    """
    if not text_norm:
        return "", 0
    
    tokens = tokenize(text_norm)
    token_set = set(tokens)

    # matches de una palabra (filtrar stopwords)
    matches_single = token_set.intersection(single_word_lemmas)
    # Filtrar stopwords de los matches
    matches_single = {m for m in matches_single if normalize_text(m) not in ALL_STOPWORDS}

    # matches de varias palabras (buscamos la frase exacta en el texto normalizado)
    matches_multi = set()
    for lemma in multi_word_lemmas:
        if lemma in text_norm:
            # Verificar que no sea solo stopwords
            lemma_tokens = tokenize(lemma)
            if any(normalize_text(t) not in ALL_STOPWORDS for t in lemma_tokens):
                matches_multi.add(lemma)

    all_matches = matches_single.union(matches_multi)

    if not all_matches:
        return "", 0

    match_count = len(all_matches)
    matched_lemmas_str = ";".join(sorted(all_matches))

    return matched_lemmas_str, match_count


print("\nAplicando detección de términos de odio...")
results = df["content_norm"].apply(detect_hate_terms)
df["terms_matched"] = results.apply(lambda x: x[0])
df["match_count"] = results.apply(lambda x: x[1])

# Filtrar solo mensajes con términos de odio detectados
df_with_hate = df[df["match_count"] > 0].copy()
print(f"  - Mensajes con términos de odio detectados: {len(df_with_hate)}")


# =====================================
# PREPARAR CSV PARA GOOGLE SHEETS
# =====================================

print("\nPreparando CSV para Google Sheets...")

# Crear DataFrame con la estructura requerida
output_df = pd.DataFrame()

# Mapear columnas existentes
output_df["message_uuid"] = df_with_hate["message_uuid"]
output_df["network"] = df_with_hate.get("network", "X")  # Usar "X" si no existe
output_df["published_at"] = df_with_hate.get("published_at", "")
output_df["video_id"] = df_with_hate.get("video_id", "")  # Vacío para X
output_df["content_original"] = df_with_hate["content_original"]
output_df["terms_matched"] = df_with_hate["terms_matched"]  # Términos detectados
output_df["language"] = df_with_hate.get("language", "es")

# Campos para etiquetado manual (vacíos)
output_df["odio_flag"] = ""
output_df["categoria_odio"] = ""
output_df["intensidad"] = ""
output_df["humor_flag"] = ""
output_df["annotator_id"] = ""
output_df["annotation_date"] = ""

# Si ya existían valores en estos campos, preservarlos
if "odio_flag" in df_with_hate.columns:
    output_df["odio_flag"] = df_with_hate["odio_flag"].fillna("")
if "categoria_odio" in df_with_hate.columns:
    output_df["categoria_odio"] = df_with_hate["categoria_odio"].fillna("")
if "intensidad" in df_with_hate.columns:
    output_df["intensidad"] = df_with_hate["intensidad"].fillna("")
if "humor_flag" in df_with_hate.columns:
    output_df["humor_flag"] = df_with_hate["humor_flag"].fillna("")
if "annotator_id" in df_with_hate.columns:
    output_df["annotator_id"] = df_with_hate["annotator_id"].fillna("")
if "annotation_date" in df_with_hate.columns:
    output_df["annotation_date"] = df_with_hate["annotation_date"].fillna("")


# =====================================
# GUARDAR CSV
# =====================================

print(f"\nGuardando CSV en {OUTPUT_CSV}...")
output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

print("\n" + "="*60)
print("RESUMEN")
print("="*60)
print(f"Total de mensajes procesados:     {len(df)}")
print(f"Mensajes con términos de odio:    {len(df_with_hate)}")
print(f"Archivo de salida:                {OUTPUT_CSV}")
print("="*60)
print("\n✓ Proceso completado.")


if __name__ == "__main__":
    pass

