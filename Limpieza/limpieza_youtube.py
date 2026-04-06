import pandas as pd
import json
import hashlib
import math
from pathlib import Path

# -----------------------------------
# 1. Cargar CSV
# -----------------------------------
# Ajusta la ruta al archivo según donde lo tengas
BASE_DIR = Path(__file__).resolve().parent
df = pd.read_csv(BASE_DIR / "v_message_anonymized.csv")

# Por si hubiera algún extra vacío
df["extra"] = df["extra"].fillna("{}")

# -----------------------------------
# 2. Función genérica para extraer campos del JSON
# -----------------------------------
def extract_field_from_extra(extra_str, path):
    if pd.isna(extra_str):
        return None
    try:
        data = json.loads(extra_str)
        for p in path.split("."):
            data = data[p]
        return data
    except Exception:
        return None

# -----------------------------------
# 3. Extraer campos útiles de YouTube
# -----------------------------------
df["video_id"] = df["extra"].apply(
    lambda x: extract_field_from_extra(x, "raw.snippet.videoId")
)
df["channel_id"] = df["extra"].apply(
    lambda x: extract_field_from_extra(x, "raw.snippet.channelId")
)
df["comment_id"] = df["extra"].apply(
    lambda x: extract_field_from_extra(x, "raw.snippet.topLevelComment.id")
)
df["author_channel_id"] = df["extra"].apply(
    lambda x: extract_field_from_extra(x, "raw.snippet.topLevelComment.snippet.authorChannelId.value")
)
df["content_original"] = df["extra"].apply(
    lambda x: extract_field_from_extra(x, "raw.snippet.topLevelComment.snippet.textOriginal")
)

# -----------------------------------
# 4. Hashes adicionales para anonimizar canal / vídeo / autor
# -----------------------------------
def sha256_or_none(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return hashlib.sha256(str(x).encode("utf-8")).hexdigest()

df["channel_hash"] = df["channel_id"].apply(sha256_or_none)
df["video_hash"] = df["video_id"].apply(sha256_or_none)
df["author_channel_hash"] = df["author_channel_id"].apply(sha256_or_none)

# -----------------------------------
# 5. Tabla de términos (message_term_match)
# -----------------------------------
matches = (
    df[["message_uuid", "term", "match_type", "term_variant"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

# -----------------------------------
# 6. Agregar términos en una sola columna por mensaje
# -----------------------------------
def agg_terms(series):
    vals = [str(t) for t in series.dropna().unique()]
    return ";".join(sorted(vals)) if vals else None

terms_agg = (
    matches.groupby("message_uuid")["term"]
    .apply(agg_terms)
    .reset_index()
    .rename(columns={"term": "terms_matched"})
)

# -----------------------------------
# 7. Deduplicar mensajes (1 fila = 1 message_uuid)
# -----------------------------------
messages = df.drop_duplicates(subset=["message_uuid"]).copy()

# Añadir los términos agregados
messages = messages.merge(terms_agg, on="message_uuid", how="left")

# -----------------------------------
# 8. Seleccionar columnas finales para messages_clean
# -----------------------------------
messages_clean = messages[
    [
        "message_uuid",
        "network",
        "author_hash",
        "message_hash",
        "author_channel_hash",
        "video_id",
        "video_hash",
        "channel_id",
        "channel_hash",
        "comment_id",
        "content_original",
        "content_clean",
        "language",
        "like_count",
        "published_at",
        "collected_at",
        "terms_matched",
        "extra",      # si luego quieres podarla, se puede
    ]
]

# -----------------------------------
# 9. Guardar resultados
# -----------------------------------
messages_clean.to_csv(BASE_DIR / "messages_clean.csv", index=False)
matches.to_csv(BASE_DIR / "message_term_match.csv", index=False)

print("Filas originales:", len(df))
print("Mensajes únicos:", messages_clean.shape[0])
print("Filas en message_term_match:", matches.shape[0])
print("Listo: messages_clean.csv y message_term_match.csv generados.")
