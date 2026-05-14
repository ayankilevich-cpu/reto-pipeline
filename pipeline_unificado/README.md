# pipeline_unificado — Adapter (Fase 1.1)

## Qué es

El módulo `adapter.py` normaliza el **output de ingesta** de X (`consolidar_csv.py`) y de YouTube (`youtube_extract_hate.py`) a un **DataFrame con schema canónico** único. Es el contrato para las fases siguientes del pipeline unificado (anonimización, scoring, etc.).

## Schema canónico

Orden fijo de columnas:

| Columna | Tipo | Descripción breve |
|---------|------|-------------------|
| `message_uuid` | str | UUID (X: existente; YT: UUID v5 desde `comment_id`) |
| `platform` | str | `"x"` o `"youtube"` (minúsculas) |
| `content_original` | str | Texto sin modificar |
| `published_at` | str | ISO 8601 UTC cuando es posible parsear la fecha |
| `source_media` | str | Nombre del medio |
| `author_username` | str | Usuario público (sin anonimizar) |
| `author_id` | str | ID autor (sin anonimizar) |
| `url` | str | URL del tweet o comentario YT |
| `language` | str | Idioma; en YT se asume `"es"` |
| `parent_id` | str | Vacío en X; `video_id` en YouTube |
| `likes` | int | Me gusta; 0 si faltan datos |
| `retweet_count` | int | Solo X; 0 en YouTube |
| `reply_count` | int | Solo X; 0 en YouTube |
| `quote_count` | int | Solo X; 0 en YouTube |
| `ingestion_batch_id` | str | `batch_id` en X; vacío en YouTube |
| `ingestion_date` | str | `scrape_date` en X; fecha UTC de ejecución del adapter en YT |
| `platform_native_id` | str | `tweet_id` (X) o `comment_id` (YouTube) |

## Uso rápido

```python
import pandas as pd
from pipeline_unificado.adapter import adapt_x, adapt_youtube

df_x = pd.read_csv("X_Mensajes/data/master/reto_x_master.csv")
canon_x = adapt_x(df_x)

df_yt = pd.read_csv("Medios/youtube_hatemedia_comments_30d.csv")
canon_yt = adapt_youtube(df_yt)
```

## Tests

Desde la carpeta `Clases/RETO` (para que `pipeline_unificado` sea importable):

```bash
cd Clases/RETO
pytest pipeline_unificado/test_adapter.py -v
```

## Validación manual con CSV reales

```bash
cd Clases/RETO
python3 pipeline_unificado/validacion_manual_adapter.py
```
