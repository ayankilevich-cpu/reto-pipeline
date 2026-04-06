"""
Exporta mensajes de YouTube desde PostgreSQL a CSV para limpieza con stopwords.
El formato del CSV es compatible con limpiar_csv_stopwords.py
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg
import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exporta mensajes de YouTube a CSV")
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config"
    )
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=None,
        help="Ruta al archivo de secretos (default: config/secrets.yaml)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta del archivo CSV de salida (default: ../Medios/youtube_hatemedia_comments_30d.csv)"
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Horas hacia atrás para exportar (default: 24)"
    )
    return parser.parse_args()


def load_secrets(secrets_file: Path) -> dict:
    """Carga configuración desde secrets.yaml"""
    with secrets_file.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def connect_db(secrets: dict):
    """Crea conexión a PostgreSQL"""
    postgres = secrets["postgres"]
    return psycopg.connect(
        host=postgres["host"],
        port=postgres.get("port", 5432),
        dbname=postgres["database"],
        user=postgres["user"],
        password=postgres["password"],
    )


def export_youtube_messages(conn, output_file: Path, hours_back: int = 24) -> int:
    """
    Exporta mensajes de YouTube con términos detectados a CSV.
    
    El formato es compatible con limpiar_csv_stopwords.py:
    - medio, provincia, channel_url, channel_id, video_id, video_title,
      video_published_at, comment_id, comment_text, comment_published_at,
      like_count, author_display_name, author_channel_id, hate_terms_matched
    """
    
    # Calcular límite de tiempo
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    
    # Query para obtener mensajes con términos agrupados
    # Compatible con el formato esperado por limpiar_csv_stopwords.py
    sql = """
        WITH message_terms AS (
            SELECT
                rm.message_uuid,
                rm.network,
                rm.network_message_id,
                rm.content AS comment_text,
                rm.published_at AS comment_published_at,
                rm.collected_at,
                rm.like_count,
                COALESCE(rm.author_handle, rm.author_name, rm.author_id, '') AS author_display_name,
                COALESCE(rm.author_id, '') AS author_channel_id,
                rm.permalink,
                rm.extra,
                -- Agrupar términos detectados (eliminar duplicados)
                STRING_AGG(DISTINCT th.term, ', ' ORDER BY th.term) AS hate_terms_matched
            FROM raw_messages rm
            LEFT JOIN term_hits th ON th.message_uuid = rm.message_uuid
            WHERE rm.network = 'youtube'
              AND rm.collected_at >= %s
              AND rm.content IS NOT NULL
              AND rm.content != ''
            GROUP BY 
                rm.message_uuid,
                rm.network,
                rm.network_message_id,
                rm.content,
                rm.published_at,
                rm.collected_at,
                rm.like_count,
                rm.author_handle,
                rm.author_name,
                rm.author_id,
                rm.permalink,
                rm.extra
        )
        SELECT
            -- Campos que pueden venir de extra (del script youtube_extract_hate.py)
            -- o valores por defecto si vienen del scraper principal
            COALESCE(NULLIF(mt.extra->>'medio', ''), 'YouTube') AS medio,
            COALESCE(NULLIF(mt.extra->>'provincia', ''), '') AS provincia,
            COALESCE(NULLIF(mt.extra->>'channel_url', ''), '') AS channel_url,
            COALESCE(NULLIF(mt.extra->>'channel_id', ''), '') AS channel_id,
            COALESCE(NULLIF(mt.extra->>'video_id', ''), '') AS video_id,
            COALESCE(NULLIF(mt.extra->>'video_title', ''), '') AS video_title,
            COALESCE(NULLIF(mt.extra->>'video_published_at', '')::text, '') AS video_published_at,
            mt.network_message_id AS comment_id,
            mt.comment_text,
            COALESCE(mt.comment_published_at::text, '') AS comment_published_at,
            COALESCE(mt.like_count, 0) AS like_count,
            mt.author_display_name,
            mt.author_channel_id,
            COALESCE(mt.hate_terms_matched, '') AS hate_terms_matched
        FROM message_terms mt
        WHERE mt.hate_terms_matched IS NOT NULL
          AND mt.hate_terms_matched != ''
        ORDER BY mt.collected_at DESC;
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (time_threshold,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    if not rows:
        print(f"No se encontraron mensajes de YouTube de las últimas {hours_back} horas.")
        return 0
    
    # Crear directorio si no existe
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Escribir CSV
    with open(output_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"✓ Exportados {len(rows)} mensajes a: {output_file}")
    return len(rows)


def main() -> None:
    args = parse_args()
    
    # Determinar archivo de secretos
    secrets_file = args.secrets_file or args.config_dir / "secrets.yaml"
    if not secrets_file.exists():
        print(f"❌ Error: No se encontró el archivo de secretos: {secrets_file}")
        return
    
    # Determinar archivo de salida
    if args.output:
        output_file = args.output
    else:
        # Default: mismo directorio que limpiar_csv_stopwords.py
        output_file = (
            Path(__file__).resolve().parents[1].parent 
            / "Medios" 
            / "youtube_hatemedia_comments_30d.csv"
        )
    
    print(f"Exportando mensajes de YouTube de las últimas {args.hours_back} horas...")
    print(f"Archivo de salida: {output_file}")
    
    # Cargar configuración y conectar
    secrets = load_secrets(secrets_file)
    with connect_db(secrets) as conn:
        count = export_youtube_messages(conn, output_file, args.hours_back)
    
    if count > 0:
        print(f"\n✓ Exportación completada. Puedes limpiar el CSV con:")
        print(f"  python limpiar_csv_stopwords.py")


if __name__ == "__main__":
    main()

