"""
Exporta mensajes de Twitter/X desde PostgreSQL a CSV para análisis.
Incluye solo mensajes que tienen términos de odio detectados.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg
import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exporta mensajes de Twitter con términos de odio a CSV")
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
        help="Ruta del archivo CSV de salida"
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Horas hacia atrás para exportar (default: 24)"
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Exportar solo mensajes de un job específico (UUID)"
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


def export_twitter_messages(
    conn,
    output_file: Path,
    hours_back: int = 24,
    job_id: Optional[str] = None
) -> int:
    """
    Exporta mensajes de Twitter con términos detectados a CSV.
    
    Args:
        conn: Conexión a la base de datos
        output_file: Archivo CSV de salida
        hours_back: Horas hacia atrás para exportar
        job_id: Si se especifica, solo exporta mensajes de ese job
    
    Returns:
        Número de mensajes exportados
    """
    
    # Calcular límite de tiempo
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    
    # Query para obtener mensajes con términos agrupados
    sql = """
        WITH message_terms AS (
            SELECT
                rm.message_uuid,
                rm.network,
                rm.network_message_id AS tweet_id,
                rm.content AS tweet_text,
                rm.content_clean AS tweet_text_clean,
                rm.published_at AS tweet_published_at,
                rm.collected_at,
                rm.author_id,
                rm.author_handle,
                rm.author_name,
                rm.permalink,
                rm.like_count,
                rm.reply_count,
                rm.repost_count,
                rm.quote_count,
                rm.language,
                rm.job_id,
                rm.extra,
                -- Agrupar términos detectados (eliminar duplicados)
                STRING_AGG(DISTINCT th.term, ', ' ORDER BY th.term) AS hate_terms_matched,
                -- Contar número de términos únicos
                COUNT(DISTINCT th.term) AS hate_terms_count
            FROM raw_messages rm
            INNER JOIN term_hits th ON th.message_uuid = rm.message_uuid
            WHERE rm.network = 'twitter'
              AND rm.collected_at >= %s
              AND rm.content IS NOT NULL
              AND rm.content != ''
    """
    
    params = [time_threshold]
    
    # Filtrar por job_id si se especifica
    if job_id:
        sql += " AND rm.job_id = %s::UUID"
        params.append(job_id)
    
    sql += """
            GROUP BY 
                rm.message_uuid,
                rm.network,
                rm.network_message_id,
                rm.content,
                rm.content_clean,
                rm.published_at,
                rm.collected_at,
                rm.author_id,
                rm.author_handle,
                rm.author_name,
                rm.permalink,
                rm.like_count,
                rm.reply_count,
                rm.repost_count,
                rm.quote_count,
                rm.language,
                rm.job_id,
                rm.extra
        )
        SELECT
            mt.tweet_id,
            mt.tweet_text,
            mt.tweet_text_clean,
            COALESCE(mt.author_handle, mt.author_name, mt.author_id, '') AS author_handle,
            COALESCE(mt.author_name, '') AS author_name,
            COALESCE(mt.author_id, '') AS author_id,
            COALESCE(mt.permalink, '') AS permalink,
            COALESCE(mt.tweet_published_at::text, '') AS tweet_published_at,
            COALESCE(mt.collected_at::text, '') AS collected_at,
            COALESCE(mt.like_count, 0) AS like_count,
            COALESCE(mt.reply_count, 0) AS reply_count,
            COALESCE(mt.repost_count, 0) AS repost_count,
            COALESCE(mt.quote_count, 0) AS quote_count,
            COALESCE(mt.language, '') AS language,
            mt.hate_terms_matched,
            mt.hate_terms_count,
            COALESCE(mt.extra->>'term', '') AS search_term
        FROM message_terms mt
        WHERE mt.hate_terms_matched IS NOT NULL
          AND mt.hate_terms_matched != ''
        ORDER BY mt.collected_at DESC;
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    if not rows:
        print(f"No se encontraron mensajes de Twitter con términos de odio.")
        return 0
    
    # Crear directorio si no existe
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Escribir CSV
    with open(output_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"✓ Exportados {len(rows)} mensajes de Twitter con términos de odio a: {output_file}")
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
        # Default: directorio Medios
        output_file = (
            Path(__file__).resolve().parents[2]
            / "Medios"
            / f"twitter_hate_messages_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
    
    print(f"Exportando mensajes de Twitter con términos de odio...")
    if args.job_id:
        print(f"Filtrado por job_id: {args.job_id}")
    else:
        print(f"Últimas {args.hours_back} horas")
    print(f"Archivo de salida: {output_file}")
    
    # Cargar configuración y conectar
    secrets = load_secrets(secrets_file)
    with connect_db(secrets) as conn:
        count = export_twitter_messages(
            conn,
            output_file,
            args.hours_back,
            args.job_id
        )
    
    if count > 0:
        print(f"\n✓ Exportación completada: {count} mensajes con términos de odio")


if __name__ == "__main__":
    main()















