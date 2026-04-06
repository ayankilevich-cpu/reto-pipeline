"""Ejecución de un job individual del scraper."""

from __future__ import annotations

import argparse
import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import psycopg
import yaml
from psycopg.types import range as pg_range

from crawlers.common import RateLimitConfig
from crawlers.twitter import TwitterCrawler
from crawlers.youtube import YouTubeCrawler
from processors.cleaner import clean_text
from processors.matcher import TermMatcher, load_terms_csv
from processors.stopwords import filter_stopwords, load_all_stopwords

try:
    from langdetect import DetectorFactory, LangDetectException, detect

    DetectorFactory.seed = 42
except ImportError:  # pragma: no cover
    detect = None

    class LangDetectException(Exception):
        """Fallback cuando langdetect no está instalado."""


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_LANGDETECT_WARNED = False


def is_twitter_rate_limit_error(exc: Exception) -> bool:
    """Heurística simple para detectar rate limit / quota en errores del crawler."""
    msg = (str(exc) or "").lower()
    return (
        "too many requests" in msg
        or "rate limit" in msg
        or "429" in msg
        or "usagecapexceeded" in msg.lower()
        or "quota exceeded" in msg
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejecuta un job de crawling.")
    parser.add_argument("--job-id", required=True, help="UUID del job en crawl_jobs.")
    parser.add_argument("--config-dir", type=Path, default=Path(__file__).resolve().parents[1] / "config")
    parser.add_argument("--secrets-file", type=Path, default=None)
    parser.add_argument("--terms-file", type=Path, default=None)
    parser.add_argument(
        "--hate-terms-file",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Medios" / "hate_terms_clean.csv",
        help="Ruta al archivo CSV con términos de odio (columna 'Lemas').",
    )
    parser.add_argument(
        "--stopwords-extra-file",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Medios" / "stopwords_extras.txt",
        help="Ruta al archivo con stopwords adicionales.",
    )
    parser.add_argument(
        "--filter-stopwords",
        action="store_true",
        default=True,
        help="Filtrar stopwords de los términos de odio (por defecto: True).",
    )
    parser.add_argument(
        "--no-filter-stopwords",
        dest="filter_stopwords",
        action="store_false",
        help="No filtrar stopwords de los términos de odio.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        default=True,
        help="Exportar mensajes con términos de odio a CSV (por defecto: True).",
    )
    parser.add_argument(
        "--no-export-csv",
        dest="export_csv",
        action="store_false",
        help="No exportar mensajes a CSV.",
    )
    parser.add_argument(
        "--csv-output-dir",
        type=Path,
        default=None,
        help="Directorio para guardar el CSV (default: ../Medios).",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def connect_db(secrets: dict):
    postgres = secrets["postgres"]
    return psycopg.connect(
        host=postgres["host"],
        port=postgres.get("port", 5432),
        dbname=postgres["database"],
        user=postgres["user"],
        password=postgres["password"],
        autocommit=False,
    )


def get_job(conn, job_id: str) -> Optional[dict]:
    sql = """
        SELECT job_id, network, term, search_window, metadata, status
        FROM crawl_jobs
        WHERE job_id = %s
        FOR UPDATE
    """
    with conn.cursor() as cur:
        cur.execute(sql, (job_id,))
        row = cur.fetchone()
        if not row:
            return None
        search_window: pg_range.Range = row[3]
        lower = search_window.lower if search_window.lower else datetime.now(timezone.utc)
        upper = search_window.upper if search_window.upper else datetime.now(timezone.utc)
        return {
            "job_id": row[0],
            "network": row[1],
            "term": row[2],
            "window_start": lower,
            "window_end": upper,
            "metadata": row[4] or {},
            "status": row[5],
        }


def update_job_status(conn, job_id: str, status: str, *, error: Optional[str] = None) -> None:
    sql = """
        UPDATE crawl_jobs
        SET status = %s,
            error = %s,
            started_at = COALESCE(started_at, NOW()),
            finished_at = CASE WHEN %s IN ('succeeded', 'failed') THEN NOW() ELSE finished_at END
        WHERE job_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, error, status, job_id))


def build_twitter_crawler(secrets: dict, metadata: dict) -> TwitterCrawler:
    rl = metadata.get("rate_limit", {})
    rate_config = RateLimitConfig(
        requests_per_window=int(rl.get("requests_per_window", 450)),
        window_seconds=int(rl.get("window_seconds", 900)),
        backoff_seconds=int(rl.get("backoff_seconds", 60)),
    )

    search_md = metadata.get("search", {})
    max_pages = int(search_md.get("max_pages", 10))
    max_results = int(search_md.get("max_results", 100))

    # Hard safety caps to avoid burning quota in a single job.
    max_pages = max(1, min(max_pages, 25))
    max_results = max(10, min(max_results, 100))

    return TwitterCrawler(
        secrets["twitter"]["bearer_token"],
        rate_limit=rate_config,
        max_pages=max_pages,
        max_results=max_results,
    )


def build_youtube_crawler(secrets: dict, metadata: dict) -> YouTubeCrawler:
    rl = metadata.get("rate_limit", {})
    rate_config = RateLimitConfig(
        requests_per_window=int(rl.get("requests_per_window", 10_000)),
        window_seconds=int(rl.get("window_seconds", 86400)),
        backoff_seconds=int(rl.get("backoff_seconds", 120)),
    )
    return YouTubeCrawler(secrets["youtube"]["api_key"], rate_limit=rate_config)


def upsert_message(conn, payload: dict) -> str:
    sql = "SELECT upsert_raw_message(%s::jsonb);"
    with conn.cursor() as cur:
        cur.execute(sql, (json.dumps(payload),))
        return cur.fetchone()[0]


def insert_hits(conn, message_uuid: str, hits: Iterable[dict]) -> int:
    sql = "SELECT insert_term_hits(%s::uuid, %s::jsonb);"
    with conn.cursor() as cur:
        cur.execute(sql, (message_uuid, json.dumps(list(hits))))
        return cur.fetchone()[0]


def normalize_language(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip().lower().split("_")[0].split("-")[0]


def detect_language(text: str) -> Optional[str]:
    global _LANGDETECT_WARNED
    if not detect:
        if not _LANGDETECT_WARNED:
            logger.warning("langdetect no está instalado; textos sin metadatos de idioma podrían no filtrarse.")
            _LANGDETECT_WARNED = True
        return None
    if not text:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None


def is_spanish_language(lang: Optional[str]) -> bool:
    if not lang:
        return False
    return lang.startswith("es")


def export_job_messages_to_csv(
    conn,
    job_id: str,
    output_dir: Optional[Path] = None
) -> Optional[Path]:
    """
    Exporta mensajes con términos de odio de un job específico a CSV.
    
    Args:
        conn: Conexión a la base de datos
        job_id: UUID del job
        output_dir: Directorio donde guardar el CSV
    
    Returns:
        Ruta al archivo CSV creado, o None si no hay mensajes
    """
    if output_dir is None:
        output_dir = Path(__file__).resolve().parents[2] / "Medios"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Nombre del archivo con timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"twitter_hate_job_{job_id[:8]}_{timestamp}.csv"
    
    # Query para obtener mensajes con términos
    sql = """
        WITH message_terms AS (
            SELECT
                rm.network_message_id AS tweet_id,
                rm.content AS tweet_text,
                rm.content_clean AS tweet_text_clean,
                COALESCE(rm.author_handle, rm.author_name, rm.author_id, '') AS author_handle,
                COALESCE(rm.author_name, '') AS author_name,
                COALESCE(rm.author_id, '') AS author_id,
                COALESCE(rm.permalink, '') AS permalink,
                COALESCE(rm.published_at::text, '') AS tweet_published_at,
                COALESCE(rm.collected_at::text, '') AS collected_at,
                COALESCE(rm.like_count, 0) AS like_count,
                COALESCE(rm.reply_count, 0) AS reply_count,
                COALESCE(rm.repost_count, 0) AS repost_count,
                COALESCE(rm.quote_count, 0) AS quote_count,
                COALESCE(rm.language, '') AS language,
                STRING_AGG(DISTINCT th.term, ', ' ORDER BY th.term) AS hate_terms_matched,
                COUNT(DISTINCT th.term) AS hate_terms_count,
                COALESCE(rm.extra->>'term', '') AS search_term
            FROM raw_messages rm
            INNER JOIN term_hits th ON th.message_uuid = rm.message_uuid
            WHERE rm.network = 'twitter'
              AND rm.job_id = %s::UUID
              AND rm.content IS NOT NULL
              AND rm.content != ''
            GROUP BY 
                rm.network_message_id,
                rm.content,
                rm.content_clean,
                rm.author_handle,
                rm.author_name,
                rm.author_id,
                rm.permalink,
                rm.published_at,
                rm.collected_at,
                rm.like_count,
                rm.reply_count,
                rm.repost_count,
                rm.quote_count,
                rm.language,
                rm.extra
        )
        SELECT
            tweet_id,
            tweet_text,
            tweet_text_clean,
            author_handle,
            author_name,
            author_id,
            permalink,
            tweet_published_at,
            collected_at,
            like_count,
            reply_count,
            repost_count,
            quote_count,
            language,
            hate_terms_matched,
            hate_terms_count,
            search_term
        FROM message_terms
        WHERE hate_terms_matched IS NOT NULL
          AND hate_terms_matched != ''
        ORDER BY collected_at DESC;
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, (job_id,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    
    if not rows:
        return None
    
    # Escribir CSV
    with open(output_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)
    
    logger.info(f"✓ Exportados {len(rows)} mensajes con términos de odio a: {output_file}")
    return output_file


def execute_job(conn, job: dict, secrets: dict, matcher: TermMatcher) -> None:
    network = job["network"]
    metadata = job["metadata"] or {}
    search_conf = metadata.get("search", {})
    term = job["term"]
    query_template = search_conf.get("query_template", "{term}")
    query = query_template.replace("{term}", term)

    if network == "twitter":
        search_md = metadata.get("search", {})
        mode = (search_md.get("mode") or "term_search").strip().lower()

        # Mode A (default): term-based search (legacy)
        if mode == "term_search":
            crawler = build_twitter_crawler(secrets, metadata)
            try:
                responses = crawler.search(
                    query,
                    start_time=job["window_start"],
                    end_time=job["window_end"],
                )
            except Exception as exc:
                if is_twitter_rate_limit_error(exc):
                    raise RuntimeError("Twitter API rate limit/quota hit during term_search") from exc
                raise

        # Mode B (recommended for ReTo): anchor on a media account (handle) and fetch replies by conversation_id
        # The job's `term` is treated as the handle (without @) unless `metadata.search.handle` is provided.
        elif mode in {"timeline_replies", "handle_replies"}:
            handle = (search_md.get("handle") or term or "").lstrip("@").strip()
            if not handle:
                raise ValueError("Twitter mode timeline_replies requires a handle (job.term or metadata.search.handle)")

            # Safety caps: keep API usage predictable per job.
            max_seed_tweets = int(search_md.get("max_seed_tweets", 10))
            max_seed_tweets = max(1, min(max_seed_tweets, 25))
            max_replies_per_tweet = int(search_md.get("max_replies_per_tweet", 50))
            max_replies_per_tweet = max(10, min(max_replies_per_tweet, 200))

            # Use a small search for seed tweets from the handle within the window.
            seed_meta = dict(metadata)
            seed_meta["search"] = dict(search_md)
            seed_meta["search"]["max_pages"] = int(seed_meta["search"].get("seed_max_pages", 2))
            seed_meta["search"]["max_results"] = int(seed_meta["search"].get("seed_max_results", 50))
            seed_crawler = build_twitter_crawler(secrets, seed_meta)

            seed_query = f"from:{handle} -is:retweet"
            try:
                seed_results = list(seed_crawler.search(
                    seed_query,
                    start_time=job["window_start"],
                    end_time=job["window_end"],
                ))
            except Exception as exc:
                if is_twitter_rate_limit_error(exc):
                    raise RuntimeError("Twitter API rate limit/quota hit while fetching seed tweets") from exc
                raise

            # Keep only the newest N seed tweets.
            seed_results = sorted(
                seed_results,
                key=lambda m: (m.get("published_at") or ""),
                reverse=True,
            )[:max_seed_tweets]

            # Now fetch replies for each seed tweet via conversation_id.
            # We aggregate into a single iterable `responses`.
            responses = []
            replies_meta = dict(metadata)
            replies_meta["search"] = dict(search_md)
            replies_meta["search"]["max_pages"] = int(replies_meta["search"].get("replies_max_pages", 3))
            replies_meta["search"]["max_results"] = int(replies_meta["search"].get("replies_max_results", 100))
            replies_crawler = build_twitter_crawler(secrets, replies_meta)

            for seed in seed_results:
                conv_id = seed.get("network_message_id")
                if not conv_id:
                    continue

                # Exclude the media's own tweets to focus on audience replies.
                replies_query = f"conversation_id:{conv_id} -from:{handle}"
                try:
                    batch = list(replies_crawler.search(
                        replies_query,
                        start_time=job["window_start"],
                        end_time=job["window_end"],
                    ))
                except Exception as exc:
                    if is_twitter_rate_limit_error(exc):
                        raise RuntimeError("Twitter API rate limit/quota hit while fetching replies") from exc
                    raise

                # Limit replies per conversation to keep job bounded.
                if len(batch) > max_replies_per_tweet:
                    batch = sorted(
                        batch,
                        key=lambda m: (m.get("published_at") or ""),
                        reverse=True,
                    )[:max_replies_per_tweet]

                responses.extend(batch)

            # Deduplicate by tweet id.
            seen_ids = set()
            deduped = []
            for m in responses:
                mid = m.get("network_message_id")
                if not mid or mid in seen_ids:
                    continue
                seen_ids.add(mid)
                deduped.append(m)
            responses = deduped

            logger.info(
                "Twitter timeline_replies: handle=%s seeds=%d replies=%d (deduped)",
                handle,
                len(seed_results),
                len(responses),
            )

        else:
            raise ValueError(f"Twitter search mode no soportado: {mode}")
    elif network == "youtube":
        crawler = build_youtube_crawler(secrets, metadata)
        lookback_minutes = search_conf.get("lookback_minutes")
        lookback_days = max(int(lookback_minutes) // 1440, 1) if lookback_minutes else 1
        responses = crawler.search_comments(
            term,
            max_results=search_conf.get("max_results", 10),
            lookback_days=lookback_days,
            relevance_language=search_conf.get("relevance_language"),
        )
    else:
        raise NotImplementedError(f"Red no soportada: {network}")

    processed = 0
    for message in responses:
        job_id_str = str(job["job_id"])
        cleaned = clean_text(message["content"])

        api_language = normalize_language(message.get("language"))
        language_source = "api"
        language = api_language

        if language:
            if not is_spanish_language(language):
                continue
        else:
            detected_language = detect_language(cleaned.normalized)
            if not is_spanish_language(detected_language):
                continue
            language = detected_language
            language_source = "detected"

        hits = matcher.find_matches(cleaned.raw)

        payload = {
            "network": network,
            "network_message_id": message["network_message_id"],
            "job_id": job_id_str,
            "author_id": message.get("author_id"),
            "author_handle": message.get("author_handle"),
            "author_name": message.get("author_name"),
            "content": message["content"],
            "content_clean": cleaned.normalized,
            "language": language,
            "published_at": message.get("published_at"),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "permalink": message.get("permalink"),
            "like_count": message.get("metrics", {}).get("like_count"),
            "reply_count": message.get("metrics", {}).get("reply_count"),
            "repost_count": message.get("metrics", {}).get("repost_count"),
            "quote_count": message.get("metrics", {}).get("quote_count"),
            "extra": {
                "term": term,
                "raw": message.get("raw"),
                "language_source": language_source,
                "api_language": api_language,
            },
        }

        message_uuid = upsert_message(conn, payload)
        if hits:
            insert_hits(conn, message_uuid, hits)
        processed += 1

    logger.info("Mensajes procesados: %d", processed)
    
    # Contar mensajes con términos de odio
    count_sql = """
        SELECT COUNT(DISTINCT rm.message_uuid)
        FROM raw_messages rm
        INNER JOIN term_hits th ON th.message_uuid = rm.message_uuid
        WHERE rm.job_id = %s::UUID
    """
    with conn.cursor() as cur:
        cur.execute(count_sql, (str(job["job_id"]),))
        row = cur.fetchone()
        hate_messages_count = row[0] if row else 0
    
    if hate_messages_count > 0:
        logger.info(f"Mensajes con términos de odio detectados: {hate_messages_count}")


def load_hate_terms_from_csv(
    csv_path: Path,
    stopwords: Optional[set] = None,
    filter_stopwords_flag: bool = True,
) -> list[tuple[str, Optional[str]]]:
    """
    Carga términos de odio desde un CSV con columna 'Lemas'.
    Retorna formato compatible con TermMatcher: list[tuple[str, Optional[str]]]
    
    Args:
        csv_path: Ruta al archivo CSV con términos de odio
        stopwords: Set de stopwords normalizadas para filtrar
        filter_stopwords_flag: Si True, filtra stopwords de los términos
    
    Returns:
        Lista de tuplas (término, variant) compatible con TermMatcher
    """
    terms = []
    
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de términos de odio: {csv_path}")
    
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "Lemas" not in reader.fieldnames:
            raise ValueError(f"El CSV debe contener una columna 'Lemas'. Columnas encontradas: {reader.fieldnames}")
        
        for row in reader:
            term = row.get("Lemas", "").strip()
            if not term or term.startswith("#"):
                continue
            terms.append(term)
    
    # Filtrar stopwords si está habilitado
    if filter_stopwords_flag and stopwords is not None:
        terms = filter_stopwords(terms, stopwords)
        logger.info(f"Términos después de filtrar stopwords: {len(terms)}")
    
    # Convertir a formato compatible con TermMatcher: (term, variant)
    return [(term, None) for term in terms]


def main() -> None:
    args = parse_args()
    config_dir = args.config_dir
    secrets_file = args.secrets_file or (config_dir / "secrets.yaml")
    
    secrets = load_yaml(secrets_file)
    
    # Determinar qué archivo de términos usar
    if args.hate_terms_file and args.hate_terms_file.exists():
        # Cargar stopwords si se va a filtrar
        stopwords = None
        if args.filter_stopwords:
            stopwords = load_all_stopwords(args.stopwords_extra_file)
            logger.info(f"Stopwords cargadas: {len(stopwords)}")
        
        # Cargar términos de odio desde CSV con columna 'Lemas'
        terms_entries = load_hate_terms_from_csv(
            args.hate_terms_file,
            stopwords,
            args.filter_stopwords,
        )
        logger.info(f"Términos de odio cargados desde {args.hate_terms_file}: {len(terms_entries)}")
        matcher = TermMatcher(terms_entries, exceptions=[])
    else:
        # Usar terms.csv estándar (fallback)
        terms_file = args.terms_file or (config_dir / "terms.csv")
        logger.info(f"Usando archivo de términos estándar: {terms_file}")
        matcher = TermMatcher.from_csv(terms_file, exceptions=[])

    with connect_db(secrets) as conn:
        job = get_job(conn, args.job_id)
        if not job:
            raise ValueError(f"Job {args.job_id} no encontrado.")

        update_job_status(conn, job["job_id"], "running")
        conn.commit()

        try:
            execute_job(conn, job, secrets, matcher)
            conn.commit()
            
            # Exportar a CSV si está habilitado
            if args.export_csv:
                csv_file = export_job_messages_to_csv(
                    conn,
                    str(job["job_id"]),
                    args.csv_output_dir
                )
                if csv_file:
                    logger.info(f"CSV generado: {csv_file}")
                else:
                    logger.info("No se encontraron mensajes con términos de odio para exportar.")
            
        except Exception as exc:
            if is_twitter_rate_limit_error(exc):
                logger.error("Twitter rate limit/quota detected. Consider lowering max_pages/max_results and using mode=timeline_replies.")
            logger.exception("Error ejecutando job %s", job["job_id"])
            conn.rollback()
            update_job_status(conn, job["job_id"], "failed", error=str(exc))
            conn.commit()
            raise
        else:
            update_job_status(conn, job["job_id"], "succeeded")
            conn.commit()


if __name__ == "__main__":
    main()

