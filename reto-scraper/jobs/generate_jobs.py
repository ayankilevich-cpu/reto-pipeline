"""Generador de jobs de crawling según configuración."""

from __future__ import annotations

import argparse
import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import psycopg
import yaml

from processors.matcher import load_terms_csv
from processors.stopwords import filter_stopwords, load_all_stopwords

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class NetworkConfig:
    name: str
    lookback_minutes: int
    enabled: bool
    rate_limit: dict
    raw: dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera jobs para el scraper.")
    parser.add_argument("--config-dir", type=Path, default=Path(__file__).resolve().parents[1] / "config")
    parser.add_argument("--secrets-file", type=Path, default=None)
    parser.add_argument("--limit-terms", type=int, default=None, help="Limita la cantidad de términos a programar.")
    parser.add_argument("--dry-run", action="store_true", help="Muestra los jobs sin insertarlos.")
    parser.add_argument(
        "--hate-terms-file",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Medios" / "hate_terms_clean.csv",
        help="Ruta al archivo CSV con términos de odio (columna 'Lemas').",
    )
    parser.add_argument(
        "--twitter-handles-file",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Medios" / "twitter_handles.txt",
        help="Ruta a un archivo con handles de X (uno por línea o CSV con columna 'handle'). Se usa para jobs de Twitter en mode=timeline_replies.",
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
    return parser.parse_args()


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_networks(config_dir: Path) -> List[NetworkConfig]:
    raw = load_yaml(config_dir / "networks.yaml")
    configs: List[NetworkConfig] = []
    for name, conf in raw.items():
        configs.append(
            NetworkConfig(
                name=name,
                lookback_minutes=int(conf.get("search", {}).get("lookback_minutes", 60)),
                enabled=bool(conf.get("enabled", False)),
                rate_limit=conf.get("rate_limit", {}),
                raw=conf,
            )
        )
    return configs


def load_hate_terms_from_csv(
    csv_path: Path,
    stopwords: Optional[set] = None,
    filter_stopwords_flag: bool = True,
) -> List[str]:
    """
    Carga términos de odio desde un CSV con columna 'Lemas'.
    
    Args:
        csv_path: Ruta al archivo CSV con términos de odio
        stopwords: Set de stopwords normalizadas para filtrar
        filter_stopwords_flag: Si True, filtra stopwords de los términos
    
    Returns:
        Lista de términos de odio (filtrados si corresponde)
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
    
    return terms


def load_twitter_handles(path: Path) -> List[str]:
    """Carga handles de X desde .txt (uno por línea) o .csv con columna 'handle'."""
    if not path.exists():
        return []  # Retornar lista vacía en lugar de lanzar error

    handles: List[str] = []
    if path.suffix.lower() == ".csv":
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                raise ValueError(f"CSV vacío: {path}")
            # Acepta 'handle' o variantes comunes
            col = None
            for candidate in ("handle", "twitter", "x", "user", "username"):
                if candidate in reader.fieldnames:
                    col = candidate
                    break
            if col is None:
                raise ValueError(
                    f"El CSV de handles debe tener columna 'handle' (o equivalente). Columnas encontradas: {reader.fieldnames}"
                )
            for row in reader:
                h = (row.get(col) or "").strip()
                if not h:
                    continue
                h = h.lstrip("@").strip()
                if h:
                    handles.append(h)
    else:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                h = line.strip()
                if not h or h.startswith("#"):
                    continue
                h = h.lstrip("@").strip()
                if h:
                    handles.append(h)

    # Dedup manteniendo orden
    seen = set()
    out: List[str] = []
    for h in handles:
        if h in seen:
            continue
        seen.add(h)
        out.append(h)
    return out


def load_terms(
    config_dir: Path,
    limit: Optional[int],
    hate_terms_file: Optional[Path] = None,
    stopwords_extra_file: Optional[Path] = None,
    filter_stopwords_flag: bool = True,
    twitter_handles_file: Optional[Path] = None,
) -> List[str]:
    """
    Carga términos para los jobs.
    
    Si se proporciona hate_terms_file, carga desde ahí (columna 'Lemas').
    Si no, carga desde config_dir/terms.csv (formato estándar con columna 'term').
    """
    # Si se pide explícitamente, cargar handles para Twitter/X.
    # Si el archivo no existe, usar términos de odio como fallback.
    if twitter_handles_file is not None:
        if twitter_handles_file.exists():
            terms = load_twitter_handles(twitter_handles_file)
            logger.info(f"Handles de Twitter/X cargados desde {twitter_handles_file}: {len(terms)}")
            if limit is not None:
                terms = terms[:limit]
            return terms
        else:
            logger.warning(f"Archivo de handles no encontrado: {twitter_handles_file}")
            logger.info("Usando términos de odio como fallback...")
            # Continuar con la lógica normal de términos de odio

    if hate_terms_file and hate_terms_file.exists():
        # Cargar stopwords si se va a filtrar
        stopwords = None
        if filter_stopwords_flag:
            stopwords = load_all_stopwords(stopwords_extra_file)
            logger.info(f"Stopwords cargadas: {len(stopwords)}")
        
        terms = load_hate_terms_from_csv(hate_terms_file, stopwords, filter_stopwords_flag)
        logger.info(f"Términos de odio cargados desde {hate_terms_file}: {len(terms)}")
    else:
        # Cargar desde terms.csv (formato estándar)
        entries = load_terms_csv(config_dir / "terms.csv")
        terms = [term for term, _ in entries]
        logger.info(f"Términos cargados desde terms.csv: {len(terms)}")
    
    if limit is not None:
        terms = terms[:limit]
    
    return terms


def compute_window(minutes: int) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    return start, end


def connect_db(config: dict):
    postgres = config["postgres"]
    return psycopg.connect(
        host=postgres["host"],
        port=postgres.get("port", 5432),
        dbname=postgres["database"],
        user=postgres["user"],
        password=postgres["password"],
    )


def insert_job(conn, *, network: str, term: str, start: datetime, end: datetime, metadata: dict) -> Optional[str]:
    sql = """
        INSERT INTO crawl_jobs (network, term, search_window, status, metadata, scheduled_at)
        VALUES (%s, %s, tstzrange(%s, %s, '[)'), 'pending', %s::jsonb, NOW())
        ON CONFLICT ON CONSTRAINT uq_crawl_jobs_window DO NOTHING
        RETURNING job_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (network, term, start, end, json.dumps(metadata)))
        row = cur.fetchone()
        return row[0] if row else None


def main() -> None:
    args = parse_args()
    logger.info("generate_jobs.py VERSION=2025-12-22 per-network-terms (twitter handles when mode=timeline_replies)")
    config_dir = args.config_dir
    secrets_file = args.secrets_file or config_dir / "secrets.yaml"

    if not secrets_file.exists():
        raise FileNotFoundError(f"No se encontró el archivo de secretos: {secrets_file}")

    secrets = load_yaml(secrets_file)
    networks = load_networks(config_dir)
    scheduled = []
    pending_networks = [net for net in networks if net.enabled]

    if not pending_networks:
        logger.warning("No hay redes habilitadas en networks.yaml")
        return

    logger.info("Generando jobs para %d redes (términos/handles se calculan por red)", len(pending_networks))

    if args.dry_run:
        for network in pending_networks:
            start, end = compute_window(network.lookback_minutes)
            search = network.raw.get("search", {})
            mode = (search.get("mode") or "").strip().lower()

            if network.name == "twitter" and mode in {"timeline_replies", "handle_replies"}:
                terms = load_terms(
                    config_dir,
                    args.limit_terms,
                    hate_terms_file=args.hate_terms_file,
                    stopwords_extra_file=args.stopwords_extra_file,
                    filter_stopwords_flag=args.filter_stopwords,
                    twitter_handles_file=args.twitter_handles_file,
                )
            else:
                terms = load_terms(
                    config_dir,
                    args.limit_terms,
                    hate_terms_file=args.hate_terms_file,
                    stopwords_extra_file=args.stopwords_extra_file,
                    filter_stopwords_flag=args.filter_stopwords,
                )

            logger.info("[%s] jobs: %d -> %s - %s", network.name, len(terms), start.isoformat(), end.isoformat())
            for term in terms:
                logger.info("[%s] %s -> %s - %s", network.name, term, start.isoformat(), end.isoformat())
        return

    with connect_db(secrets) as conn:
        for network in pending_networks:
            start, end = compute_window(network.lookback_minutes)
            search = network.raw.get("search", {})
            mode = (search.get("mode") or "").strip().lower()

            if network.name == "twitter" and mode in {"timeline_replies", "handle_replies"}:
                terms = load_terms(
                    config_dir,
                    args.limit_terms,
                    hate_terms_file=args.hate_terms_file,
                    stopwords_extra_file=args.stopwords_extra_file,
                    filter_stopwords_flag=args.filter_stopwords,
                    twitter_handles_file=args.twitter_handles_file,
                )
            else:
                terms = load_terms(
                    config_dir,
                    args.limit_terms,
                    hate_terms_file=args.hate_terms_file,
                    stopwords_extra_file=args.stopwords_extra_file,
                    filter_stopwords_flag=args.filter_stopwords,
                )

            metadata = {"rate_limit": network.rate_limit, "search": search}
            for term in terms:
                job_id = insert_job(conn, network=network.name, term=term, start=start, end=end, metadata=metadata)
                if job_id:
                    scheduled.append((network.name, term, job_id))
        conn.commit()

    logger.info("Jobs nuevos: %d", len(scheduled))
    for network, term, job_id in scheduled[:10]:
        logger.info("Job %s -> %s (%s)", job_id, term, network)


if __name__ == "__main__":
    main()
