"""Ejecuta en lote los jobs pendientes de YouTube."""

from __future__ import annotations

import os
import sys
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import psycopg


@dataclass
class Settings:
    limit: int = int(os.getenv("RUN_YT_LIMIT", "10"))
    sleep_seconds: float = float(os.getenv("RUN_YT_SLEEP", "2"))
    hours_back: int = int(os.getenv("RUN_YT_HOURS_BACK", "24"))  # Solo jobs de las últimas N horas
    dsn: str = os.getenv(
        "POSTGRES_DSN",
        "dbname=reto_scraper user=reto_writer password=Ale211083 host=localhost",
    )


def fetch_pending_jobs(conn, limit: int, hours_back: int = 24) -> List[str]:
    """
    Obtiene jobs pendientes de YouTube de las últimas N horas.
    
    Filtra por el límite superior (upper) de la ventana de búsqueda (search_window)
    para asegurar que solo procesamos videos recientes.
    """
    # Calcular el límite de tiempo: ahora - N horas
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    
    sql = """
        SELECT job_id::text
        FROM crawl_jobs
        WHERE network = 'youtube'
          AND status = 'pending'
          AND upper(search_window) >= %s
        ORDER BY created_at
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (time_threshold, limit))
        return [row[0] for row in cur.fetchall()]


def run_job(job_id: str) -> None:
    # Asegurar que usamos el mismo Python que está ejecutando este script
    python_exe = sys.executable
    # Cambiar al directorio del proyecto (donde está reto-scraper)
    project_root = Path(__file__).parent.parent
    cmd = [python_exe, "-m", "jobs.run_job", "--job-id", job_id]
    # Ejecutar desde el directorio raíz del proyecto para que encuentre el módulo jobs
    subprocess.run(cmd, cwd=project_root, check=False)


def main() -> None:
    settings = Settings()
    print(f"Conectando a PostgreSQL con DSN: {settings.dsn!r}")
    print(f"Filtrando jobs de las últimas {settings.hours_back} horas")
    
    with psycopg.connect(settings.dsn) as conn:
        jobs = fetch_pending_jobs(conn, settings.limit, settings.hours_back)

    if not jobs:
        print(f"No se encontraron jobs pendientes de YouTube de las últimas {settings.hours_back} horas.")
        return

    print(f"Se ejecutarán {len(jobs)} jobs de YouTube (pausa {settings.sleep_seconds}s).")
    for job_id in jobs:
        print(f"\n=== Ejecutando job {job_id} ===")
        run_job(job_id)
        time.sleep(settings.sleep_seconds)


if __name__ == "__main__":
    main()

