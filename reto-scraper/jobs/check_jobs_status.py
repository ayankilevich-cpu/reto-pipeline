"""Verifica el estado de los jobs ejecutados recientemente."""

import os
from datetime import datetime, timedelta
import psycopg


def check_jobs_status(hours_back: int = 24):
    """Consulta y muestra el estado de los jobs ejecutados recientemente."""
    
    dsn = os.getenv(
        "POSTGRES_DSN",
        "dbname=reto_scraper user=reto_writer password=Ale211083 host=localhost",
    )
    
    cutoff_time = datetime.now() - timedelta(hours=hours_back)
    
    print(f"🔍 Consultando estado de jobs de Twitter ejecutados en las últimas {hours_back} horas...")
    print(f"   Fecha de corte: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    with psycopg.connect(dsn) as conn:
        # Consultar jobs ejecutados recientemente
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    job_id,
                    term,
                    status,
                    created_at,
                    started_at,
                    finished_at,
                    error,
                    retries
                FROM crawl_jobs
                WHERE network = 'twitter' 
                  AND (started_at >= %s OR finished_at >= %s OR created_at >= %s)
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT 50
            """, (cutoff_time, cutoff_time, cutoff_time))
            
            jobs = cur.fetchall()
        
        if not jobs:
            print("⚠️ No se encontraron jobs ejecutados en las últimas 24 horas.")
            return
        
        print(f"📊 Total de jobs encontrados: {len(jobs)}\n")
        
        # Agrupar por estado
        status_counts = {}
        for job in jobs:
            status = job[2]  # status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print("📈 Resumen por estado:")
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {status}: {count} jobs")
        
        print("\n" + "=" * 100)
        print("📝 Detalle de jobs (últimos 20):\n")
        
        for idx, job in enumerate(jobs[:20], 1):
            job_id, term, status, created_at, started_at, finished_at, error, retries = job
            
            print(f"\n[{idx}] Job ID: {str(job_id)[:8]}...")
            print(f"    Término: {term}")
            print(f"    Estado: {status}")
            print(f"    Reintentos: {retries}")
            print(f"    Creado: {created_at.strftime('%Y-%m-%d %H:%M:%S') if created_at else 'N/A'}")
            print(f"    Iniciado: {started_at.strftime('%Y-%m-%d %H:%M:%S') if started_at else 'N/A'}")
            print(f"    Finalizado: {finished_at.strftime('%Y-%m-%d %H:%M:%S') if finished_at else 'N/A'}")
            
            if error:
                error_preview = error[:200] + "..." if len(error) > 200 else error
                print(f"    ⚠️ Error: {error_preview}")
            
            # Contar mensajes encontrados para este job
            with conn.cursor() as cur2:
                cur2.execute("""
                    SELECT COUNT(*) 
                    FROM raw_messages 
                    WHERE job_id = %s
                """, (job_id,))
                msg_count = cur2.fetchone()[0]
                print(f"    Mensajes encontrados: {msg_count}")
            
            print("-" * 100)
        
        # Verificar mensajes totales en la base de datos
        print("\n📊 Estadísticas generales:\n")
        
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_messages WHERE network = 'twitter'")
            total_twitter_messages = cur.fetchone()[0]
            print(f"   Total de mensajes de Twitter en la BD: {total_twitter_messages}")
            
            cur.execute("""
                SELECT COUNT(*) 
                FROM raw_messages 
                WHERE network = 'twitter' 
                  AND collected_at >= %s
            """, (cutoff_time,))
            recent_messages = cur.fetchone()[0]
            print(f"   Mensajes en las últimas {hours_back} horas: {recent_messages}")
            
            cur.execute("""
                SELECT MAX(collected_at) 
                FROM raw_messages 
                WHERE network = 'twitter'
            """)
            last_message = cur.fetchone()[0]
            if last_message:
                print(f"   Último mensaje recolectado: {last_message.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"   Último mensaje recolectado: Ninguno")


if __name__ == "__main__":
    check_jobs_status(hours_back=24)




