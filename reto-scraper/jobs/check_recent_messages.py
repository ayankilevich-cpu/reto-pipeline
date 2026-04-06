"""Verifica los mensajes encontrados recientemente en la base de datos."""

import os
from datetime import datetime, timedelta
import psycopg


def check_recent_messages(hours_back: int = 24, limit: int = 10):
    """Consulta y muestra los mensajes encontrados recientemente."""
    
    dsn = os.getenv(
        "POSTGRES_DSN",
        "dbname=reto_scraper user=reto_writer password=Ale211083 host=localhost",
    )
    
    cutoff_time = datetime.now() - timedelta(hours=hours_back)
    
    print(f"🔍 Consultando mensajes de Twitter encontrados en las últimas {hours_back} horas...")
    print(f"   Fecha de corte: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    with psycopg.connect(dsn) as conn:
        # Primero, contar total de mensajes
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) 
                FROM raw_messages 
                WHERE network = 'twitter' 
                  AND collected_at >= %s
            """, (cutoff_time,))
            total_count = cur.fetchone()[0]
        
        print(f"📊 Total de mensajes encontrados: {total_count}\n")
        
        if total_count == 0:
            print("⚠️ No se encontraron mensajes nuevos en las últimas 24 horas.")
            print("   Esto puede significar que:")
            print("   - Los jobs no encontraron mensajes con los términos buscados")
            print("   - Los mensajes fueron filtrados (idioma, etc.)")
            print("   - Hubo algún error en la ejecución")
            return
        
        # Obtener muestra de mensajes
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    rm.message_uuid,
                    rm.author_handle,
                    rm.author_name,
                    rm.content,
                    rm.content_clean,
                    rm.published_at,
                    rm.collected_at,
                    rm.permalink,
                    rm.like_count,
                    rm.reply_count,
                    rm.repost_count,
                    rm.quote_count,
                    rm.language,
                    cj.term as search_term
                FROM raw_messages rm
                LEFT JOIN crawl_jobs cj ON rm.job_id = cj.job_id
                WHERE rm.network = 'twitter' 
                  AND rm.collected_at >= %s
                ORDER BY rm.collected_at DESC
                LIMIT %s
            """, (cutoff_time, limit))
            
            messages = cur.fetchall()
        
        print(f"📝 Muestra de {len(messages)} mensajes (más recientes primero):\n")
        print("=" * 100)
        
        for idx, msg in enumerate(messages, 1):
            message_uuid, author_handle, author_name, content, content_clean, \
            published_at, collected_at, permalink, like_count, reply_count, \
            repost_count, quote_count, language, search_term = msg
            
            print(f"\n[{idx}] Mensaje ID: {str(message_uuid)[:8]}...")
            print(f"    Autor: @{author_handle or 'N/A'}" + (f" ({author_name})" if author_name else ""))
            print(f"    Término buscado: {search_term or 'N/A'}")
            print(f"    Idioma: {language or 'N/A'}")
            print(f"    Publicado: {published_at.strftime('%Y-%m-%d %H:%M:%S') if published_at else 'N/A'}")
            print(f"    Recolectado: {collected_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"    Métricas: 👍 {like_count or 0} | 💬 {reply_count or 0} | 🔄 {repost_count or 0} | 📢 {quote_count or 0}")
            
            # Mostrar contenido (truncar si es muy largo)
            display_content = content_clean if content_clean else content
            if len(display_content) > 200:
                display_content = display_content[:200] + "..."
            print(f"    Contenido: {display_content}")
            
            if permalink:
                print(f"    Link: {permalink}")
            
            print("-" * 100)
        
        # Estadísticas adicionales
        print("\n📈 Estadísticas adicionales:\n")
        
        with conn.cursor() as cur:
            # Mensajes por término de búsqueda
            cur.execute("""
                SELECT 
                    cj.term,
                    COUNT(*) as count
                FROM raw_messages rm
                JOIN crawl_jobs cj ON rm.job_id = cj.job_id
                WHERE rm.network = 'twitter' 
                  AND rm.collected_at >= %s
                GROUP BY cj.term
                ORDER BY count DESC
            """, (cutoff_time,))
            
            terms_stats = cur.fetchall()
            
            if terms_stats:
                print("   Mensajes por término de búsqueda:")
                for term, count in terms_stats:
                    print(f"      - '{term}': {count} mensajes")
            
            # Mensajes con términos de odio (que tienen hits)
            cur.execute("""
                SELECT COUNT(DISTINCT rm.message_uuid)
                FROM raw_messages rm
                JOIN term_hits th ON rm.message_uuid = th.message_uuid
                WHERE rm.network = 'twitter' 
                  AND rm.collected_at >= %s
            """, (cutoff_time,))
            
            with_hits = cur.fetchone()[0]
            print(f"\n   Mensajes con coincidencias de términos de odio: {with_hits}")
            print(f"   Mensajes sin coincidencias: {total_count - with_hits}")


if __name__ == "__main__":
    check_recent_messages(hours_back=24, limit=10)

