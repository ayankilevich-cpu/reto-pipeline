#!/usr/bin/env python3
"""Script rápido para verificar mensajes guardados en PostgreSQL."""

import sys
from pathlib import Path

import psycopg
import yaml

# Cargar configuración
config_dir = Path(__file__).parent.parent / "config"
secrets_file = config_dir / "secrets.yaml"

if not secrets_file.exists():
    print(f"❌ No se encontró secrets.yaml en {secrets_file}")
    sys.exit(1)

with secrets_file.open() as f:
    secrets = yaml.safe_load(f)

postgres = secrets["postgres"]

# Conectar
try:
    conn = psycopg.connect(
        host=postgres["host"],
        port=postgres.get("port", 5432),
        dbname=postgres["database"],
        user=postgres["user"],
        password=postgres["password"],
    )
except Exception as e:
    print(f"❌ Error conectando a PostgreSQL: {e}")
    sys.exit(1)

print("=" * 60)
print("📊 RESUMEN DE MENSAJES EN POSTGRESQL")
print("=" * 60)

with conn.cursor() as cur:
    # 1. Total de mensajes por red
    print("\n1️⃣ Total de mensajes por red social:")
    print("-" * 60)
    cur.execute("""
        SELECT network, COUNT(*) as total_mensajes 
        FROM raw_messages 
        GROUP BY network 
        ORDER BY total_mensajes DESC
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:15} {row[1]:>10} mensajes")
    
    # 2. Mensajes con términos de odio detectados
    print("\n2️⃣ Mensajes con términos de odio detectados:")
    print("-" * 60)
    cur.execute("""
        SELECT 
            rm.network,
            COUNT(DISTINCT rm.message_uuid) as mensajes_con_odio,
            COUNT(th.hit_id) as total_coincidencias,
            COUNT(DISTINCT th.term) as terminos_unicos
        FROM raw_messages rm
        INNER JOIN term_hits th ON th.message_uuid = rm.message_uuid
        GROUP BY rm.network
    """)
    results = cur.fetchall()
    if results:
        for row in results:
            print(f"   {row[0]:15} {row[1]:>6} mensajes | {row[2]:>6} coincidencias | {row[3]:>4} términos únicos")
    else:
        print("   (No hay mensajes con términos de odio detectados)")
    
    # 3. Estado de los jobs
    print("\n3️⃣ Estado de los jobs:")
    print("-" * 60)
    cur.execute("""
        SELECT 
            network,
            status,
            COUNT(*) as cantidad
        FROM crawl_jobs 
        GROUP BY network, status 
        ORDER BY network, status
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:15} {row[1]:12} {row[2]:>6} jobs")
    
    # 4. Últimos mensajes capturados
    print("\n4️⃣ Últimos 5 mensajes capturados:")
    print("-" * 60)
    cur.execute("""
        SELECT 
            network,
            LEFT(content, 80) as contenido_preview,
            published_at,
            collected_at
        FROM raw_messages 
        ORDER BY collected_at DESC 
        LIMIT 5
    """)
    for row in cur.fetchall():
        fecha = row[3].strftime("%Y-%m-%d %H:%M") if row[3] else "N/A"
        print(f"   [{row[0]}] {fecha}")
        print(f"   {row[1]}...")
        print()
    
    # 5. Resumen del dashboard
    print("\n5️⃣ Resumen diario (últimos 7 días):")
    print("-" * 60)
    cur.execute("""
        SELECT * FROM v_dashboard_summary 
        ORDER BY collected_day DESC, network 
        LIMIT 14
    """)
    for row in cur.fetchall():
        fecha = row[0].strftime("%Y-%m-%d") if row[0] else "N/A"
        print(f"   {fecha} | {row[1]:15} | {row[2]:>6} msgs | {row[3]:>6} hits | {row[4]:>4} términos")

print("\n" + "=" * 60)
conn.close()
















