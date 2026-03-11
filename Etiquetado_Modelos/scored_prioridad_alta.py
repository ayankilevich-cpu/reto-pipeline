"""
Filtrar Mensajes de Prioridad Alta
==================================
Script que toma el CSV scored por score_baseline.py y genera un CSV
con solo las filas de prioridad alta (proba_odio >= 0.55).

Este CSV filtrado se usa como entrada para etiquetar_completo_llm.py

Uso:
    python scored_prioridad_alta.py
    python scored_prioridad_alta.py --input otro_scored.csv --output otro_destino.csv
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

# =============================================================================
# CONFIGURACIÓN - Rutas por defecto
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV de entrada (generado por score_baseline.py)
DEFAULT_INPUT_CSV = os.path.join(SCRIPT_DIR, "x_manual_label_scored.csv")

# CSV de salida (para etiquetar_completo_llm.py)
DEFAULT_OUTPUT_CSV = os.path.join(SCRIPT_DIR, "x_manual_label_scored_prioridad_alta.csv")

# Filtro de prioridad (por defecto "alta")
DEFAULT_PRIORITY = "alta"

# =============================================================================
# FUNCIONES
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filtrar mensajes de prioridad alta del CSV scored"
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=DEFAULT_INPUT_CSV,
        help=f"CSV de entrada (scored). Default: {DEFAULT_INPUT_CSV}"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=DEFAULT_OUTPUT_CSV,
        help=f"CSV de salida (filtrado). Default: {DEFAULT_OUTPUT_CSV}"
    )
    parser.add_argument(
        "--priority", "-p",
        type=str,
        default=DEFAULT_PRIORITY,
        choices=["alta", "media", "baja"],
        help=f"Nivel de prioridad a filtrar. Default: {DEFAULT_PRIORITY}"
    )
    parser.add_argument(
        "--min-proba",
        type=float,
        default=None,
        help="Filtrar por proba_odio mínima (alternativa a --priority). Ej: 0.55"
    )
    parser.add_argument(
        "--include-media",
        action="store_true",
        help="Incluir también prioridad media además de alta"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    print("=" * 70)
    print("FILTRADO DE MENSAJES - PRIORIDAD ALTA")
    print("=" * 70)
    
    # -------------------------------------------------------------------------
    # 1. Validar archivo de entrada
    # -------------------------------------------------------------------------
    if not os.path.exists(args.input):
        print(f"\nERROR: No se encontró el archivo de entrada:")
        print(f"  {args.input}")
        print("\nAsegúrate de ejecutar primero score_baseline.py para generar el CSV scored.")
        return 1
    
    # -------------------------------------------------------------------------
    # 2. Cargar CSV
    # -------------------------------------------------------------------------
    print(f"\nCargando: {args.input}")
    df = pd.read_csv(args.input)
    total_original = len(df)
    print(f"  - Filas totales: {total_original}")
    
    # Verificar columnas requeridas
    columnas_requeridas = ['priority', 'proba_odio']
    columnas_faltantes = [c for c in columnas_requeridas if c not in df.columns]
    
    if columnas_faltantes:
        print(f"\nERROR: Faltan columnas requeridas: {columnas_faltantes}")
        print("Columnas disponibles:", list(df.columns))
        print("\nEl CSV debe haber sido procesado por score_baseline.py")
        return 1
    
    # -------------------------------------------------------------------------
    # 3. Mostrar distribución actual
    # -------------------------------------------------------------------------
    print("\nDistribución de prioridades en el archivo:")
    priority_counts = df['priority'].value_counts()
    for prio in ['alta', 'media', 'baja']:
        count = priority_counts.get(prio, 0)
        pct = 100 * count / total_original if total_original > 0 else 0
        print(f"  - {prio.capitalize():6s}: {count:6d} ({pct:5.1f}%)")
    
    # -------------------------------------------------------------------------
    # 4. Aplicar filtro
    # -------------------------------------------------------------------------
    print("\nAplicando filtro...")
    
    if args.min_proba is not None:
        # Filtrar por probabilidad mínima
        df_filtrado = df[df['proba_odio'] >= args.min_proba].copy()
        filtro_desc = f"proba_odio >= {args.min_proba}"
    elif args.include_media:
        # Incluir alta y media
        df_filtrado = df[df['priority'].isin(['alta', 'media'])].copy()
        filtro_desc = "priority IN ('alta', 'media')"
    else:
        # Solo la prioridad especificada
        df_filtrado = df[df['priority'] == args.priority].copy()
        filtro_desc = f"priority == '{args.priority}'"
    
    total_filtrado = len(df_filtrado)
    print(f"  - Filtro aplicado: {filtro_desc}")
    print(f"  - Filas filtradas: {total_filtrado} de {total_original} ({100*total_filtrado/total_original:.1f}%)")
    
    if total_filtrado == 0:
        print("\nADVERTENCIA: No hay filas que cumplan el criterio de filtrado.")
        print("No se generará archivo de salida.")
        return 0
    
    # -------------------------------------------------------------------------
    # 5. Ordenar por probabilidad (más probable primero)
    # -------------------------------------------------------------------------
    df_filtrado = df_filtrado.sort_values('proba_odio', ascending=False)
    
    # -------------------------------------------------------------------------
    # 6. Guardar CSV filtrado
    # -------------------------------------------------------------------------
    # Crear directorio si no existe
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    df_filtrado.to_csv(args.output, index=False, encoding='utf-8')
    print(f"\nCSV guardado: {args.output}")
    
    # -------------------------------------------------------------------------
    # 7. Resumen
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"Archivo de entrada:  {args.input}")
    print(f"Archivo de salida:   {args.output}")
    print(f"Filtro aplicado:     {filtro_desc}")
    print(f"Filas originales:    {total_original}")
    print(f"Filas filtradas:     {total_filtrado}")
    
    if total_filtrado > 0:
        print(f"\nEstadísticas de proba_odio (filtradas):")
        print(f"  - Media:   {df_filtrado['proba_odio'].mean():.4f}")
        print(f"  - Min:     {df_filtrado['proba_odio'].min():.4f}")
        print(f"  - Max:     {df_filtrado['proba_odio'].max():.4f}")
    
    print("\n" + "=" * 70)
    print("SIGUIENTE PASO")
    print("=" * 70)
    print(f"El archivo '{os.path.basename(args.output)}' está listo para ser procesado con:")
    print(f"  python etiquetar_completo_llm.py")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
