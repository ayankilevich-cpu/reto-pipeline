import tabula
import pandas as pd
from pathlib import Path

# ================================
# CONFIGURACIÓN
# ================================
BASE_PATH = Path("/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Delitos de Odio")
OUTPUT_PATH = BASE_PATH / "output_csv_debug"
OUTPUT_PATH.mkdir(exist_ok=True)

PDF_TEST = BASE_PATH / "2024.pdf"   # probamos solo con 2024

# ================================
# LECTURA Y VOLCADO DE TODAS LAS TABLAS
# ================================

def extraer_todas_las_tablas(pdf_path):
    print(f"\n🔎 Leyendo TODAS las tablas de: {pdf_path.name}")

    tables = tabula.read_pdf(
        str(pdf_path),
        pages="all",
        multiple_tables=True,
        lattice=True
    )

    if not tables:
        print("⚠️ No se encontró ninguna tabla en el PDF.")
        return

    print(f"📊 Tablas encontradas: {len(tables)}")

    for i, df in enumerate(tables):
        df = df.astype(str)
        out_file = OUTPUT_PATH / f"DEBUG_2024_tabla_{i}.csv"
        df.to_csv(out_file, index=False, encoding="utf-8")
        print(f"   ➜ Tabla {i}: shape={df.shape}, guardada como {out_file.name}")


if __name__ == "__main__":
    extraer_todas_las_tablas(PDF_TEST)
    print("\n✅ Fin del modo diagnóstico.")