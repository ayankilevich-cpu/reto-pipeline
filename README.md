# ReTo — Pipeline y dashboard (repositorio oficial)

**Repositorio canónico (privado):** `ayankilevich-cpu/reto-pipeline`  
El desarrollo activo, el pipeline de datos, modelos y el dashboard viven aquí.

El repositorio público **`ayankilevich-cpu/reto`** queda como **referencia archivada**; no debe usarse para nuevas entregas ni como fuente de código.

---

## Qué incluye este proyecto

- **Dashboard** Streamlit modular (`automatizacion_diaria/dashboard.py`): monitorización de discurso de odio en redes, conexión a PostgreSQL (`reto_db`).
- **Pipeline y automatización** (`automatizacion_diaria/`, `Medios/`, `Etiquetado_Modelos/`, etc.).
- **Scrapers y utilidades** (`reto-scraper/`, `Limpieza/`, `X_Mensajes/`, …).

---

## Estructura del dashboard (modular desde jul 2026)

El dashboard Streamlit vive en `automatizacion_diaria/` con arquitectura modular:

- `dashboard.py` — entry point (118 líneas: config + auth + routing)
- `components/` — módulos compartidos (auth, constants, db_helpers, exports, ui, theme, layout)
- `secciones/` — 13 secciones independientes, una por archivo

Deploy activo: Streamlit Cloud (`proyectoreto.streamlit.app`)

---

## Arranque rápido del dashboard

1. Python 3.11+ recomendado.
2. Crear entorno virtual e instalar dependencias:

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Copiar variables de entorno a partir de `.env.example` → `.env` (sin subir `.env` a Git).
4. Credenciales de base de datos: el dashboard usa `db_utils` desde `automatizacion_diaria/` (vía `sys.path`).

5. Ejecutar (desde la raíz del repo):

   ```bash
   streamlit run automatizacion_diaria/dashboard.py
   ```

**Streamlit secrets:** si usás `secrets.toml`, debe quedar fuera del repo (ya está en `.gitignore`).

---

## Archivos relevantes en la raíz

| Archivo / carpeta | Uso |
|-------------------|-----|
| `automatizacion_diaria/` | Dashboard modular, pipeline diario, `db_utils` |
| `logo_reto.png`, `logos/` | Imágenes del dashboard |
| `legacy/` | Dashboard y scripts legacy (no productivos) |
| `Dockerfile`, `docker-compose.yml` | Despliegue opcional |

Los duplicados que antes vivían en la raíz (`terminos_exclusion_oficial.py`, `medios_validos.json`, `dashboard.py`) están en `automatizacion_diaria/` o en `legacy/`.

---

## Estructura (resumen)

- `automatizacion_diaria/` — Pipeline diario, `db_utils`, migraciones SQL.
- `Medios/` — YouTube, etiquetado, ML/LLM.
- `Etiquetado_Modelos/` — Artefactos de modelo (según `.gitignore` y excepciones).
- `reto-scraper/`, `X_Mensajes/`, `Limpieza/` — Ingesta y limpieza.

Detalle adicional en los `README.md` dentro de algunas subcarpetas.

---

## Seguridad

No commitear: `.env`, `credentials.json`, `secrets.toml`, datos personales sin anonimizar. Revisá `.gitignore` antes de un `git add -A`.

---

## Contacto con despliegue institucional (Cifal / Ciedes)

Para publicar en web ajena: acordar URL, si es enlace o iframe, y el procedimiento de actualización con quien administre el servidor.
