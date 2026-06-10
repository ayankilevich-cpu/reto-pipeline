# CLAUDE.md — Proyecto ReTo · Dashboard

Este archivo es leído automáticamente por Claude Code y Claude (Cowork) al abrir este proyecto.

---

## Archivo principal

`dashboard_v3.py` es la **única fuente de verdad** del dashboard.  
`dashboard_v2.py` está obsoleto — no modificar ni deployar.

---

## Estructura de repositorios

| Repositorio | Carpeta local | Destino |
|-------------|---------------|---------|
| Principal | `Clases/RETO/` | GitHub: `github.com/ayankilevich-cpu/reto-pipeline` |
| HuggingFace Space | `Clases/RETO/hf-space-temp/` | `huggingface.co/spaces/aleyanki13/proyectoreto` |

**El Dockerfile del Space HF corre `automatizacion_diaria/dashboard.py`**, no `dashboard_v3.py`.  
El script `sync_hf.sh` copia uno en el otro automáticamente.

---

## Flujo de deploy obligatorio

Cada vez que se modifica `dashboard_v3.py`, ejecutar **los dos pasos**:

```bash
cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO"

# 1. GitHub
git add "automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py"
git commit -m "descripción"
git push

# 2. HuggingFace
./sync_hf.sh "descripción"
```

Hacer solo el paso 1 deja HuggingFace desactualizado.  
Hacer solo el paso 2 deja GitHub desactualizado.

---

## Archivos que NO commitear al repo GitHub desde esta carpeta

- `.streamlit/config.toml` del Space HF
- `hf-space-temp/automatizacion_diaria/dashboard.py`
- Cualquier archivo de `hf-space-temp/`

---

## Base de datos

- PostgreSQL en **Neon** (serverless)
- Base: `reto_db`
- Schema `raw.*` → solo admin/editor
- Schema `processed.*` → viewer y HuggingFace público
- Conexión via `DATABASE_URL` en secrets

---

## Roles de usuario

| Rol | Acceso |
|-----|--------|
| `admin` | Todo |
| `editor` | Todo excepto Comparativa modelos |
| `viewer` | Panel público (sin anotación, calidad LLM, Art. 510, comparativa) |

El viewer se asigna automáticamente sin login. Admin y editor requieren credenciales en `st.secrets["users"]`.

---

## Seguridad — notas importantes

- Las contraseñas en secrets deben estar hasheadas con `pbkdf2_hmac`.  
  Para generar un hash: ejecutar `_hash_password("contraseña")` en Python con el módulo del dashboard.
- Si los secrets tienen contraseñas en plain text, el admin verá un aviso en el sidebar.
- Rate limiting activo: bloqueo 5 min tras 5 intentos fallidos.
- Timeout de sesión: 8 horas para admin/editor.
