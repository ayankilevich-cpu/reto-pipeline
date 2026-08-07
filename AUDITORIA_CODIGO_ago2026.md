# Auditoría de código — reto-pipeline (agosto 2026)

Estado general: el pipeline está bien construido y el trabajo reciente de performance y caché
(ya mergeado a `main`) se nota cuidado. El hallazgo más importante no es un bug nuevo: es que
el fix del incidente de `load_to_db.py` **ya existe** en una rama sin mergear, y el pipeline
falló ayer (jueves 6/8) exactamente por eso. Aparte de eso, hay 4 mejoras de tipo "media" (sobre
todo reintentos ante fallos transitorios de red/API) y algunos puntos menores de orden y
limpieza. No se encontraron secretos expuestos ni riesgos de inyección SQL.

**Resumen de hallazgos:** 1 Alta · 4 Media · 3 Baja/opcional.

---

## Hallazgos por severidad

### Alta (afecta datos, seguridad, o puede romper producción)

- **El fix del incidente ya existe, pero no está en `main`** — [`automatizacion_diaria/load_to_db.py:704-715`](automatizacion_diaria/load_to_db.py#L704-L715)

  Qué pasa: en `main` todavía hay una sola conexión a la base de datos compartida entre las
  9 etapas de carga (YouTube, X, LLM, etc.), y si una etapa falla, el código intenta hacer
  `conn.rollback()` *antes* de guardar el mensaje de error real en el log. El problema es que, si
  la conexión ya se cayó (como pasó ayer — Neon cerró la conexión en medio de la carga), ese
  mismo `rollback()` también falla, y ese segundo error "tapa" al primero: en el log de ayer
  aparece "No se pudo conectar a PostgreSQL: connection already closed", un mensaje engañoso
  que hace parecer que nunca se pudo conectar, cuando en realidad sí se conectó, cargó 139.079
  filas, y recién ahí se cortó la conexión.

  Ya existe un commit que arregla exactamente esto, en la rama `fix/load-to-db-error-handling`
  (commit `3833b74`, sin mergear a `main`): abre una conexión nueva por cada una de las 9 etapas
  (así si una se cae, no arrastra a las demás) y loggea el error real primero, antes de intentar
  cualquier limpieza.

  Por qué importa: es la causa directa de que la corrida del jueves 6/8 haya fallado, y va a
  seguir fallando de la misma forma hasta que esa rama se mergee.

  Sugerencia: mergear `fix/load-to-db-error-handling` a `main` (sin aplicar cambios adicionales
  — ya está listo).

---

### Media (mejora real pero no urgente)

- **El mismo patrón de "tapar el error" sigue latente en el helper de conexión** — [`automatizacion_diaria/db_utils.py:280-287`](automatizacion_diaria/db_utils.py#L280-L287) y [`automatizacion_diaria/components/db_helpers.py:46-59`](automatizacion_diaria/components/db_helpers.py#L46-L59)

  Qué pasa: incluso después de mergear el fix de arriba, la función compartida `get_conn()`
  (y su versión con pool en el dashboard, `_pooled_conn()`) sigue haciendo `conn.rollback()`
  dentro de un bloque `except`. Si esa conexión específica ya está muerta, el `rollback()` puede
  lanzar su propio error, y ese error reemplaza al mensaje corto que ve quien lee el log (aunque,
  a diferencia del caso de arriba, el detalle completo del error original sigue quedando guardado
  en el traceback gracias a `exc_info=True` donde se usa).

  Por qué importa: es una fuente de confusión de "bajo nivel" — cualquier función nueva que use
  `get_conn()` o `_pooled_conn()` hereda este comportamiento sin darse cuenta.

  Sugerencia: envolver el `conn.rollback()` en su propio `try/except` que ignore (o loggee aparte,
  a nivel debug) un fallo de rollback, para que el error original nunca se pise.

- **Sin reintentos ante fallos transitorios de red en Google Drive y YouTube** — [`X_Mensajes/sync_drive_csvs.py`](X_Mensajes/sync_drive_csvs.py) y [`Medios/youtube_extract_hate.py`](Medios/youtube_extract_hate.py)

  Qué pasa: ambos scripts llaman a APIs externas (Google Drive y YouTube Data API) sin ninguna
  lógica de reintento. `youtube_extract_hate.py` sí detecta bien cuando la API dice "se acabó la
  cuota del día" (`quota_exceeded`) y corta la corrida de forma prolija — pero un error de red
  transitorio (timeout, conexión reseteada) que no venga envuelto como `HttpError` no se
  reintenta, corta el script entero y esa etapa queda como fallida por el resto del día (recién
  se vuelve a intentar en el cron de mañana). El pipeline LLM (`pipeline_unificado/etiquetar_llm_unified.py`),
  en cambio, sí tiene reintentos con backoff — así que hay precedente ya usado en el repo.

  Por qué importa: un hipo momentáneo de red (algo común en corridas de 8 minutos en GitHub
  Actions) hace perder un día entero de datos de esa plataforma, cuando con 2-3 reintentos
  cortos probablemente se hubiera resuelto solo.

  Sugerencia: aplicar el mismo patrón de retry/backoff que ya usa `etiquetar_llm_unified.py` a
  las llamadas de Drive y YouTube.

- **`rate_limit` (429, transitorio) se trata igual que `quota_exceeded` (cuota diaria agotada)** — [`Medios/youtube_extract_hate.py:756-772`](Medios/youtube_extract_hate.py#L756-L772)

  Qué pasa: cuando la YouTube API devuelve un 429 ("demasiadas solicitudes, esperá un poco") el
  script corta la corrida completa y espera al cron de mañana, igual que si se hubiera agotado
  la cuota diaria (que sí tiene sentido cortar, porque no se puede hacer nada hasta que resetee).
  Un 429 en cambio suele resolverse esperando unos segundos y reintentando.

  Por qué importa: se está perdiendo un día entero de extracción de YouTube por algo que
  probablemente se resuelve con una pausa de 10-30 segundos.

  Sugerencia: distinguir los dos casos — para `rate_limit`, esperar un poco y reintentar un par
  de veces antes de cortar; para `quota_exceeded`, mantener el corte limpio actual.

- **El pipeline diario no avisa activamente cuando falla** — [`.github/workflows/daily.yml`](.github/workflows/daily.yml)

  Qué pasa: el workflow tiene una validación robusta de etapas críticas y un healthcheck que
  escribe en la base de datos, pero no hay ningún paso que mande un email, mensaje de Slack, o
  notificación de ningún tipo cuando algo falla. El único aviso visible es el banner rojo dentro
  del dashboard — que solo se ve si alguien entra a mirarlo. Esto fue justamente lo que pasó
  con el fallo del jueves: quedó registrado en GitHub Actions y en el banner, pero nadie se
  entera hasta que abre el dashboard o revisa Actions manualmente.

  Por qué importa: un fallo puede pasar desapercibido varios días si nadie entra al dashboard.

  Sugerencia: agregar un paso final (`if: failure()`) que mande una notificación simple —
  GitHub ya tiene acciones prearmadas para email o Slack, no hace falta escribir nada custom.

---

### Baja / opcional (calidad de código, no afecta funcionamiento)

- **Dependencia declarada pero sin uso real** — [`requirements.txt:33`](requirements.txt#L33) y [`automatizacion_diaria/requirements.txt:11`](automatizacion_diaria/requirements.txt#L11)

  `streamlit-extras` está en los dos archivos de dependencias pero no se encontró ningún
  `import` de esa librería en el dashboard activo (`dashboard.py`, `components/`, `secciones/`).
  No rompe nada, pero agrega tiempo de instalación de más en cada corrida. Confirmar si se usa
  en algún lado que no se haya detectado antes de sacarla.

- **Algunas funciones de renderizado son muy largas** — por ejemplo [`secciones/gold_dataset.py:28`](automatizacion_diaria/secciones/gold_dataset.py#L28) (`render_gold_dataset`, ~550 líneas), [`secciones/analisis_510.py:458`](automatizacion_diaria/secciones/analisis_510.py#L458) (`_render_art510_preview`, ~326 líneas), y [`secciones/anotacion_validacion.py:2110`](automatizacion_diaria/secciones/anotacion_validacion.py#L2110) / [`:2432`](automatizacion_diaria/secciones/anotacion_validacion.py#L2432) (`_fragment_validacion_llm_youtube` / `_fragment_validacion_llm_x`, ~250 líneas cada una).

  Mezclan carga de datos, cálculos y elementos visuales de Streamlit todo en una sola función.
  Funcionan bien tal cual están; dividirlas en sub-funciones más chicas (ej. separar "traer los
  datos" de "dibujar la tabla") las haría más fáciles de tocar sin miedo a romper algo en otra
  parte de la misma función.

- **Dos workflows secundarios sin límite de tiempo explícito** — [`.github/workflows/wakeup_apps.yml:10`](.github/workflows/wakeup_apps.yml#L10) y [`.github/workflows/keep_hf_space_warm.yml:9`](.github/workflows/keep_hf_space_warm.yml#L9)

  A diferencia de `daily.yml` (90 min) y `ci.yml` (10 min), estos dos no declaran
  `timeout-minutes`, así que usan el default de GitHub (6 horas). `keep_hf_space_warm.yml` está
  acotado igual por sus propios `--max-time` en `curl`, pero `wakeup_apps.yml` usa Playwright
  para hacer clic en un botón — si ese botón cambia de lugar o desaparece, Playwright podría
  quedarse esperando bastante más de lo razonable antes de fallar.

  Sugerencia: agregar `timeout-minutes: 5` (o similar) a ambos jobs, más por prolijidad que por
  un riesgo grave hoy.

---

## Código posiblemente muerto (a confirmar con Alejandro)

- **`dashboard_v3.py` en la raíz del repo y `automatizacion_diaria/dashboard_v3.py`** — son dos
  copias idénticas entre sí (~11.775 líneas cada una), y ninguna de las dos coincide con la
  versión "oficial" que usa el script `sync_hf.sh` para desplegar al Space de Hugging Face
  (`automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py`). Estas dos copias no se tocaron
  desde el 29/6/2026 (commit `07c7b36`), mientras que la versión oficial se actualizó por
  última vez el 27/7/2026 — están desactualizadas y nada en el pipeline activo las importa o
  las usa. Podrían ser residuos de antes de que el archivo se moviera a la carpeta "Diseñador
  Web Reto/". No quedó claro si se dejaron ahí a propósito (¿backup?) o por accidente.

- **`automatizacion_diaria/Diseñador Web Reto/dashboard_v2.py`** — su propio docstring dice
  "ha sido superado por dashboard_v3.py" y el `CLAUDE.md` del repo indica explícitamente "no
  modificar ni deployar". Ya está identificado como obsoleto por el propio equipo; se menciona
  acá solo para que quede registrado en la auditoría.

- **`legacy/dashboard_legacy.py`** (7.210 líneas) y **`legacy/analisis_contexto_semanal_legacy.py`**
  — ya viven en una carpeta llamada `legacy/`, así que están correctamente señalizados como no
  activos. No se revisaron en profundidad por estar fuera del alcance pedido.

- **Nota aclaratoria:** `automatizacion_diaria/Diseñador Web Reto/dashboard_v3.py` (el archivo
  real, no sus copias huérfanas) **no es código muerto** — es la fuente de verdad de un segundo
  dashboard, desplegado aparte en Hugging Face Space, mantenido en sincronía con `sync_hf.sh`.
  Quedó fuera del alcance de esta auditoría porque el pedido original apuntaba al dashboard de
  Streamlit Cloud (`automatizacion_diaria/dashboard.py` + `components/` + `secciones/`). Si en
  algún momento se quiere auditar ese segundo dashboard, hace falta una revisión aparte — es un
  archivo monolítico de casi 12.000 líneas.

---

## Lo que ya está bien (no hace falta tocar)

- **Las consultas SQL están parametrizadas de forma consistente en todo el código revisado.**
  Incluso donde arman el `WHERE` dinámicamente con f-strings (para agregar filtros opcionales),
  los valores reales siempre viajan por separado vía `%s` / `params=`, nunca interpolados
  directo en el string. No se encontró ningún caso real de riesgo de inyección SQL.
- **El `.gitignore` cubre bien lo sensible**: credenciales, `.env`, `secrets.toml`, datos en CSV.
  No hay secretos ni datos sin anonimizar versionados en el repo.
- **Las funciones sin `@st.cache_data` que aparecieron en la revisión están así a propósito y
  documentado** (ej. la muestra aleatoria de mensajes LLM, o las colas de anotación pendiente,
  que necesitan verse siempre actualizadas) — no es un descuido, es una decisión correcta caso
  por caso.
- **El trabajo de performance ya mergeado a `main`** (caché precalentado en el arranque del
  dashboard, TTLs ajustados, pool de conexiones reutilizado entre reruns de Streamlit) se ve
  bien pensado y ya resuelve buena parte de lo que esta auditoría iba a buscar en esa categoría.
