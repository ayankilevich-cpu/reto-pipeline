# MD-012 — Actualización diccionario hate_terms_clean.csv (2026-10-20)

**Fecha de entrada en producción:** 2026-10-20  
**Rama de trabajo:** `diccionario/descarte-20261019`  
**Commit:** 9fcbdc2  
**Mergeado a main:** 2026-10-20

---

## Resumen del cambio

`Medios/hate_terms_clean.csv` pasó de **4 937** a **4 905** lemas netos.

| Operación | Términos | Cantidad |
|---|---|---|
| Descartados (sesiones 19-oct) | degenerado, distinguido, mariconada, miserablemente, analfabeta, asesinara, bloqueados, botarate, carguito, caribeno, catalanofobia, chupando, disentir, disparates, estereotipo, iluminada, pardillos, rumano, taurino, tiburon, vascos, discurso del odio, izquierda populista, vendido de, pobres mujeres, vender humo, inmigrante irregular, inmigracion irregular, situacion vulnerable, progre comunista, abusos policiales, dictadura socialista | −32 |
| Deduplicado | golpistas (filas 679-680 en base → 1) | −1 |
| Añadido nuevo lema | golpista (independiente de golpistas) | +1 |
| **Neto** | | **−32** |

Términos restaurados en esta sesión (estaban en el base y habían sido quitados):  
`odian`, `sobra`, `mafia`, `huevos`, `sinverguenzas`, `gilipolleces` — todos conservados (net 0 vs base).

---

## Impacto medido (dry-run)

- Ventana: últimos 7 días disponibles en master CSV (2026-09-24 → 2026-09-30)
- Mensajes analizados: 15 051 (X + YouTube)

| | Matches |
|---|---|
| Main (antes) | 8 760 |
| Branch (nuevo diccionario) | 8 751 |
| Diferencia | −9 |

Los −9 matches son mensajes capturados únicamente por alguno de los 32 términos descartados. El nuevo lema `golpista` no añade matches netos (todos esos mensajes ya contenían `golpistas`).

---

## Validación del archivo

- Encoding: UTF-8 ✓  
- Saltos de línea: CRLF (igual que versión anterior) ✓  
- Cabecera: `Lemas` ✓  
- Filas vacías: 0 ✓  
- Duplicado pre-existente no introducido por esta rama: `enganarlos` (x2) — presente también en main  
- `git diff --stat`: `1 file changed, 1 insertion(+), 33 deletions(-)` ✓

---

## Puntos de consumo del diccionario

Todos los scripts leen el archivo por path relativo al repo o al script; ninguno tiene `ref: main` hardcodeado.  
Excepción: `Medios/test_matching.py` usa path absoluto hardcodeado (no afecta CI).  
El workflow `daily.yml` usa el archivo del checkout activo (main tras el merge).
