#!/usr/bin/env python3
"""
extraer_victimas_minint.py — Extrae de los informes anuales del Ministerio del Interior
("Informe sobre la evolución de los delitos de odio en España", 2018–2025) las tablas
de PERFIL DE LA VÍCTIMA y de delitos cometidos A TRAVÉS DE INTERNET, en formato largo.

Salidas (CSV, UTF-8):
  victimas_motivo_sexo.csv   anio, grupo_edad, motivo_code, motivo_pdf, sexo, n
      grupo_edad: TOTAL | MENORES | 18_25 | 26_40 | 41_50 | 51_65 | 65_MAS
      sexo: H | M | DESC (persona jurídica + sexo desconocido) | TOTAL
      motivo_code: códigos de delitos.* (RACISMO_XENOFOBIA, ORI_SEX_IDENT_GEN, ...)
                   + TOTAL_DELITOS, INFRAC_ADMIN, TOTAL_GENERAL
  victimas_nacionalidad.csv  anio, grupo_edad, nivel, region, pais, n
      nivel: ESPANA | EXTRANJEROS | REGION | PAIS | TOTAL
  internet_hechos_motivo.csv anio, motivo_code, motivo_pdf, hechos_conocidos, informe_fuente

Requiere `pdftotext` (poppler-utils). Uso:
  python3 extraer_victimas_minint.py --pdf-dir "Delitos de Odio" --out-dir salida_victimas
"""
from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

ANIOS = range(2018, 2026)

# ------------------------------------------------------------------ utilidades


def _sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", _sin_tildes(s).upper()).strip()


_NUM = r"\d{1,3}(?:\.\d{3})+|\d+"


def _int(tok: str) -> int:
    return int(tok.replace(".", ""))


def motivo_code(label: str) -> str | None:
    """Normaliza la etiqueta del PDF al código usado en delitos.* (None si no es un motivo)."""
    u = _norm(label)
    if not u:
        return None
    if "TOTAL" in u:
        if "INCIDENT" in u:
            return "TOTAL_GENERAL"
        if "DELITO" in u:
            return "TOTAL_DELITOS"
        return "TOTAL_SIN_ESPECIFICAR"  # "Total VICTIMIZACIONES" (se resuelve después)
    if "INFRAC" in u:
        return "INFRAC_ADMIN"
    if "ANTISEMIT" in u:
        return "ANTISEMITISMO"
    if "ANTIGITAN" in u:
        return "ANTIGITANISMO"
    if "APOROF" in u:
        return "APOROFOBIA"
    if "ISLAMOF" in u:
        return "ISLAMOFOBIA"
    if "CREENCIA" in u or "RELIGIO" in u:
        return "RELIGION"
    if "DISCAPACIDAD" in u or "DISFOBIA" in u or re.search(r"PERS(ONAS?)?\.? (CON )?DIS", u):
        return "DISCAPACIDAD"
    if "ORIENTA" in u or "ORIEN." in u or "ORIET" in u:
        return "ORI_SEX_IDENT_GEN"
    if "RACISMO" in u or "XENOFOB" in u:
        return "RACISMO_XENOFOBIA"
    if "IDEOLOG" in u:
        return "IDEOLOGIA"
    if "SEXO/GENERO" in u or "SEXO / GENERO" in u:
        return "DISCRIM_SEXO_GENERO"
    if "GENERAC" in u:
        return "DISCRIM_GENERACIONAL"
    if "ENFERM" in u or u.startswith("DISCRIMINACION POR RAZON"):
        return "DISCRIM_ENFERMEDAD"
    return None


def grupo_edad(linea: str) -> str | None:
    u = _norm(linea)
    # solo títulos de sección ("3.2 Perfil de la VÍCTIMA según grupo de edad: ..."), no frases del texto
    if "GRUPO DE EDAD" not in u or "PERFIL DE LA VICTIMA" not in u:
        return None
    if "MENORES" in u:
        return "MENORES"
    for a, b, code in (("18", "25", "18_25"), ("26", "40", "26_40"), ("41", "50", "41_50"), ("51", "65", "51_65")):
        if re.search(rf"{a}\s*A\s*{b}", u):
            return code
    if "65" in u and ("MAYOR" in u or "MAS DE" in u):
        return "65_MAS"
    return None


def columnas_cabecera(linea: str) -> list[str] | None:
    """Columnas de sexo de una cabecera 'VICTIMIZACIONES POR DELITOS ...'."""
    u = _norm(linea)
    if "VICTIMIZACIONES POR DELITOS" not in u or "TOTAL" not in u.split("VICTIMIZACIONES POR DELITOS", 1)[1]:
        return None
    resto = u.split("VICTIMIZACIONES POR DELITOS", 1)[1]
    cols = []
    for tok in re.findall(r"HOMBRES?|MUJER(?:ES)?|PERS?\.?\s*JUR[A-Z]*\.?(?:\s*/\s*DESC\.?)?|DESC\.?|TOTAL", resto):
        if tok.startswith("HOMBRE"):
            cols.append("H")
        elif tok.startswith("MUJER"):
            cols.append("M")
        elif tok.startswith("TOTAL"):
            cols.append("TOTAL")
        else:
            cols.append("DESC")
    if cols and "H" not in cols and "M" in cols:     # cabecera con «Hombre» perdido (p. ej. 2019, 41–50)
        cols.insert(0, "H")
    return cols if cols and cols[-1] == "TOTAL" else None


def pdf_a_texto(pdf: Path) -> list[str]:
    out = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


# ------------------------------------------------------- víctimas: motivo × sexo


def extraer_motivo_sexo(lineas: list[str], anio: int) -> list[dict]:
    filas: list[dict] = []
    contexto = None          # TOTAL | código de edad | None (fuera de perfil de víctima)
    en_victima = False
    i = 0
    while i < len(lineas):
        ln = lineas[i]
        u = _norm(ln)
        if re.search(r"PERFIL DE LA VICTIMA\s*:?\s*(AMBITO)", u) and not re.search(r"\.{5,}|\s\d{1,3}$", ln):
            en_victima, contexto = True, "TOTAL"
        elif "PERFIL DEL AUTOR" in u and not re.search(r"\.{5,}", ln):
            en_victima = False
        ge = grupo_edad(ln)
        if ge and en_victima and not re.search(r"\.{5,}", ln):
            contexto = ge
        cols = columnas_cabecera(ln) if en_victima else None
        if cols and contexto:
            pendiente = ""
            j = i + 1
            vistos_total_general = False
            while j < len(lineas) and j < i + 40:
                fila = lineas[j]
                fu = _norm(fila)
                if not fu:
                    j += 1
                    continue
                if fu.startswith("TIPO DE HECHO") or fu.startswith(">>") or "%" in fila:
                    break
                nums = re.findall(rf"(?<![\d,])({_NUM})(?![\d,%])", fila)
                etiqueta = re.split(rf"\s{{2,}}(?:{_NUM})\b", fila.strip())[0]
                etiqueta = re.sub(rf"\s+({_NUM})(\s+({_NUM}))*\s*$", "", etiqueta).strip()
                if len(nums) >= len(cols):
                    nums = nums[-len(cols):]
                    label = (pendiente + " " + etiqueta).strip()
                    pendiente = ""
                    code = motivo_code(label)
                    if code is None:
                        j += 1
                        continue
                    por_sexo: dict[str, int] = {}
                    for c, v in zip(cols, nums):          # Per.Jur. y Desc. separadas → se suman en DESC
                        por_sexo[c] = por_sexo.get(c, 0) + _int(v)
                    for c, v in por_sexo.items():
                        filas.append({"anio": anio, "grupo_edad": contexto, "motivo_code": code,
                                      "motivo_pdf": re.sub(r"\s+", " ", label), "sexo": c, "n": v})
                    if code in ("TOTAL_GENERAL",) or (code == "TOTAL_SIN_ESPECIFICAR"):
                        vistos_total_general = True
                        break
                else:
                    pendiente = (pendiente + " " + etiqueta).strip()
                j += 1
            i = j
            continue
        i += 1
    return _consolidar(filas)


def _consolidar(filas: list[dict]) -> list[dict]:
    """Une DESC duplicadas (Per.Jur. + Desc.) y resuelve 'Total VICTIMIZACIONES'."""
    acc: dict[tuple, dict] = {}
    for f in filas:
        k = (f["anio"], f["grupo_edad"], f["motivo_code"], f["sexo"])
        if k in acc:      # tabla repetida más adelante en el informe (síntesis) → se conserva la primera
            continue
        acc[k] = dict(f)
    # 'Total VICTIMIZACIONES' sin especificar: si no hay INFRAC_ADMIN en ese bloque → TOTAL_GENERAL = TOTAL_DELITOS
    out = list(acc.values())
    final = []
    for f in out:
        if f["motivo_code"] == "TOTAL_SIN_ESPECIFICAR":
            hay_infrac = any(g["anio"] == f["anio"] and g["grupo_edad"] == f["grupo_edad"]
                             and g["motivo_code"] == "INFRAC_ADMIN" for g in out)
            hay_td = any(g["anio"] == f["anio"] and g["grupo_edad"] == f["grupo_edad"]
                         and g["motivo_code"] == "TOTAL_DELITOS" for g in out)
            if hay_infrac or hay_td:
                f["motivo_code"] = "TOTAL_GENERAL"
                final.append(f)
            else:
                f2 = dict(f); f2["motivo_code"] = "TOTAL_DELITOS"
                f3 = dict(f); f3["motivo_code"] = "TOTAL_GENERAL"
                final += [f2, f3]
        else:
            final.append(f)
    for f in final:
        f.pop("_fila", None)
    return final


# ------------------------------------------------------------- nacionalidad

_REG = [("AFRICA", "AFRICA"), ("AMERICA", "AMERICA"), ("UNION", "UNION_EUROPEA"), ("EUROPEA", "UNION_EUROPEA"),
        ("ASIA", "ASIA"), ("RESTO", "RESTO_PAISES")]
_FILA_NAC = re.compile(rf"(?<![\d,.])({_NUM})\s+(\d{{1,3}}(?:,\d+)?)\s*%")
_REGION = re.compile(r"(?<!\d)2\.+(\d)\.?\s*[-\u2010\u2011\u2013]?\s*([A-Z][A-Z .]*)$")


def _pais(pre: str) -> str:
    trozo = re.split(r"\s{2,}", pre.strip())[-1]
    toks = trozo.split()
    # 2018–2019: restos de texto oculto delante del país ("RU MARRUECOS", "DO BULGARIA")
    if len(toks) > 1 and len(toks[0]) <= 3 and toks[0] not in ("EE.", "EEUU"):
        toks = toks[1:]
    return " ".join(toks)


def extraer_nacionalidad(lineas: list[str], anio: int) -> list[dict]:
    filas: list[dict] = []
    contexto, en_victima = None, False
    i = 0
    while i < len(lineas):
        ln = lineas[i]
        u = _norm(ln)
        if re.search(r"PERFIL DE LA VICTIMA\s*:?\s*(AMBITO|TIPOLOGIA)", u) and not re.search(r"\.{5,}", ln):
            en_victima = True
            if "AMBITO" in u:
                contexto = "TOTAL"
        elif "PERFIL DEL AUTOR" in u and not re.search(r"\.{5,}", ln):
            en_victima = False
        ge = grupo_edad(ln)
        if ge and en_victima and not re.search(r"\.{5,}", ln):
            contexto = ge
        # ">> Nacionalidad de la víctima" o, si falta ese título (2018–2019, 18–25), la cabecera de la tabla
        if en_victima and contexto and re.match(r"^\s*(>+\s*NACIONALIDAD|NACIONALIDAD\s+VICTIMAS)", u):
            region, region_pend = None, None
            j = i + 1
            while j < len(lineas) and j < i + 45:
                fu = _norm(lineas[j])
                m = _FILA_NAC.search(fu)
                if not m:
                    mr = _REGION.search(fu.strip())
                    if mr:                                   # "2.3 UNIÓN" partido en dos líneas
                        region_pend = next((c for k, c in _REG if k in mr.group(2)), None)
                    j += 1
                    continue
                n = _int(m.group(1))
                pre = fu[:m.start()].strip()
                base = {"anio": anio, "grupo_edad": contexto, "region": None, "pais": None, "n": n}
                mr = _REGION.search(pre)
                if re.search(r"\bTOTAL\b", pre) or pre.endswith("NACIONALIDAD"):
                    filas.append({**base, "nivel": "TOTAL"})
                    break
                vistos_niv = {f["nivel"] for f in filas if f["anio"] == anio and f["grupo_edad"] == contexto}
                if not pre and region is None:              # etiqueta ausente en el PDF (p. ej. 2022): por posición
                    pre = "ESPANA" if "ESPANA" not in vistos_niv else "EXTRANJEROS"
                if "ESPAN" in pre and not mr:
                    filas.append({**base, "nivel": "ESPANA"})
                elif "EXTRANJ" in pre:
                    filas.append({**base, "nivel": "EXTRANJEROS"})
                elif mr or (region_pend and pre.split()[-1:] == ["EUROPEA"]):
                    nombre = mr.group(2) if mr else "UNION EUROPEA"
                    region = next((c for k, c in _REG if k in nombre), nombre.strip())
                    filas.append({**base, "nivel": "REGION", "region": region})
                    region_pend = None
                elif region:
                    pais = _pais(pre)
                    if anio >= 2021:   # 2018–2020: nombres de país contaminados por texto oculto del PDF
                        filas.append({**base, "nivel": "PAIS", "region": region,
                                      "pais": "RESTO" if pais.startswith("RESTO") else pais})
                j += 1
            i = j
            continue
        i += 1
    vistos, out = set(), []
    for f in filas:
        k = (f["anio"], f["grupo_edad"], f["nivel"], f["region"], f["pais"])
        if k not in vistos:
            vistos.add(k)
            out.append(f)
    return out


# ------------------------------------------------------------- internet

def extraer_internet(lineas: list[str], informe: int) -> list[dict]:
    """Tabla «Hechos conocidos» de la sección 5.1 (delitos a través de internet y redes sociales).
    Devuelve los valores de todas las columnas-año de la tabla."""
    inicio = next((i for i, ln in enumerate(lineas)
                   if re.match(r"^\s*5\.1\b", ln) and "INTERNET" in _norm(ln) and not re.search(r"\.{5,}", ln)), None)
    if inicio is None:
        return []
    cab = next((i for i in range(inicio, min(inicio + 12, len(lineas)))
                if len(re.findall(r"\b20[12]\d\b", lineas[i])) >= 2), None)
    if cab is None:
        return []
    anios = [int(a) for a in re.findall(r"\b(20[12]\d)\b", lineas[cab])][:3]
    filas, pendiente = [], ""
    for fl in lineas[cab + 1: cab + 30]:
        fu = _norm(fl)
        if not fu:
            continue
        if fu.startswith(">>") or fu.startswith("TIPO DE HECHO") or "ESCLARECID" in fu:
            break
        m = re.search(rf"\s{{2,}}((?:{_NUM}|-)(?:\s+(?:{_NUM}|-))*)", fl)
        if not m:
            pendiente = (pendiente + " " + fl.strip()).strip()
            continue
        lab = (pendiente + " " + fl[:m.start()].strip()).strip()
        pendiente = ""
        vals = m.group(1).split()[:len(anios)]
        code = motivo_code(lab)
        if code == "TOTAL_SIN_ESPECIFICAR":      # fila "TOTAL" sin más: total de la tabla
            code = "TOTAL_GENERAL"
        if code is None or len(vals) < len(anios):
            continue
        for a, v in zip(anios, vals):
            filas.append({"anio": a, "motivo_code": code, "motivo_pdf": re.sub(r"\s+", " ", lab),
                          "hechos_conocidos": None if v == "-" else _int(v), "informe_fuente": informe})
        if code == "TOTAL_GENERAL":
            break
    return filas


# ------------------------------------------------------------- main

def _escribir(path: Path, filas: list[dict]) -> None:
    if not filas:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    pdf_dir, out = Path(args.pdf_dir), Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    sexo, nac, inet = [], [], []
    for anio in ANIOS:
        pdf = pdf_dir / f"{anio}.pdf"
        if not pdf.exists():   # otros nombres, p. ej. "Delitos de Odio 2025.pdf" (excluye Fiscalía / Memoria)
            cand = [x for x in sorted(pdf_dir.glob(f"*{anio}*.pdf"))
                    if not re.search(r"fiscal|memoria", x.name, re.IGNORECASE)]
            pdf = cand[0] if cand else pdf
        if not pdf.exists():
            print(f"[aviso] falta {pdf}", file=sys.stderr)
            continue
        lineas = pdf_a_texto(pdf)
        s = extraer_motivo_sexo(lineas, anio)
        n = extraer_nacionalidad(lineas, anio)
        t = extraer_internet(lineas, anio)
        print(f"{anio}: motivo×sexo={len(s)} filas, nacionalidad={len(n)}, internet={len(t)}")
        sexo += s
        nac += n
        inet += t

    # Internet: todas las filas de un año salen de un único informe (el del propio año; 2018 → informe 2019)
    fuente = {}
    for f in inet:
        a, r = f["anio"], f["informe_fuente"]
        if a not in fuente or abs(r - a) < abs(fuente[a] - a) or (r == a):
            fuente[a] = r if r == a or a not in fuente else min(fuente[a], r) if fuente[a] != a else fuente[a]
    mejor = {(f["anio"], f["motivo_code"]): f for f in inet if f["informe_fuente"] == fuente[f["anio"]]}
    inet_final = sorted(mejor.values(), key=lambda f: (f["anio"], f["motivo_code"]))

    _escribir(out / "victimas_motivo_sexo.csv", sexo)
    _escribir(out / "victimas_nacionalidad.csv", nac)
    _escribir(out / "internet_hechos_motivo.csv", inet_final)
    print(f"OK → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
