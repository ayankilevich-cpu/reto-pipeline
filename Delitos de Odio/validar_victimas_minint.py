#!/usr/bin/env python3
"""Controles de coherencia de los CSV de extraer_victimas_minint.py.
Uso: python3 validar_victimas_minint.py <carpeta_csv>
Sale con código 1 si hay problemas no conocidos. Errores conocidos del propio PDF se listan aparte."""
import sys
import pandas as pd

# Inconsistencias presentes en los propios informes (no son errores de extracción)
CONOCIDOS = {
    "2019 65_MAS: regiones 6 != EXT 4",
    "2024 51_65: países AMERICA 34 != región 33",
    "2025 18_25: regiones 155 != EXT 168",           # el PDF imprime regiones que suman 37,7 % vs 40,9 % extranjeros
    "2025 65_MAS: regiones 9 != EXT 11",             # UE figura con 0 pero lista Italia 1 y Francia 1
    "2025 65_MAS: países UNION_EUROPEA 2 != región 0",
}
TOT = {"TOTAL_DELITOS", "INFRAC_ADMIN", "TOTAL_GENERAL"}
carpeta = sys.argv[1]
probs = []

s = pd.read_csv(f"{carpeta}/victimas_motivo_sexo.csv")
for (a, g), b in s.groupby(["anio", "grupo_edad"]):
    p = b.pivot_table(index="motivo_code", columns="sexo", values="n", aggfunc="sum").fillna(0)
    for c in ("H", "M", "DESC", "TOTAL"):
        if c not in p:
            p[c] = 0
    for m, r in p[(p.H + p.M + p.DESC) != p.TOTAL].iterrows():
        probs.append(f"{a} {g} {m}: H+M+DESC != TOTAL")
    mot = p.loc[[i for i in p.index if i not in TOT]]
    if "TOTAL_DELITOS" not in p.index:
        probs.append(f"{a} {g}: falta TOTAL_DELITOS")
        continue
    for c in ("H", "M", "DESC", "TOTAL"):
        if mot[c].sum() != p.loc["TOTAL_DELITOS", c]:
            probs.append(f"{a} {g}: suma motivos {c} != TOTAL_DELITOS")
grupos = s.groupby("anio")["grupo_edad"].nunique()
probs += [f"{a}: solo {k} grupos de edad" for a, k in grupos.items() if k != 7]

n = pd.read_csv(f"{carpeta}/victimas_nacionalidad.csv")
tg = s[(s.motivo_code == "TOTAL_GENERAL") & (s.sexo == "TOTAL")].set_index(["anio", "grupo_edad"])["n"]
for (a, g), b in n.groupby(["anio", "grupo_edad"]):
    v = lambda lv: b[b.nivel == lv]["n"].sum()
    es, ex, tot = v("ESPANA"), v("EXTRANJEROS"), v("TOTAL")
    reg, pai = b[b.nivel == "REGION"], b[b.nivel == "PAIS"]
    if es + ex != tot:
        probs.append(f"{a} {g}: ESP {es} + EXT {ex} != TOTAL {tot}")
    if reg.n.sum() != ex:
        probs.append(f"{a} {g}: regiones {reg.n.sum()} != EXT {ex}")
    for r, rb in pai.groupby("region"):
        if rb.n.sum() != reg[reg.region == r].n.sum():
            probs.append(f"{a} {g}: países {r} {rb.n.sum()} != región {reg[reg.region == r].n.sum()}")
    if (a, g) in tg.index and tg[(a, g)] != tot:
        probs.append(f"{a} {g}: TOTAL nacionalidad {tot} != TOTAL víctimas {tg[(a, g)]}")

i = pd.read_csv(f"{carpeta}/internet_hechos_motivo.csv")
for a, b in i.groupby("anio"):
    mot = b[~b.motivo_code.isin(TOT)].hechos_conocidos.sum()
    ref = b[b.motivo_code == "TOTAL_DELITOS"].hechos_conocidos.sum() or b[b.motivo_code == "TOTAL_GENERAL"].hechos_conocidos.sum()
    if mot != ref:
        probs.append(f"internet {a}: suma motivos {mot} != total {ref}")

nuevos = [p for p in probs if p not in CONOCIDOS]
print(f"Filas: motivo×sexo={len(s)}, nacionalidad={len(n)}, internet={len(i)}")
print(f"Errores conocidos del PDF: {len([p for p in probs if p in CONOCIDOS])}")
print(f"Problemas nuevos: {len(nuevos)}")
print("\n".join(nuevos))
sys.exit(1 if nuevos else 0)
