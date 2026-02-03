# -*- coding: utf-8 -*-
"""
Genera un calendario de festivos para Barcelona/Catalunya/España (2019-2025),
con 1 fila por fecha para evitar duplicados en los joins.

Salida:
  data/silver/festivos/festivos_bcn_2019_2025.csv

Columnas:
  - date (YYYY-MM-DD)
  - is_holiday (0/1)
  - scope (pipe-separated): spain|catalunya|barcelona
  - name  (pipe-separated): nombres (uno o varios)

Notas:
- Incluye festivos fijos estatales, autonómicos catalanes típicos y locales BCN (Mercè + Pasqua Granada).
- Fechas móviles calculadas por Pascua (Gregorio): Viernes Santo, Lunes de Pascua, Lunes de Pascua Granada.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import pandas as pd


# -----------------------------
# Utilidades
# -----------------------------

def find_project_root(start: Path) -> Path:
    """Sube directorios hasta encontrar una carpeta 'data' o '.git'."""
    cur = start.resolve()
    for _ in range(6):
        if (cur / "data").exists() or (cur / ".git").exists():
            return cur
        cur = cur.parent
    return start.resolve()


def easter_sunday_gregorian(year: int) -> date:
    """
    Computa Domingo de Pascua (calendario gregoriano) usando el algoritmo de Meeus/Jones/Butcher.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


@dataclass(frozen=True)
class Holiday:
    dt: date
    name: str
    scope: str  # "spain" | "catalunya" | "barcelona"


def add_or_merge(by_date: dict[date, dict], h: Holiday) -> None:
    """
    Mantiene 1 fila por fecha:
    - scope: conjunto -> "a|b|c"
    - name : conjunto -> "x|y|z"
    """
    if h.dt not in by_date:
        by_date[h.dt] = {
            "date": h.dt.isoformat(),
            "is_holiday": 1,
            "scope_set": {h.scope},
            "name_set": {h.name},
        }
    else:
        by_date[h.dt]["scope_set"].add(h.scope)
        by_date[h.dt]["name_set"].add(h.name)


# -----------------------------
# Reglas de festivos
# -----------------------------

def holidays_for_year(year: int) -> list[Holiday]:
    """
    Genera festivos relevantes para BCN (estatales + catalunya + locales BCN).
    """
    out: list[Holiday] = []

    # Estatales (fijos)
    out += [
        Holiday(date(year, 1, 1),  "Año Nuevo", "spain"),
        Holiday(date(year, 1, 6),  "Reyes", "spain"),
        Holiday(date(year, 5, 1),  "Fiesta del Trabajo", "spain"),
        Holiday(date(year, 8, 15), "Asunción", "spain"),
        Holiday(date(year, 10, 12), "Fiesta Nacional de España", "spain"),
        Holiday(date(year, 11, 1), "Todos los Santos", "spain"),
        Holiday(date(year, 12, 6), "Día de la Constitución", "spain"),
        Holiday(date(year, 12, 8), "Inmaculada Concepción", "spain"),
        Holiday(date(year, 12, 25), "Navidad", "spain"),
    ]

    # Catalunya (fijos típicos)
    out += [
        Holiday(date(year, 6, 24), "Sant Joan", "catalunya"),
        Holiday(date(year, 9, 11), "Diada Nacional de Catalunya", "catalunya"),
        Holiday(date(year, 12, 26), "Sant Esteve", "catalunya"),
    ]

    # Pascua (móviles)
    easter = easter_sunday_gregorian(year)
    good_friday = easter - timedelta(days=2)
    easter_monday = easter + timedelta(days=1)

    # En Catalunya: Viernes Santo y Lunes de Pascua suelen ser festivos
    out += [
        Holiday(good_friday, "Viernes Santo", "catalunya"),
        Holiday(easter_monday, "Lunes de Pascua", "catalunya"),
    ]

    # Local Barcelona: Pasqua Granada (Pentecostés lunes = 50 días después de Pascua)
    # (Easter Sunday + 50 = Monday of Pentecost)
    whit_monday = easter + timedelta(days=50)
    out += [
        Holiday(whit_monday, "Lunes de Pascua Granada (Segona Pasqua)", "barcelona"),
        Holiday(date(year, 9, 24), "La Mercè", "barcelona"),
    ]

    return out


def build_festivos_bcn(year_start: int, year_end: int) -> pd.DataFrame:
    by_date: dict[date, dict] = {}

    for y in range(year_start, year_end + 1):
        for h in holidays_for_year(y):
            add_or_merge(by_date, h)

    # Pasamos a DataFrame 1 fila por fecha
    rows = []
    for _, rec in sorted(by_date.items(), key=lambda x: x[0]):
        rows.append({
            "date": rec["date"],
            "is_holiday": rec["is_holiday"],
            "scope": "|".join(sorted(rec["scope_set"])),
            "name": "|".join(sorted(rec["name_set"])),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date  # date puro
    return df


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    root = find_project_root(Path.cwd())
    out_dir = root / "data" / "silver" / "festivos"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_csv = out_dir / "festivos_bcn_2019_2025.csv"

    df = build_festivos_bcn(2019, 2025)

    # Guardado
    df_out = df.copy()
    df_out["date"] = df_out["date"].astype(str)  # YYYY-MM-DD
    df_out.to_csv(out_csv, index=False, encoding="utf-8")

    # Prints (aquí estaba tu error)
    years_present = sorted(pd.to_datetime(df_out["date"]).dt.year.unique().tolist())

    print("✅ Festivos BCN generados")
    print("   OUT:", str(out_csv))
    print("   rows=", len(df_out))
    print("   min=", df_out["date"].min(), "max=", df_out["date"].max())
    print("   unique_dates=", df_out["date"].nunique())
    print("   years=", years_present)


if __name__ == "__main__":
    main()
