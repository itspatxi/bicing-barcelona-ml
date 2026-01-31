"""
fetch_meteo_openmeteo_barcelona.py
==================================

Por qué este script:
- XEMA/Socrata está dando timeouts y errores raros.
- Open-Meteo es estable, sin API key, y da meteo horaria.

Qué hace:
- Descarga meteo HORARIA para Barcelona (coordenadas del centro)
- Variables:
  - temperature_2m
  - relative_humidity_2m
  - precipitation
  - wind_speed_10m
  - pressure_msl
- Divide por AÑOS para no pedir un mega JSON de golpe.
- Guarda:
  - data/raw/meteo_openmeteo/barcelona_hourly_YYYY.parquet
  - data/raw/meteo_openmeteo/barcelona_hourly_all.parquet (todo unido)

Nota pandas:
- En algunas versiones recientes, floor("H") ya no vale y hay que usar floor("h").
"""

from __future__ import annotations

import time
from pathlib import Path
from datetime import date

import pandas as pd
import requests


# =========================
# Config
# =========================

OUT_DIR = Path("data/raw/meteo_openmeteo")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Coordenadas centro de Barcelona (aprox Plaça Catalunya)
LAT = 41.3874
LON = 2.1686

# Rango que encaja con tu histórico Bicing (ajusta si quieres)
START_YEAR = 2019
END_YEAR = 2025  # inclusive

# API Open-Meteo (Histórico)
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variables horarias
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "pressure_msl",
]

TIMEZONE = "Europe/Madrid"


# =========================
# HTTP robusto
# =========================

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "bicing-barcelona-ml/0.1 (educational)",
        "Accept": "application/json",
        "Connection": "keep-alive",
    }
)


def get_json_with_retries(url: str, params: dict, max_retries: int = 5):
    """
    GET con reintentos y backoff para evitar fallos puntuales de red.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=(10, 60))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            print(f"   ⚠️ intento {attempt}/{max_retries} fallido: {type(e).__name__}")
            time.sleep(2 * attempt)
    raise last_err


# =========================
# Descarga por año
# =========================

def fetch_year(year: int) -> pd.DataFrame:
    """
    Descarga meteo horario para un año concreto y lo devuelve como DataFrame.
    """
    start = date(year, 1, 1).isoformat()
    end = date(year, 12, 31).isoformat()

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
    }

    print(f"Descargando Open-Meteo año {year}...")
    data = get_json_with_retries(BASE_URL, params=params)

    if "hourly" not in data or "time" not in data["hourly"]:
        raise RuntimeError(f"Respuesta inesperada Open-Meteo para {year}: keys={list(data.keys())}")

    hourly = data["hourly"]

    # time = lista de strings "YYYY-MM-DDTHH:MM"
    df = pd.DataFrame({"time": pd.to_datetime(hourly["time"], errors="coerce")})

    for v in HOURLY_VARS:
        df[v] = hourly.get(v, [pd.NA] * len(df))

    # Normalizamos a "hora" (en tu pandas es 'h' no 'H')
    df["time_hour"] = df["time"].dt.floor("h")

    return df


def main():
    all_years = []

    for y in range(START_YEAR, END_YEAR + 1):
        dfy = fetch_year(y)

        out_y = OUT_DIR / f"barcelona_hourly_{y}.parquet"
        dfy.to_parquet(out_y, index=False)
        print(f"   ✅ guardado: {out_y} | filas={len(dfy):,}")

        all_years.append(dfy)

    df_all = pd.concat(all_years, ignore_index=True).sort_values("time")
    out_all = OUT_DIR / "barcelona_hourly_all.parquet"
    df_all.to_parquet(out_all, index=False)

    print("\n==============================")
    print(f"✅ OK -> {out_all.resolve()}")
    print(f"Total filas: {len(df_all):,}")
    print(df_all.head(5).to_string(index=False))
    print("==============================\n")


if __name__ == "__main__":
    main()
