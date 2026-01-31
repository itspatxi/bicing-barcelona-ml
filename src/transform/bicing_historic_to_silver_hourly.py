"""
bicing_historic_to_silver_hourly.py
===================================

OBJETIVO (modo dummies):
- Tienes MUCHOS CSV mensuales de Bicing (muy grandes).
- Queremos convertirlos a un dataset "Silver" en formato horario:
    (station_id, time_hour_local) -> métricas agregadas

POR QUÉ:
- Para unir luego con:
  - meteo (Open-Meteo) por hora
  - festivos por fecha
- Y para entrenar modelos sin manejar 10M+ filas crudas.

ENTRADA:
- data/raw/bicing_historic/extracted/*.csv

SALIDA:
- data/silver/bicing_hourly/bicing_hourly_yyyymm.parquet  (uno por CSV/mes)
- data/silver/bicing_hourly/_manifest.csv                 (log de procesado)

IMPORTANTE (DST / cambio de hora):
- En octubre hay una hora ambigua (02:00 aparece 2 veces).
- Si haces floor("h") en hora local con tz, pandas puede explotar.
- Solución: redondear EN UTC (no hay ambigüedad) y luego convertir a Europe/Madrid.

NOTAS:
1) Si te falla to_parquet con error de "pyarrow" o "fastparquet":
   pip install pyarrow
2) Si un CSV no tiene last_reported (y sólo tiene info de estación),
   se marca como skipped_schema y se salta.
"""

from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


# =========================
# Config
# =========================

IN_DIR = Path("data/raw/bicing_historic/extracted")
OUT_DIR = Path("data/silver/bicing_hourly")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST = OUT_DIR / "_manifest.csv"

# Zona horaria local para joins humanos (meteo/festivos)
TZ_LOCAL = "Europe/Madrid"

# Tamaño de lectura por trozos (chunks) para no reventar RAM
CHUNK_SIZE = 500_000  # si tu PC va justo, baja a 200_000

# Colu
