"""
rebuild_gold_clean.py
=====================

OBJETIVO:
- Tu gold se ha generado, pero han aparecido filas con time_hour=1970-01-01.
- Eso es casi seguro un "epoch 0" / parse erróneo.
- Este script NO vuelve a procesar los CSV gigantes:
  reutiliza las partes GOLD ya generadas en data/gold/parts/.

QUÉ HACE:
1) Lee cada gold_YYYYMM.parquet de data/gold/parts/
2) Limpia y filtra time_hour:
   - datetime válido
   - rango esperado: 2019-01-01 00:00:00 hasta 2025-12-31 23:00:00
3) Guarda:
   - data/gold/bicing_gold_clean.parquet
   - data/gold/bicing_gold_clean_sample.csv
4) Reporta:
   - filas antes/después
   - cuántas filas "1970" había
   - min/max time_hour reales

NOTA:
- Si quieres ampliar rango (ej: 2018 o 2026), cambia START/END.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd


# =========================
# Config
# =========================

PARTS_DIR = Path("data/gold/parts")
OUT_DIR = Path("data/gold")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_GOLD_CLEAN = OUT_DIR / "bicing_gold_clean.parquet"
OUT_SAMPLE_CLEAN = OUT_DIR / "bicing_gold_clean_sample.csv"

# Rango esperado (según tus meteo 2019-2025)
START = pd.Timestamp("2019-01-01 00:00:00")
END = pd.Timestamp("2025-12-31 23:00:00")

SAMPLE_N = 200_000


# =========================
# Main
# =========================

def main():
    parts = sorted(PARTS_DIR.glob("gold_*.parquet"))
    if not parts:
        raise RuntimeError(f"No encuentro partes en: {PARTS_DIR.resolve()}")

    total_in = 0
    total_out = 0
    bad_1970 = 0

    cleaned_parts = []

    print(f"Encontradas {len(parts)} partes GOLD en {PARTS_DIR}")
    print(f"Rango permitido: {START} -> {END}")

    for p in parts:
        df = pd.read_parquet(p)
        total_in += len(df)

        # Normalizamos time_hour
        df["time_hour"] = pd.to_datetime(df["time_hour"], errors="coerce")

        # Contar 1970 antes de filtrar (solo para informe)
        bad_1970 += int((df["time_hour"] < START).sum())

        # Filtrado rango
        df = df.dropna(subset=["time_hour"])
        df = df[(df["time_hour"] >= START) & (df["time_hour"] <= END)].copy()

        total_out += len(df)

        cleaned_parts.append(df)
        print(f"   ✅ {p.name}: in={len(pd.read_parquet(p)):,} -> out={len(df):,}")

    print("\nUniendo partes limpias...")
    all_df = pd.concat(cleaned_parts, ignore_index=True)
    all_df = all_df.sort_values(["time_hour", "station_id"])

    # Guardado
    all_df.to_parquet(OUT_GOLD_CLEAN, index=False)
    print(f"\n✅ GOLD limpio guardado: {OUT_GOLD_CLEAN.resolve()}")
    print(f"   filas_in_total={total_in:,}")
    print(f"   filas_out_total={len(all_df):,}")
    print(f"   filas_eliminadas={total_in - len(all_df):,}")
    print(f"   filas_fuera_rango(<{START.date()}): {bad_1970:,}")
    print(f"   min_time_hour={all_df['time_hour'].min()}")
    print(f"   max_time_hour={all_df['time_hour'].max()}")

    # Sample para PBI/Excel
    sample = all_df.sample(min(SAMPLE_N, len(all_df)), random_state=42)
    sample.to_csv(OUT_SAMPLE_CLEAN, index=False, encoding="utf-8")
    print(f"\n✅ Sample limpio: {OUT_SAMPLE_CLEAN.resolve()} | filas={len(sample):,}")

    print("\nEjemplo filas limpias:")
    print(all_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
