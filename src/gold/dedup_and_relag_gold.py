"""
dedup_and_relag_gold.py
=======================

PROBLEMA:
- En bicing_gold_clean.parquet hay duplicados de (station_id, time_hour).
- Eso rompe features de serie temporal (lags, rolling) y cualquier modelo.

SOLUCIÓN:
1) Cargamos data/gold/bicing_gold_clean.parquet
2) Detectamos duplicados de (station_id, time_hour)
3) Colapsamos duplicados en 1 fila por clave:
   - Variables bicing: media ponderada por obs_count (si existe)
   - Meteo/festivos: tomamos el primero no nulo
4) Recalculamos lags y rolling ya con dataset limpio
5) Guardamos:
   - data/gold/bicing_gold_final.parquet
   - data/gold/bicing_gold_final_sample.csv
   - (opcional) reporte: data/gold/_dedup_report.csv

NOTA:
- Esto puede tardar porque son ~27M filas, pero es un paso único y definitivo.
- Si te queda corto de RAM, lo hacemos por particiones (te lo adapto).
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


IN_FILE = Path("data/gold/bicing_gold_clean.parquet")
OUT_DIR = Path("data/gold")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FINAL = OUT_DIR / "bicing_gold_final.parquet"
OUT_SAMPLE = OUT_DIR / "bicing_gold_final_sample.csv"
OUT_REPORT = OUT_DIR / "_dedup_report.csv"

SAMPLE_N = 200_000


# =========================
# Helpers
# =========================

def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    """Media ponderada robusta (ignora NaNs en values y weights)."""
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")

    mask = (~v.isna()) & (~w.isna()) & (w > 0)
    if not mask.any():
        # si no hay pesos válidos, media simple
        return float(np.nanmean(v.to_numpy(dtype="float64")))
    return float(np.average(v[mask], weights=w[mask]))


def first_not_null(s: pd.Series):
    """Devuelve el primer valor no nulo (o NaN si todo es nulo)."""
    s2 = s.dropna()
    return s2.iloc[0] if len(s2) else np.nan


def add_lags(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula lags y rolling sobre dataset ya deduplicado."""
    df = df.sort_values(["station_id", "time_hour"])

    df["lag_1h_bikes"] = df.groupby("station_id")["bikes_available_mean"].shift(1)
    df["lag_24h_bikes"] = df.groupby("station_id")["bikes_available_mean"].shift(24)

    df["roll3h_bikes_mean"] = (
        df.groupby("station_id")["bikes_available_mean"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )
    return df


# =========================
# Main
# =========================

def main():
    if not IN_FILE.exists():
        raise RuntimeError(f"No encuentro: {IN_FILE.resolve()}")

    print("A) Cargando GOLD limpio...")
    df = pd.read_parquet(IN_FILE)

    # Tipos básicos
    df["time_hour"] = pd.to_datetime(df["time_hour"], errors="coerce")
    df = df.dropna(subset=["station_id", "time_hour"]).copy()

    # Info duplicados
    print("B) Midiendo duplicados (station_id, time_hour)...")
    dup_mask = df.duplicated(subset=["station_id", "time_hour"], keep=False)
    n_dups_rows = int(dup_mask.sum())
    n_rows = len(df)

    # número de claves duplicadas (no filas)
    dup_keys = df.loc[dup_mask, ["station_id", "time_hour"]].drop_duplicates()
    n_dup_keys = len(dup_keys)

    print(f"   filas_totales={n_rows:,}")
    print(f"   filas_en_grupos_duplicados={n_dups_rows:,}")
    print(f"   claves_duplicadas={n_dup_keys:,}")

    # Guardamos report rápido
    report = pd.DataFrame([{
        "rows_total": n_rows,
        "rows_in_dup_groups": n_dups_rows,
        "dup_keys": n_dup_keys,
        "pct_rows_in_dup_groups": (n_dups_rows / n_rows * 100.0) if n_rows else 0.0,
    }])
    report.to_csv(OUT_REPORT, index=False, encoding="utf-8")
    print(f"   🧾 reporte: {OUT_REPORT.resolve()}")

    if n_dups_rows == 0:
        print("✅ No hay duplicados. Recalculando lags y guardando final...")
        df = add_lags(df)
        df.to_parquet(OUT_FINAL, index=False)
        sample = df.sample(min(SAMPLE_N, len(df)), random_state=42)
        sample.to_csv(OUT_SAMPLE, index=False, encoding="utf-8")
        print(f"✅ FINAL -> {OUT_FINAL.resolve()} | filas={len(df):,}")
        print(f"✅ SAMPLE -> {OUT_SAMPLE.resolve()} | filas={len(sample):,}")
        return

    print("C) Colapsando duplicados (1 fila por station_id+time_hour)...")

    # Columnas típicas de Bicing que queremos ponderar
    bicing_cols_weighted = [
        "bikes_available_mean",
        "docks_available_mean",
        "mechanical_mean",
        "ebike_mean",
    ]

    # Peso: obs_count (si no está, creamos 1)
    if "obs_count" not in df.columns:
        df["obs_count"] = 1

    # Columnas meteo / flags: nos quedamos con el primero no nulo
    passthrough_cols = [
        "hour", "dayofweek", "month", "date", "is_weekend", "is_holiday",
        "temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "pressure_msl",
    ]

    group_cols = ["station_id", "time_hour"]

    # Agregación:
    # - weighted means para bicing_cols_weighted
    # - sum obs_count
    # - first_not_null para passthrough
    agg_dict = {}

    for c in bicing_cols_weighted:
        if c in df.columns:
            agg_dict[c] = lambda s, c=c: weighted_mean(s, df.loc[s.index, "obs_count"])

    agg_dict["obs_count"] = "sum"

    for c in passthrough_cols:
        if c in df.columns:
            agg_dict[c] = first_not_null

    # Ejecutamos groupby
    dedup = df.groupby(group_cols, as_index=False).agg(agg_dict)

    print(f"   ✅ dedup filas={len(dedup):,} (antes {len(df):,})")

    # Recalcular lags/rolling ahora que todo es 1 fila por hora
    print("D) Recalculando lags/rolling sobre dataset deduplicado...")
    dedup = add_lags(dedup)

    # Guardar final
    dedup.to_parquet(OUT_FINAL, index=False)
    print(f"✅ FINAL -> {OUT_FINAL.resolve()} | filas={len(dedup):,}")

    # Sample para PBI/Excel
    sample = dedup.sample(min(SAMPLE_N, len(dedup)), random_state=42)
    sample.to_csv(OUT_SAMPLE, index=False, encoding="utf-8")
    print(f"✅ SAMPLE -> {OUT_SAMPLE.resolve()} | filas={len(sample):,}")

    # Ejemplo
    print("\nEjemplo filas final:")
    print(dedup.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
