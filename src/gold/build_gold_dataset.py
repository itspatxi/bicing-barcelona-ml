"""
build_gold_dataset.py
=====================

OBJETIVO (modo dummies):
- Ya tienes:
  1) Bicing SILVER por hora y estación: data/silver/bicing_hourly/*.parquet
  2) Meteo Open-Meteo por hora (Barcelona): data/raw/meteo_openmeteo/barcelona_hourly_all.parquet
  3) Festivos (bronze): data/bronze/festivos/festivos.parquet

- Ahora creamos el dataset GOLD para:
  - entrenamiento ML (LSTM / modelos clásicos)
  - visualización Power BI
  - features listos para modelar

SALIDA:
- data/gold/bicing_gold.parquet
- data/gold/bicing_gold_sample.csv (una muestra pequeña para inspeccionar en Excel/PBI)

QUÉ UNE:
- Bicing (station_id, time_hour)  LEFT JOIN  Meteo (time_hour)
- + festivo por fecha

FEATURES:
- hour (0-23)
- dayofweek (0=lunes..6=domingo)
- month
- is_weekend
- is_holiday (festivo)
- lag_1h (disponibilidad H-1 por estación)
- lag_24h (disponibilidad H-24 por estación)
- rolling_3h_mean (media móvil 3h por estación)
- target opcional: bikes_available_mean (puedes usarlo como y)

NOTAS:
- Procesa por PARTES (por fichero mensual parquet) para no reventar RAM.
- El merge con meteo es rápido porque meteo son 61k filas.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd


# =========================
# Paths
# =========================

SILVER_DIR = Path("data/silver/bicing_hourly")
METEO_FILE = Path("data/raw/meteo_openmeteo/barcelona_hourly_all.parquet")
FESTIVOS_FILE = Path("data/bronze/festivos/festivos.parquet")

OUT_DIR = Path("data/gold")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_GOLD = OUT_DIR / "bicing_gold.parquet"
OUT_SAMPLE = OUT_DIR / "bicing_gold_sample.csv"

# Guardaremos también en particiones (opcional pero útil)
OUT_PARTS_DIR = OUT_DIR / "parts"
OUT_PARTS_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Helpers
# =========================

def load_meteo() -> pd.DataFrame:
    """
    Carga meteo horario y deja columnas limpias:
    time_hour, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m, pressure_msl
    """
    if not METEO_FILE.exists():
        raise RuntimeError(f"No encuentro meteo: {METEO_FILE.resolve()}")

    meteo = pd.read_parquet(METEO_FILE)

    # Nos aseguramos de que time_hour existe y es datetime
    if "time_hour" not in meteo.columns:
        raise RuntimeError("Meteo no tiene columna time_hour (algo raro).")

    meteo["time_hour"] = pd.to_datetime(meteo["time_hour"], errors="coerce")
    meteo = meteo.dropna(subset=["time_hour"]).drop_duplicates(subset=["time_hour"]).sort_values("time_hour")

    keep = ["time_hour", "temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "pressure_msl"]
    meteo = meteo[keep]

    return meteo


def load_festivos() -> pd.DataFrame:
    """
    Carga festivos y deja una tabla por fecha con is_holiday=1.
    OJO: el parquet tiene 1 fila por idioma; filtramos español (es) para evitar duplicados.
    """
    if not FESTIVOS_FILE.exists():
        raise RuntimeError(f"No encuentro festivos: {FESTIVOS_FILE.resolve()}")

    f = pd.read_parquet(FESTIVOS_FILE)

    # Nos quedamos con 'es' para no duplicar fechas
    if "lang" in f.columns:
        f = f[f["lang"] == "es"].copy()

    f["date"] = pd.to_datetime(f["date"], errors="coerce").dt.date
    f = f.dropna(subset=["date"]).drop_duplicates(subset=["date"])

    f["is_holiday"] = 1
    return f[["date", "is_holiday"]]


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea columnas temporales básicas para modelos y Power BI.
    """
    dt = pd.to_datetime(df["time_hour"], errors="coerce")
    df["hour"] = dt.dt.hour
    df["dayofweek"] = dt.dt.dayofweek  # 0=lunes
    df["month"] = dt.dt.month
    df["date"] = dt.dt.date
    df["is_weekend"] = (df["dayofweek"] >= 5).astype("int8")
    return df


def add_lags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade lags por estación.
    IMPORTANTE:
    - Debe estar ordenado por station_id y time_hour.
    """
    df = df.sort_values(["station_id", "time_hour"])

    # target base (bikes_available_mean)
    # lags clásicos:
    df["lag_1h_bikes"] = df.groupby("station_id")["bikes_available_mean"].shift(1)
    df["lag_24h_bikes"] = df.groupby("station_id")["bikes_available_mean"].shift(24)

    # rolling 3h (media móvil)
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
    # 1) Cargamos tablas pequeñas (meteo y festivos) una vez
    print("A) Cargando meteo...")
    meteo = load_meteo()
    print(f"   ✅ meteo filas={len(meteo):,} | rango={meteo['time_hour'].min()} -> {meteo['time_hour'].max()}")

    print("B) Cargando festivos...")
    festivos = load_festivos()
    print(f"   ✅ festivos filas={len(festivos):,}")

    # 2) Listamos parquets silver mensuales
    parts = sorted(SILVER_DIR.glob("bicing_hourly_*.parquet"))
    if not parts:
        raise RuntimeError(f"No encuentro parquets Silver en: {SILVER_DIR.resolve()}")

    gold_parts_paths = []

    print(f"C) Procesando {len(parts)} partes Silver -> Gold...")
    for p in parts:
        print(f"\n   Procesando parte: {p.name}")
        df = pd.read_parquet(p)

        # Tipos y limpieza mínima
        df["time_hour"] = pd.to_datetime(df["time_hour"], errors="coerce")
        df = df.dropna(subset=["station_id", "time_hour"]).copy()

        # Features temporales
        df = add_time_features(df)

        # Join festivos por fecha
        df = df.merge(festivos, on="date", how="left")
        df["is_holiday"] = df["is_holiday"].fillna(0).astype("int8")

        # Join meteo por hora
        df = df.merge(meteo, on="time_hour", how="left")

        # Lags por estación
        df = add_lags(df)

        # Guardamos esta parte ya en Gold/parts
        out_part = OUT_PARTS_DIR / p.name.replace("bicing_hourly_", "gold_")
        df.to_parquet(out_part, index=False)
        gold_parts_paths.append(out_part)

        print(f"   ✅ guardado: {out_part.name} | filas={len(df):,}")

    # 3) Unimos todas las partes en un solo parquet final
    # OJO: esto sí puede ocupar bastante RAM, pero normalmente con tus tamaños debería ir bien.
    # Si te preocupa, me lo dices y lo dejamos solo por partes.
    print("\nD) Uniendo partes a un único parquet GOLD...")
    all_df = pd.concat([pd.read_parquet(x) for x in gold_parts_paths], ignore_index=True)
    all_df = all_df.sort_values(["time_hour", "station_id"])

    all_df.to_parquet(OUT_GOLD, index=False)
    print(f"✅ GOLD final -> {OUT_GOLD.resolve()} | filas={len(all_df):,}")

    # 4) Muestra para inspección rápida (Excel / Power BI)
    sample = all_df.sample(min(200_000, len(all_df)), random_state=42)
    sample.to_csv(OUT_SAMPLE, index=False, encoding="utf-8")
    print(f"✅ Sample CSV -> {OUT_SAMPLE.resolve()} | filas={len(sample):,}")

    # Print rápido
    print("\nEjemplo filas:")
    print(all_df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
