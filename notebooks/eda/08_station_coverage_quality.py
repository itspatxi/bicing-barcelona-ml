from pathlib import Path
import duckdb
import pandas as pd

def find_root() -> Path:
    root = Path.cwd().resolve()
    for _ in range(6):
        if (root / "data").exists():
            return root
        root = root.parent
    raise FileNotFoundError("No encuentro la carpeta /data subiendo desde el CWD. Ejecuta desde el repo o ajusta root.")

def main():
    ROOT = find_root()
    PARQ = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"
    OUT_DIR = ROOT / "reports" / "tables"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not PARQ.exists():
        raise FileNotFoundError(f"No existe: {PARQ}")

    con = duckdb.connect()

    # 1) Rango global y horas esperadas
    df_global = con.execute(f"""
        SELECT
            MIN(time_hour) AS min_time,
            MAX(time_hour) AS max_time
        FROM read_parquet('{PARQ.as_posix()}')
    """).df()

    min_time = pd.to_datetime(df_global.loc[0, "min_time"])
    max_time = pd.to_datetime(df_global.loc[0, "max_time"])
    expected_hours_global = int(((max_time - min_time).total_seconds() // 3600) + 1)

    # 2) Cobertura por estación (y ratio global)
    df_cov = con.execute(f"""
        SELECT
            station_id,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT time_hour) AS n_distinct_hours,
            MIN(time_hour) AS min_time,
            MAX(time_hour) AS max_time,
            EXTRACT(YEAR FROM MIN(time_hour))::INT AS first_year,
            EXTRACT(YEAR FROM MAX(time_hour))::INT AS last_year
        FROM read_parquet('{PARQ.as_posix()}')
        GROUP BY 1
        ORDER BY n_rows DESC
    """).df()

    # coverage_ratio "local": continuidad dentro de su ventana activa
    # expected_hours_local = horas entre min_time y max_time de esa estación
    df_cov["min_time"] = pd.to_datetime(df_cov["min_time"])
    df_cov["max_time"] = pd.to_datetime(df_cov["max_time"])
    df_cov["expected_hours_local"] = (((df_cov["max_time"] - df_cov["min_time"]).dt.total_seconds() // 3600) + 1).astype(int)
    df_cov["coverage_ratio"] = (df_cov["n_distinct_hours"] / df_cov["expected_hours_local"]).astype(float)

    # coverage_ratio_global: sobre todo el rango global
    df_cov["expected_hours_global"] = expected_hours_global
    df_cov["coverage_ratio_global"] = (df_cov["n_distinct_hours"] / expected_hours_global).astype(float)

    # 3) Etiquetas (tú ya tenías algo parecido; aquí lo dejamos bien definido)
    def tag_row(r):
        if r["n_rows"] < 1000:
            return "noise_very_sparse"
        # estaciones nuevas: empiezan 2024/2025 (ajustable)
        if r["first_year"] >= 2024:
            return "new_station"
        # estaciones con huecos (malas para series)
        if r["coverage_ratio_global"] < 0.70:
            return "sparse_or_gappy"
        # full coverage: buena cobertura global y local
        if r["coverage_ratio_global"] >= 0.90 and r["coverage_ratio"] >= 0.90:
            return "full_coverage"
        return "sparse_or_gappy"

    df_cov["coverage_tag"] = df_cov.apply(tag_row, axis=1)

    out_csv = OUT_DIR / "station_quality.csv"
    out_parq = OUT_DIR / "station_quality.parquet"
    df_cov.to_csv(out_csv, index=False)
    df_cov.to_parquet(out_parq, index=False)

    print("ROOT:", ROOT)
    print("PARQ:", PARQ)
    print("Global range:", min_time, "->", max_time)
    print("expected_hours_global:", expected_hours_global)
    print("\n== Resumen por coverage_tag ==")
    print(df_cov["coverage_tag"].value_counts())
    print("\n✅ Guardado:")
    print("-", out_csv)
    print("-", out_parq)

    con.close()

if __name__ == "__main__":
    main()
