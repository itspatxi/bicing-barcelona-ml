from pathlib import Path
import duckdb

def find_root() -> Path:
    root = Path.cwd().resolve()
    for _ in range(6):
        if (root / "data").exists():
            return root
        root = root.parent
    raise FileNotFoundError("No encuentro /data. Ejecuta desde el repo.")

def main():
    ROOT = find_root()
    INP = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"
    STQ = ROOT / "reports" / "tables" / "station_quality.parquet"

    OUT_BI = ROOT / "data" / "gold" / "bicing_gold_bi.parquet"
    OUT_ML = ROOT / "data" / "gold" / "bicing_gold_ml.parquet"

    if not INP.exists():
        raise FileNotFoundError(f"No existe: {INP}")
    if not STQ.exists():
        raise FileNotFoundError(f"No existe: {STQ} (primero ejecuta 08_station_coverage_quality.py)")

    con = duckdb.connect()

    # BI: full_coverage + new_station (ideal para dashboards)
    con.execute(f"""
        COPY (
            SELECT g.*
            FROM read_parquet('{INP.as_posix()}') g
            JOIN read_parquet('{STQ.as_posix()}') s
            USING (station_id)
            WHERE s.coverage_tag IN ('full_coverage','new_station')
        ) TO '{OUT_BI.as_posix()}' (FORMAT PARQUET);
    """)

    # ML: solo full_coverage y buena cobertura global
    con.execute(f"""
        COPY (
            SELECT g.*
            FROM read_parquet('{INP.as_posix()}') g
            JOIN read_parquet('{STQ.as_posix()}') s
            USING (station_id)
            WHERE s.coverage_tag = 'full_coverage'
              AND s.coverage_ratio_global >= 0.90
              AND s.n_rows >= 20000
        ) TO '{OUT_ML.as_posix()}' (FORMAT PARQUET);
    """)

    df_counts = con.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM read_parquet('{INP.as_posix()}')) AS rows_in,
            (SELECT COUNT(*) FROM read_parquet('{OUT_BI.as_posix()}')) AS rows_bi,
            (SELECT COUNT(*) FROM read_parquet('{OUT_ML.as_posix()}')) AS rows_ml
    """).df()

    print("✅ OK")
    print("IN :", INP)
    print("BI :", OUT_BI)
    print("ML :", OUT_ML)
    print(df_counts)

    con.close()

if __name__ == "__main__":
    main()
