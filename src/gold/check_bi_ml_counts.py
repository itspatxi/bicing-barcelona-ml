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
    bi = ROOT / "data" / "gold" / "bicing_gold_bi.parquet"
    ml = ROOT / "data" / "gold" / "bicing_gold_ml.parquet"

    con = duckdb.connect()

    print("\n== BI ==")
    print(con.execute(f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(DISTINCT station_id) AS stations,
          MIN(time_hour) AS min_time,
          MAX(time_hour) AS max_time,
          SUM(CASE WHEN is_holiday_new=1 THEN 1 ELSE 0 END) AS holiday_any_rows
        FROM read_parquet('{bi.as_posix()}')
    """).df())

    print("\n== ML ==")
    print(con.execute(f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(DISTINCT station_id) AS stations,
          MIN(time_hour) AS min_time,
          MAX(time_hour) AS max_time,
          SUM(CASE WHEN is_holiday_new=1 THEN 1 ELSE 0 END) AS holiday_any_rows
        FROM read_parquet('{ml.as_posix()}')
    """).df())

    print("\n== Distribución filas por estación (ML) ==")
    print(con.execute(f"""
        SELECT
          MIN(n_rows) AS min_rows_station,
          APPROX_QUANTILE(n_rows, 0.25) AS p25,
          APPROX_QUANTILE(n_rows, 0.50) AS p50,
          APPROX_QUANTILE(n_rows, 0.75) AS p75,
          MAX(n_rows) AS max_rows_station
        FROM (
          SELECT station_id, COUNT(*) n_rows
          FROM read_parquet('{ml.as_posix()}')
          GROUP BY 1
        )
    """).df())

    con.close()

if __name__ == "__main__":
    main()
