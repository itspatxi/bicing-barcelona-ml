from pathlib import Path
import duckdb
import pandas as pd

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(12):
        if (p / "data").exists():
            return p
        p = p.parent
    raise FileNotFoundError("No encuentro /data. Ejecuta desde el repo.")

def main():
    root = find_root()
    p = root / "data" / "gold" / "bicing_gold_final_plus.parquet"
    if not p.exists():
        raise FileNotFoundError(f"No existe: {p}")

    con = duckdb.connect()

    holiday_any = """
      (COALESCE(is_holiday_barcelona,0)=1
       OR COALESCE(is_holiday_catalunya,0)=1
       OR COALESCE(is_holiday_spain,0)=1)
    """

    print("\n== Conteos globales (derivado) ==")
    q1 = f"""
    SELECT
      COUNT(*) AS n_rows,
      SUM(CASE WHEN {holiday_any} THEN 1 ELSE 0 END) AS holiday_any,
      SUM(COALESCE(is_holiday_barcelona,0)) AS holiday_bcn,
      SUM(COALESCE(is_holiday_catalunya,0)) AS holiday_cat,
      SUM(COALESCE(is_holiday_spain,0)) AS holiday_es
    FROM read_parquet('{p.as_posix()}')
    """
    print(con.execute(q1).df())

    print("\n== Por holiday_scope (top 30) ==")
    q2 = f"""
    SELECT
      holiday_scope,
      COUNT(*) AS n
    FROM read_parquet('{p.as_posix()}')
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 30
    """
    print(con.execute(q2).df())

    print("\n== Muestra de festivos (primeros 30) ==")
    q3 = f"""
    SELECT
      date,
      holiday_scope,
      holiday_name,
      COUNT(*) AS n_rows
    FROM read_parquet('{p.as_posix()}')
    WHERE {holiday_any}
    GROUP BY 1,2,3
    ORDER BY 1
    LIMIT 30
    """
    print(con.execute(q3).df())

    con.close()
    print("\n✅ OK")

if __name__ == "__main__":
    main()
