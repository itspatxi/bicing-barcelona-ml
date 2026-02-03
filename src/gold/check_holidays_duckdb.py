from pathlib import Path
import duckdb

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "data").exists():
            return p
        p = p.parent
    raise FileNotFoundError("No encuentro la carpeta /data. Ejecuta desde dentro del repo.")

def main():
    root = find_root()
    p = root / "data" / "gold" / "bicing_gold_final_plus.parquet"
    if not p.exists():
        raise FileNotFoundError(f"No existe: {p}")

    con = duckdb.connect()

    # 1) Conteos globales
    q1 = f"""
    SELECT
      COUNT(*) AS n_rows,
      SUM(is_holiday) AS holiday_any,
      SUM(is_holiday_barcelona) AS holiday_bcn,
      SUM(is_holiday_catalunya) AS holiday_cat,
      SUM(is_holiday_spain) AS holiday_es
    FROM read_parquet('{p.as_posix()}')
    """
    print("\n== Conteos globales ==")
    print(con.execute(q1).df())

    # 2) Conteo por scope (incluye NULL)
    q2 = f"""
    SELECT
      COALESCE(holiday_scope, 'NULL') AS holiday_scope,
      COUNT(*) AS n
    FROM read_parquet('{p.as_posix()}')
    GROUP BY 1
    ORDER BY 2 DESC
    """
    print("\n== Por holiday_scope (top 30) ==")
    df2 = con.execute(q2).df()
    print(df2.head(30))

    # 3) Muestra de días festivos (para validar name/scope)
    q3 = f"""
    SELECT
      date,
      holiday_scope,
      holiday_name,
      COUNT(*) AS n_rows
    FROM read_parquet('{p.as_posix()}')
    WHERE is_holiday = 1
    GROUP BY 1,2,3
    ORDER BY date
    LIMIT 30
    """
    print("\n== Muestra de festivos (primeros 30) ==")
    print(con.execute(q3).df())

    con.close()
    print("\n✅ OK")

if __name__ == "__main__":
    main()
