from pathlib import Path
import duckdb

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(10):
        if (p / "data").exists():
            return p
        p = p.parent
    raise FileNotFoundError("No encuentro /data. Ejecuta desde el repo.")

def main():
    root = find_root()
    inp = root / "data" / "gold" / "bicing_gold_final_plus.parquet"
    out = root / "data" / "gold" / "bicing_gold_final_plus_holidays_ok.parquet"
    if not inp.exists():
        raise FileNotFoundError(f"No existe: {inp}")

    con = duckdb.connect()

    q = f"""
    COPY (
      SELECT
        *,
        CASE
          WHEN COALESCE(is_holiday_barcelona,0)=1
            OR COALESCE(is_holiday_catalunya,0)=1
            OR COALESCE(is_holiday_spain,0)=1
          THEN 1 ELSE 0
        END AS is_holiday_any_fixed
      FROM read_parquet('{inp.as_posix()}')
    ) TO '{out.as_posix()}' (FORMAT PARQUET);
    """
    con.execute(q)

    # Verificación
    qv = f"""
    SELECT
      COUNT(*) AS n_rows,
      SUM(is_holiday_any_fixed) AS holiday_any_fixed,
      SUM(is_holiday_barcelona) AS holiday_bcn,
      SUM(is_holiday_catalunya) AS holiday_cat,
      SUM(is_holiday_spain) AS holiday_es
    FROM read_parquet('{out.as_posix()}')
    """
    print(con.execute(qv).df())

    con.close()
    print(f"\n✅ OUT: {out}")

if __name__ == "__main__":
    main()
