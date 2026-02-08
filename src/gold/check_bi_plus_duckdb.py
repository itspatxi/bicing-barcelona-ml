# src/gold/check_bi_plus_duckdb.py
from pathlib import Path
import duckdb

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()

def main():
    root = find_root()
    p = root / "data" / "gold" / "bicing_gold_bi_plus.parquet"
    print("ROOT:", root)
    print("PARQ:", p)
    print("EXISTS:", p.exists())
    if not p.exists():
        raise FileNotFoundError(p)

    con = duckdb.connect()
    q = f"""
    SELECT
      COUNT(*) AS n_rows,
      MAX(precipitation) AS max_precip,
      SUM(is_rain) AS rain_rows,
      SUM(holiday_any) AS holiday_rows,
      SUM(is_holiday_spain) AS es_rows
    FROM read_parquet('{p.as_posix()}')
    """
    print(con.execute(q).df())
    con.close()

if __name__ == "__main__":
    main()
