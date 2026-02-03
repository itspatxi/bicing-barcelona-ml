# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import duckdb

def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(6):
        if (cur / "data").exists() or (cur / ".git").exists():
            return cur
        cur = cur.parent
    return start.resolve()

def main() -> None:
    root = find_project_root(Path.cwd())

    gold_in = root / "data" / "gold" / "bicing_gold_final.parquet"
    fest_csv = root / "data" / "silver" / "festivos" / "festivos_bcn_2019_2025.csv"
    gold_out = root / "data" / "gold" / "bicing_gold_final_plus.parquet"

    if not gold_in.exists():
        raise FileNotFoundError(f"No existe: {gold_in}")
    if not fest_csv.exists():
        raise FileNotFoundError(f"No existe: {fest_csv}")

    con = duckdb.connect()

    # Importante: date en gold ya es date; si no, lo casteamos.
    # Creamos flags:
    # - is_holiday (ya existe pero lo recalculamos consistente)
    # - is_holiday_barcelona (scope contiene 'barcelona')
    # - is_holiday_catalunya (scope contiene 'catalunya')
    # - is_holiday_spain (scope contiene 'spain')
    # - holiday_scope y holiday_name (útiles para Power BI / trazabilidad)
    query = f"""
    COPY (
      SELECT
        g.*,
        COALESCE(f.is_holiday, 0) AS is_holiday_new,
        CASE WHEN f.scope LIKE '%barcelona%' THEN 1 ELSE 0 END AS is_holiday_barcelona,
        CASE WHEN f.scope LIKE '%catalunya%' THEN 1 ELSE 0 END AS is_holiday_catalunya,
        CASE WHEN f.scope LIKE '%spain%' THEN 1 ELSE 0 END AS is_holiday_spain,
        f.scope AS holiday_scope,
        f.name  AS holiday_name
      FROM read_parquet('{gold_in.as_posix()}') g
      LEFT JOIN read_csv_auto('{fest_csv.as_posix()}') f
        ON CAST(g.date AS DATE) = CAST(f.date AS DATE)
    ) TO '{gold_out.as_posix()}' (FORMAT PARQUET);
    """
    print("🧠 Añadiendo festivos a GOLD...")
    con.execute(query)
    con.close()

    print("✅ OK")
    print("   IN :", gold_in)
    print("   OUT:", gold_out)

    # Mini chequeo rápido con DuckDB
    con = duckdb.connect()
    chk = con.execute(f"""
      SELECT
        COUNT(*) AS rows,
        SUM(is_holiday_new) AS rows_holiday,
        SUM(is_holiday_barcelona) AS rows_holiday_bcn
      FROM read_parquet('{gold_out.as_posix()}')
    """).fetchdf()
    con.close()
    print(chk.to_string(index=False))

if __name__ == "__main__":
    main()
