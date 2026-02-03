from pathlib import Path
import duckdb

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(12):
        if (p / "data").exists():
            return p
        p = p.parent
    raise FileNotFoundError("No encuentro /data. Ejecuta desde el repo.")

def main():
    root = find_root()

    # Usa el parquet con columnas de festivos (el tuyo)
    inp = root / "data" / "gold" / "bicing_gold_final_plus.parquet"
    out_dir = root / "data" / "gold" / "samples"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "bicing_gold_final_plus_sample_1M_strat_holidays.parquet"

    if not inp.exists():
        raise FileNotFoundError(f"No existe el input: {inp}")

    con = duckdb.connect()

    # Definimos "holiday_any" sobre la marcha (sin depender de is_holiday)
    holiday_any_expr = """
      (COALESCE(is_holiday_barcelona,0)=1
       OR COALESCE(is_holiday_catalunya,0)=1
       OR COALESCE(is_holiday_spain,0)=1)
    """

    # Ajusta proporciones: 300k festivos + 700k no festivos (ejemplo)
    n_h = 300_000
    n_n = 700_000

    q = f"""
    COPY (
      SELECT * FROM (
        SELECT * FROM read_parquet('{inp.as_posix()}')
        WHERE {holiday_any_expr}
        USING SAMPLE {n_h} ROWS

        UNION ALL

        SELECT * FROM read_parquet('{inp.as_posix()}')
        WHERE NOT ({holiday_any_expr})
        USING SAMPLE {n_n} ROWS
      )
      ORDER BY random()
      LIMIT 1000000
    ) TO '{out.as_posix()}' (FORMAT PARQUET);
    """

    con.execute(q)

    # Chequeos rápidos
    qc = f"""
    SELECT
      COUNT(*) AS rows,
      SUM(CASE WHEN {holiday_any_expr} THEN 1 ELSE 0 END) AS rows_holiday_any,
      SUM(COALESCE(is_holiday_barcelona,0)) AS rows_bcn,
      SUM(COALESCE(is_holiday_catalunya,0)) AS rows_cat,
      SUM(COALESCE(is_holiday_spain,0)) AS rows_es
    FROM read_parquet('{out.as_posix()}')
    """
    print(con.execute(qc).df())

    con.close()
    print(f"\n✅ Sample estratificado creado:\n{out}")

if __name__ == "__main__":
    main()
