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

    inp = root / "data" / "gold" / "bicing_gold_final_plus.parquet"
    out_dir = root / "data" / "gold" / "samples"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "bicing_gold_final_plus_sample_1M_strat_holidays.parquet"

    if not inp.exists():
        raise FileNotFoundError(f"No existe el input: {inp}")

    con = duckdb.connect()

    holiday_any_expr = """
      (COALESCE(is_holiday_barcelona,0)=1
       OR COALESCE(is_holiday_catalunya,0)=1
       OR COALESCE(is_holiday_spain,0)=1)
    """

    # Objetivo: 1M final con más festivos que un sample uniforme
    target_total = 1_000_000
    target_h = 300_000
    target_n = target_total - target_h

    # Conteos
    qc = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN {holiday_any_expr} THEN 1 ELSE 0 END) AS n_holiday
    FROM read_parquet('{inp.as_posix()}')
    """
    counts = con.execute(qc).df().iloc[0]
    n_total = int(counts["n_total"])
    n_holiday = int(counts["n_holiday"])
    n_non = n_total - n_holiday

    n_h = min(target_h, n_holiday)
    n_n = min(target_n, n_non)

    # Si faltan filas por cualquier motivo, completamos con no-festivo
    remaining = target_total - (n_h + n_n)
    if remaining > 0:
        n_n = min(n_non, n_n + remaining)

    q = f"""
    COPY (
      WITH
      h AS (
        SELECT *
        FROM read_parquet('{inp.as_posix()}')
        WHERE {holiday_any_expr}
        ORDER BY random()
        LIMIT {n_h}
      ),
      n AS (
        SELECT *
        FROM read_parquet('{inp.as_posix()}')
        WHERE NOT ({holiday_any_expr})
        ORDER BY random()
        LIMIT {n_n}
      )
      SELECT * FROM (
        SELECT * FROM h
        UNION ALL
        SELECT * FROM n
      )
      ORDER BY random()
      LIMIT {target_total}
    ) TO '{out.as_posix()}' (FORMAT PARQUET);
    """

    con.execute(q)

    # Verificación
    qv = f"""
    SELECT
      COUNT(*) AS rows,
      SUM(CASE WHEN {holiday_any_expr} THEN 1 ELSE 0 END) AS rows_holiday_any,
      SUM(COALESCE(is_holiday_barcelona,0)) AS rows_bcn,
      SUM(COALESCE(is_holiday_catalunya,0)) AS rows_cat,
      SUM(COALESCE(is_holiday_spain,0)) AS rows_es
    FROM read_parquet('{out.as_posix()}')
    """
    print(con.execute(qv).df())

    con.close()
    print(f"\n✅ Sample estratificado creado:\n{out}")

if __name__ == "__main__":
    main()
