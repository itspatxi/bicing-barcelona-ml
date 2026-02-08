from pathlib import Path
import duckdb

def find_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(7):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()

def main():
    ROOT = find_root()

    pred = ROOT / "data" / "gold" / "bi" / "ml_pred_vs_real_last90d.parquet"
    gold = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"
    out  = ROOT / "data" / "gold" / "bi" / "ml_pred_vs_real_last90d_plus.parquet"

    if not pred.exists():
        raise FileNotFoundError(f"No existe: {pred}")
    if not gold.exists():
        raise FileNotFoundError(f"No existe: {gold}")

    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # OJO: aquí definimos holiday_any y flags lluvia/viento en SQL
    q = f"""
    COPY (
      SELECT
        p.*,
        g.date,
        g.is_weekend,
        g.is_holiday_barcelona,
        g.is_holiday_catalunya,
        g.is_holiday_spain,
        CASE
          WHEN COALESCE(g.is_holiday_barcelona,0)=1
            OR COALESCE(g.is_holiday_catalunya,0)=1
            OR COALESCE(g.is_holiday_spain,0)=1
          THEN 1 ELSE 0
        END AS holiday_any,
        g.temperature_2m,
        g.relative_humidity_2m,
        g.precipitation,
        g.wind_speed_10m,
        CASE WHEN COALESCE(g.precipitation,0) > 0 THEN 1 ELSE 0 END AS is_rain,
        CASE WHEN COALESCE(g.precipitation,0) >= 1 THEN 1 ELSE 0 END AS is_heavy_rain,
        CASE WHEN COALESCE(g.wind_speed_10m,0) >= 20 THEN 1 ELSE 0 END AS is_windy
      FROM read_parquet('{pred.as_posix()}') p
      LEFT JOIN read_parquet('{gold.as_posix()}') g
        ON p.station_id = g.station_id AND p.time_hour = g.time_hour
    ) TO '{out.as_posix()}' (FORMAT PARQUET);
    """
    con.execute(q)
    con.close()

    print("✅ OK")
    print("OUT:", out)

if __name__ == "__main__":
    main()
