from pathlib import Path
import duckdb

def find_root() -> Path:
    root = Path.cwd().resolve()
    for _ in range(8):
        if (root / "data").exists():
            return root
        root = root.parent
    raise FileNotFoundError("No encuentro carpeta /data hacia arriba. Ejecuta desde dentro del repo.")

def main():
    ROOT = find_root()

    INP = ROOT / "data" / "gold" / "bicing_gold_ml.parquet"
    OUT = ROOT / "data" / "gold" / "bicing_gold_ml_features_tplus1.parquet"

    assert INP.exists(), f"No existe input: {INP}"

    con = duckdb.connect()

    # Generamos features + target t+1h por estación
    q = f"""
    COPY (
      WITH base AS (
        SELECT
          station_id,
          time_hour,
          bikes_available_mean,
          docks_available_mean,
          mechanical_mean,
          ebike_mean,
          obs_count,

          hour,
          dayofweek,
          month,
          date,
          is_weekend,

          temperature_2m,
          relative_humidity_2m,
          precipitation,
          wind_speed_10m,
          pressure_msl,

          lag_1h_bikes,
          lag_24h_bikes,
          roll3h_bikes_mean,

          -- flags de festivos (ya vienen en final_plus, pero en ML view depende de tu build)
          COALESCE(is_holiday_barcelona, 0) AS is_holiday_barcelona,
          COALESCE(is_holiday_catalunya, 0) AS is_holiday_catalunya,
          COALESCE(is_holiday_spain, 0) AS is_holiday_spain,

          CASE
            WHEN COALESCE(is_holiday_barcelona,0)=1 THEN 'barcelona'
            WHEN COALESCE(is_holiday_catalunya,0)=1 THEN 'catalunya'
            WHEN COALESCE(is_holiday_spain,0)=1 THEN 'spain'
            ELSE 'none'
          END AS holiday_scope_final,

          CASE WHEN (COALESCE(is_holiday_barcelona,0)=1 OR COALESCE(is_holiday_catalunya,0)=1 OR COALESCE(is_holiday_spain,0)=1)
               THEN 1 ELSE 0 END AS holiday_any,

          -- seno/coseno hora y día semana (evita que 23 y 0 estén "lejos")
          SIN(2*PI()*hour/24.0) AS sin_hour,
          COS(2*PI()*hour/24.0) AS cos_hour,
          SIN(2*PI()*dayofweek/7.0) AS sin_dow,
          COS(2*PI()*dayofweek/7.0) AS cos_dow,

          -- flags meteo sencillos (ajusta umbrales si quieres)
          CASE WHEN precipitation >= 0.1 THEN 1 ELSE 0 END AS is_rain,
          CASE WHEN precipitation >= 2.0 THEN 1 ELSE 0 END AS is_heavy_rain,
          CASE WHEN wind_speed_10m >= 25.0 THEN 1 ELSE 0 END AS is_windy,

          -- lags extra (se calculan aquí, no en tu gold original)
          LAG(bikes_available_mean, 2) OVER (PARTITION BY station_id ORDER BY time_hour) AS lag_2h_bikes,

          -- TARGET: bikes en t+1h
          LEAD(bikes_available_mean, 1) OVER (PARTITION BY station_id ORDER BY time_hour) AS y_bikes_tplus1
        FROM read_parquet('{INP.as_posix()}')
      )
      SELECT *
      FROM base
      WHERE y_bikes_tplus1 IS NOT NULL
    )
    TO '{OUT.as_posix()}'
    (FORMAT PARQUET);
    """

    print("IN :", INP)
    print("OUT:", OUT)
    con.execute(q)

    # checks básicos
    chk = con.execute(f"""
      SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT station_id) AS stations,
        MIN(time_hour) AS min_time,
        MAX(time_hour) AS max_time,
        SUM(CASE WHEN y_bikes_tplus1 IS NULL THEN 1 ELSE 0 END) AS null_y
      FROM read_parquet('{OUT.as_posix()}')
    """).df()
    print("\n== CHECK ==")
    print(chk)

    # Duplicados por clave (debería ser 0)
    dup = con.execute(f"""
      SELECT
        COUNT(*) - COUNT(DISTINCT (CAST(station_id AS VARCHAR) || '|' || CAST(time_hour AS VARCHAR))) AS dup_keys
      FROM read_parquet('{OUT.as_posix()}')
    """).fetchone()[0]
    print("dup_keys =", dup)

    con.close()
    print("\n✅ OK: features + target t+1 generados")

if __name__ == "__main__":
    main()
