# src/gold/build_gold_bi_plus.py
# Crea una vista BI "plus" desde bicing_gold_final_plus.parquet
# añadiendo holiday_any + flags meteo y normalizando holiday_scope_final.

from pathlib import Path
import duckdb


def find_project_root() -> Path:
    p = Path.cwd().resolve()
    for _ in range(8):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        p = p.parent
    return Path.cwd().resolve()


def main():
    ROOT = find_project_root()

    INP = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"
    OUT = ROOT / "data" / "gold" / "bicing_gold_bi_plus.parquet"

    if not INP.exists():
        raise FileNotFoundError(f"No existe el input: {INP}")

    OUT.parent.mkdir(parents=True, exist_ok=True)

    print("ROOT:", ROOT)
    print("INP :", INP)
    print("OUT :", OUT)

    con = duckdb.connect()

    # NOTA viento:
    # Si tu wind_speed_10m está en m/s, usa 8.0 aprox; si está en km/h, usa 20.0.
    WINDY_THRESHOLD = 20.0

    q = f"""
    COPY (
      SELECT
        -- claves
        station_id,
        time_hour,
        date,

        -- medidas
        bikes_available_mean,
        docks_available_mean,
        mechanical_mean,
        ebike_mean,
        obs_count,

        -- calendario
        hour,
        dayofweek,
        month,
        is_weekend,

        -- meteo
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        pressure_msl,

        -- lags/rolling
        lag_1h_bikes,
        lag_24h_bikes,
        roll3h_bikes_mean,

        -- festivos (flags base)
        is_holiday_barcelona,
        is_holiday_catalunya,
        is_holiday_spain,

        -- scope y nombre (tal como vienen en gold_plus)
        holiday_scope,
        holiday_name,

        -- scope_final normalizado (prioridad: BCN > CAT > ES > none)
        CASE
          WHEN COALESCE(is_holiday_barcelona,0)=1 THEN 'barcelona'
          WHEN COALESCE(is_holiday_catalunya,0)=1 THEN 'catalunya'
          WHEN COALESCE(is_holiday_spain,0)=1 THEN 'spain'
          ELSE 'none'
        END AS holiday_scope_final,

        -- holiday_any (0/1)
        CASE
          WHEN COALESCE(is_holiday_barcelona,0)=1
            OR COALESCE(is_holiday_catalunya,0)=1
            OR COALESCE(is_holiday_spain,0)=1
          THEN 1 ELSE 0
        END AS holiday_any,

        -- flags meteo (0/1)
        CASE WHEN COALESCE(precipitation,0) > 0 THEN 1 ELSE 0 END AS is_rain,
        CASE WHEN COALESCE(precipitation,0) >= 1.0 THEN 1 ELSE 0 END AS is_heavy_rain,
        CASE WHEN COALESCE(wind_speed_10m,0) >= {WINDY_THRESHOLD} THEN 1 ELSE 0 END AS is_windy

      FROM read_parquet('{INP.as_posix()}')
      WHERE time_hour >= TIMESTAMP '2019-01-01 00:00:00'
    )
    TO '{OUT.as_posix()}' (FORMAT PARQUET);
    """

    con.execute(q)

    df_check = con.execute(f"""
      SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT station_id) AS stations,
        MIN(time_hour) AS min_time,
        MAX(time_hour) AS max_time,
        SUM(holiday_any) AS holiday_any_rows,
        SUM(is_rain) AS rain_rows,
        SUM(is_windy) AS windy_rows
      FROM read_parquet('{OUT.as_posix()}')
    """).df()

    con.close()

    print("\n✅ OK. BI PLUS creado.")
    print(df_check.to_string(index=False))
    print("\nOUT:", OUT)


if __name__ == "__main__":
    main()
