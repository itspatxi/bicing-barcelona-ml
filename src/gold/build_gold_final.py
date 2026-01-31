"""
build_gold_final.py
-------------------
Objetivo:
  1) Partimos del GOLD "sucio" o del dedup_global (da igual, lo limpiamos aquí).
  2) Quitamos filas fuera de rango (ej: 1970).
  3) Hacemos deduplicado GLOBAL por (station_id, time_hour).
  4) Guardamos un parquet final listo para:
       - EDA
       - Power BI
       - ML

Por qué DuckDB:
  - Maneja muy bien Parquet grande sin cargar todo en RAM.
  - Deduplicar 27M filas con pandas puede tardar muchísimo y petar memoria.

Uso:
  Desde la raíz del repo:
    python .\src\gold\build_gold_final.py

Salida:
  data/gold/bicing_gold_final.parquet
"""

from __future__ import annotations

from pathlib import Path
import duckdb


# ====== CONFIG (fácil para dummies) ======
ROOT = Path(__file__).resolve().parents[2]  # .../bicing-barcelona-ml
DATA_GOLD = ROOT / "data" / "gold"

# Inputs posibles (elige el que exista en tu caso)
IN_CANDIDATES = [
    DATA_GOLD / "bicing_gold_dedup_global.parquet",  # el que tienes ya (dedup, pero con 1970)
    DATA_GOLD / "bicing_gold_clean.parquet",         # el que tienes sin 1970 (pero puede tener duplicados)
    DATA_GOLD / "bicing_gold.parquet",               # el original (tiene 1970 y duplicados)
]

OUT_FILE = DATA_GOLD / "bicing_gold_final.parquet"

# Rango válido (lo que tú estás usando)
MIN_TS = "2019-01-01 00:00:00"
MAX_TS = "2025-12-31 23:00:00"

# Clave de deduplicado
KEYS = ["station_id", "time_hour"]


def pick_input_file() -> Path:
    """Devuelve el primer input que exista (en orden)."""
    for p in IN_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError(
        "No encuentro ningún input parquet. He mirado:\n- "
        + "\n- ".join(str(x) for x in IN_CANDIDATES)
    )


def main() -> None:
    inp = pick_input_file()
    print(f"📁 ROOT: {ROOT}")
    print(f"📥 IN : {inp}")
    print(f"📤 OUT: {OUT_FILE}")

    # Conexión DuckDB (en memoria)
    con = duckdb.connect(database=":memory:")

    # Truco útil: que DuckDB use varios hilos si puede
    con.execute("PRAGMA threads=8;")

    # 1) Contar filas de entrada
    in_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{inp.as_posix()}')"
    ).fetchone()[0]
    print(f"🔢 Filas input: {in_rows:,}")

    # 2) Creamos una vista con solo el rango correcto (aquí desaparece 1970)
    #    OJO: usamos BETWEEN inclusivo o >= y <=.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW gold_ranged AS
        SELECT *
        FROM read_parquet('{inp.as_posix()}')
        WHERE time_hour >= TIMESTAMP '{MIN_TS}'
          AND time_hour <= TIMESTAMP '{MAX_TS}'
        """
    )

    ranged_rows = con.execute("SELECT COUNT(*) FROM gold_ranged").fetchone()[0]
    removed_range = in_rows - ranged_rows
    print(f"🧹 Fuera de rango eliminadas: {removed_range:,}")
    print(f"✅ Filas tras rango: {ranged_rows:,}")

    # 3) Deduplicado GLOBAL:
    #    Nos quedamos con 1 fila por clave. Como tu dataset ya es “una fila por hora”,
    #    lo normal es que duplicados sean pocas filas “repetidas”.
    #
    #    Estrategia:
    #      - Si hay duplicados, elegimos una fila representativa con ANY_VALUE()
    #      - Para columnas numéricas (medias/lags) ANY_VALUE es suficiente si son clones.
    #
    #    Si quisieras lógica más estricta:
    #      - podríamos hacer AVG de las columnas numéricas
    #      - y MAX de obs_count
    #
    #    Aquí usamos ANY_VALUE para rapidez y simplicidad.
    cols = con.execute(
        f"DESCRIBE SELECT * FROM gold_ranged"
    ).fetchall()
    colnames = [c[0] for c in cols]

    # Construimos SELECT con ANY_VALUE para todas las columnas excepto KEYS
    select_exprs = []
    for c in colnames:
        if c in KEYS:
            select_exprs.append(c)
        else:
            select_exprs.append(f"ANY_VALUE({c}) AS {c}")

    select_sql = ",\n           ".join(select_exprs)

    print("🧠 Deduplicando globalmente por (station_id, time_hour)...")
    con.execute(
        f"""
        COPY (
            SELECT
                {select_sql}
            FROM gold_ranged
            GROUP BY {", ".join(KEYS)}
        )
        TO '{OUT_FILE.as_posix()}'
        (FORMAT PARQUET);
        """
    )

    # 4) Verificación rápida
    out_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUT_FILE.as_posix()}')"
    ).fetchone()[0]

    dup_keys = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT station_id, time_hour, COUNT(*) AS c
            FROM read_parquet('{OUT_FILE.as_posix()}')
            GROUP BY station_id, time_hour
            HAVING c > 1
        )
        """
    ).fetchone()[0]

    min_ts = con.execute(
        f"SELECT MIN(time_hour) FROM read_parquet('{OUT_FILE.as_posix()}')"
    ).fetchone()[0]
    max_ts = con.execute(
        f"SELECT MAX(time_hour) FROM read_parquet('{OUT_FILE.as_posix()}')"
    ).fetchone()[0]

    print("\n==============================")
    print(f"✅ OK -> {OUT_FILE}")
    print(f"Filas OUT: {out_rows:,}")
    print(f"Dup keys:  {dup_keys:,}")
    print(f"Rango:     {min_ts} -> {max_ts}")
    print("==============================\n")

    con.close()


if __name__ == "__main__":
    main()
