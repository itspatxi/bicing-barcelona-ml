"""
dedup_gold_global_duckdb.py
---------------------------
Deduplicado GLOBAL del GOLD (1 fila por station_id + time_hour).

¿Por qué hace falta?
- Aunque cada parte mensual gold_YYYYMM.parquet no tenga duplicados internos,
  al UNIR 80 partes puede haber SOLAPES entre meses.
- Eso genera claves repetidas (station_id, time_hour) en el parquet final.

Estrategia:
- Leer el parquet final (bicing_gold_dedup.parquet)
- GROUP BY station_id, time_hour
- Colapsar duplicados:
    * Columnas "medias" / meteo / lags / rolling -> AVG()
    * Columnas tipo hora/mes/día/flags/obs_count -> MAX()
  (Normalmente esos campos son iguales en duplicados; MAX no cambia nada,
   pero si hay discrepancias pequeñas, da un resultado determinista.)

Salida:
- data/gold/bicing_gold_dedup_global.parquet

Requisitos:
- pip install duckdb
"""

from __future__ import annotations

from pathlib import Path
import sys


def repo_root() -> Path:
    """
    Devuelve la raíz del repo suponiendo esta estructura:
    repo/
      src/gold/dedup_gold_global_duckdb.py

    parents[2]:
      - parents[0] = gold
      - parents[1] = src
      - parents[2] = repo_root
    """
    return Path(__file__).resolve().parents[2]


def safe_import_duckdb():
    """
    Importa duckdb y da un mensaje claro si no está instalado.
    """
    try:
        import duckdb  # type: ignore
        return duckdb
    except ImportError:
        print("\n❌ No tienes 'duckdb' instalado.")
        print("   Solución: pip install duckdb")
        print("   (Dentro del venv activo)")
        sys.exit(1)


def get_columns_via_pyarrow(parquet_path: Path) -> list[str]:
    """
    Obtiene la lista de columnas del Parquet sin cargarlo entero.
    Esto lo hacemos con pyarrow (ya lo tienes) para generar SQL dinámico.
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_path)
    schema = pf.schema_arrow
    return [field.name for field in schema]


def build_aggregation_sql(columns: list[str]) -> str:
    """
    Construye la lista de SELECT agregados en SQL.

    Reglas:
    - Keys: station_id, time_hour -> van tal cual (GROUP BY)
    - Campos "enteros/flags" -> MAX()
    - El resto numérico (medias, meteo, lags...) -> AVG()

    Nota:
    - Aquí no miramos tipos, solo nombres, para simplificar y hacerlo robusto.
      (Los nombres de tus columnas son muy estables.)
    """

    # Claves (deben existir sí o sí)
    keys = ["station_id", "time_hour"]

    # Campos que NO queremos promediar porque son categóricos/derivados/flags/contadores.
    # Si tus columnas cambian, añade aquí.
    use_max = {
        "obs_count",
        "hour",
        "dayofweek",
        "month",
        "date",
        "is_weekend",
        "is_holiday",
    }

    # Crea la lista SELECT
    select_exprs: list[str] = []
    select_exprs.extend(keys)

    for c in columns:
        if c in keys:
            continue

        if c in use_max:
            select_exprs.append(f"MAX({c}) AS {c}")
        else:
            # Para todo lo demás (medias, meteo, lags, rolling) usamos AVG
            # Si hay duplicados idénticos, AVG no cambia nada.
            select_exprs.append(f"AVG({c}) AS {c}")

    return ",\n    ".join(select_exprs)


def main():
    ROOT = repo_root()

    IN_PARQUET = ROOT / "data" / "gold" / "bicing_gold_dedup.parquet"
    OUT_PARQUET = ROOT / "data" / "gold" / "bicing_gold_dedup_global.parquet"

    print(f"📁 ROOT: {ROOT}")
    print(f"📥 IN : {IN_PARQUET}")
    print(f"📤 OUT: {OUT_PARQUET}")

    if not IN_PARQUET.exists():
        print("\n❌ No existe el fichero de entrada.")
        print("   Esperaba: data/gold/bicing_gold_dedup.parquet")
        sys.exit(1)

    # Importa duckdb (o te dice cómo instalarlo)
    duckdb = safe_import_duckdb()

    # Lista columnas (sin cargar datos)
    cols = get_columns_via_pyarrow(IN_PARQUET)
    if "station_id" not in cols or "time_hour" not in cols:
        print("\n❌ El parquet no tiene station_id/time_hour. No puedo deduplicar.")
        print("   Columnas encontradas:", cols[:30], "...")
        sys.exit(1)

    # Construye SQL dinámico para el GROUP BY
    select_list = build_aggregation_sql(cols)

    # DuckDB puede leer el parquet directamente
    # Importante: ordenamos al final para que el parquet sea más amigable en lectura
    sql = f"""
    COPY (
      SELECT
        {select_list}
      FROM read_parquet('{IN_PARQUET.as_posix()}')
      GROUP BY station_id, time_hour
      ORDER BY station_id, time_hour
    )
    TO '{OUT_PARQUET.as_posix()}'
    (FORMAT PARQUET);
    """

    print("\n🧠 Ejecutando deduplicado global (GROUP BY)...")
    con = duckdb.connect(database=":memory:")

    # Ejecuta (esto tarda dependiendo del disco; pero es la forma correcta y estable)
    con.execute(sql)
    con.close()

    print("\n✅ Deduplicado global terminado.")
    print(f"   Archivo creado: {OUT_PARQUET}")

    # Verificación rápida: contar duplicados
    print("\n🔍 Verificando duplicados en el resultado...")
    con = duckdb.connect(database=":memory:")
    dup = con.execute(
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT station_id || '|' || CAST(time_hour AS VARCHAR)) AS dup_keys
        FROM read_parquet('{OUT_PARQUET.as_posix()}')
        """
    ).fetchone()[0]
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUT_PARQUET.as_posix()}')"
    ).fetchone()[0]
    con.close()

    print(f"   rows={rows:,}")
    print(f"   dup_keys={dup:,}")

    if dup == 0:
        print("\n🎉 Perfecto: 0 duplicados (station_id + time_hour).")
    else:
        print("\n⚠️  Siguen quedando duplicados. Eso ya sería raro.")
        print("   En ese caso te hago un plan alternativo (normalmente no pasa).")


if __name__ == "__main__":
    main()
