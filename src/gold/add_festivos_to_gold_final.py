# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import duckdb


def find_project_root(start: Path, max_levels: int = 10) -> Path:
    """
    Intenta detectar el 'root' del proyecto subiendo carpetas desde `start`.

    Criterio de root:
    - Existe una carpeta "data" (típico en pipelines tipo bronze/silver/gold), o
    - Existe ".git" (indica repo git).

    Si no lo encuentra tras `max_levels` niveles, devuelve el `start` resuelto.
    """
    cur = start.resolve()

    # Subimos como mucho `max_levels` niveles para evitar recorrer todo el FS por error.
    for _ in range(max_levels):
        # Si detectamos señales típicas de root, paramos aquí.
        if (cur / "data").exists() or (cur / ".git").exists():
            return cur

        # Si llegamos a la raíz del sistema (parent == self), ya no podemos subir más.
        if cur.parent == cur:
            break

        cur = cur.parent

    # Si no encontramos root, devolvemos la ruta inicial resuelta.
    return start.resolve()


def _sql_escape_path(p: Path) -> str:
    """
    Escapa una ruta para incrustarla en SQL con comillas simples.

    En SQL, una comilla simple dentro de un literal se escapa duplicándola:
    ' -> ''
    """
    return p.as_posix().replace("'", "''")


def main() -> None:
    # 1) Localizamos el root del proyecto para construir rutas de forma estable.
    root = find_project_root(Path.cwd())

    # 2) Definimos rutas de entrada/salida.
    gold_in = root / "data" / "gold" / "bicing_gold_final.parquet"
    fest_csv = root / "data" / "silver" / "festivos" / "festivos_bcn_2019_2025.csv"
    gold_out = root / "data" / "gold" / "bicing_gold_final_plus.parquet"

    # 3) Validamos que los ficheros existan antes de abrir DuckDB (fallo temprano).
    if not gold_in.exists():
        raise FileNotFoundError(f"No existe: {gold_in}")
    if not fest_csv.exists():
        raise FileNotFoundError(f"No existe: {fest_csv}")

    # 4) Preparamos rutas escapadas por si contienen caracteres raros.
    gold_in_sql = _sql_escape_path(gold_in)
    fest_csv_sql = _sql_escape_path(fest_csv)
    gold_out_sql = _sql_escape_path(gold_out)

    # 5) Construimos el SQL:
    #    - Leemos el parquet "gold" como tabla `g`.
    #    - Leemos el CSV de festivos como tabla `f`.
    #    - LEFT JOIN por fecha para mantener todas las filas de `g`.
    #
    # Nota: aquí asumimos que `f.date` y `g.date` son parseables a DATE.
    # Si el CSV trae formatos raros, `read_csv_auto` puede interpretarlo como VARCHAR
    # y el CAST seguirá funcionando si el formato es ISO-like; si no, petará.
    #
    # Ojo: si en `f` hay varias filas por una misma fecha, esto DUPLICA filas de `g`.
    # (eso puede ser un problemilla si luego haces métricas por recuento)
    query = f"""
    COPY (
      SELECT
        g.*,

        -- is_holiday_new: flag "general" de festivo.
        -- Si no hay match en `f`, COALESCE lo baja a 0.
        COALESCE(f.is_holiday, 0) AS is_holiday_new,

        -- Flags por ámbito/scope.
        -- Usamos LOWER para hacer el match case-insensitive (más robusto).
        CASE WHEN LOWER(f.scope) LIKE '%barcelona%' THEN 1 ELSE 0 END AS is_holiday_barcelona,
        CASE WHEN LOWER(f.scope) LIKE '%catalunya%' THEN 1 ELSE 0 END AS is_holiday_catalunya,
        CASE WHEN LOWER(f.scope) LIKE '%spain%' THEN 1 ELSE 0 END AS is_holiday_spain,

        -- Campos informativos para auditoría / Power BI.
        f.scope AS holiday_scope,
        f.name  AS holiday_name

      FROM read_parquet('{gold_in_sql}') g
      LEFT JOIN read_csv_auto('{fest_csv_sql}') f
        ON CAST(g.date AS DATE) = CAST(f.date AS DATE)
    ) TO '{gold_out_sql}' (FORMAT PARQUET);
    """

    print("🧠 Añadiendo festivos a GOLD...")

    # 6) Ejecutamos con un contexto `with` para asegurar cierre pase lo que pase.
    with duckdb.connect() as con:
        con.execute(query)

    print("✅ OK")
    print("   IN :", gold_in)
    print("   OUT:", gold_out)

    # 7) Mini chequeo rápido:
    #    - rows: total filas
    #    - rows_holiday: cuántas marcamos como festivo general
    #    - rows_holiday_bcn: cuántas caen en ámbito Barcelona
    #
    # Esto no valida todo, pero da un olorcillo rápido de si algo fue mal.
    with duckdb.connect() as con:
        chk = con.execute(f"""
          SELECT
            COUNT(*) AS rows,
            SUM(is_holiday_new) AS rows_holiday,
            SUM(is_holiday_barcelona) AS rows_holiday_bcn
          FROM read_parquet('{gold_out_sql}')
        """).fetchdf()

    print(chk.to_string(index=False))


if __name__ == "__main__":
    main()
