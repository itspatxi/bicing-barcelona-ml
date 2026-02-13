# -*- coding: utf-8 -*-
# Indicamos explícitamente la codificación del fichero.
# Esto evita problemas raros si algún día hay acentos en comentarios o strings.

from __future__ import annotations
# Permite usar anotaciones de tipo modernas sin que se evalúen inmediatamente.
# Es útil sobre todo si trabajas con tipos que aún no están definidos o para
# evitar imports circulares en proyectos grandes.

from pathlib import Path
# Pathlib nos da una forma orientada a objetos de trabajar con rutas,
# mucho más limpia que usar os.path.

import duckdb
# DuckDB es una base de datos analítica embebida (tipo SQLite pero pensada
# para analítica y columnar). Aquí la usamos como motor SQL sobre parquet y CSV.


def find_project_root(start: Path, max_levels: int = 10) -> Path:
    """
    Intenta localizar la raíz del proyecto subiendo niveles de directorio
    desde la ruta inicial `start`.

    Consideramos que estamos en el root si:
    - Existe una carpeta llamada "data" (típico en pipelines tipo bronze/silver/gold)
    - O existe una carpeta ".git" (indicador de repositorio)

    Si no lo encuentra tras `max_levels` niveles, devuelve la ruta inicial.
    """

    # Resolvemos la ruta (convierte a absoluta y limpia posibles "..")
    cur = start.resolve()

    # Iteramos un número limitado de veces para evitar recorrer todo el sistema
    # de archivos si por error ejecutamos esto desde un sitio raro.
    for _ in range(max_levels):

        # Comprobamos si la carpeta actual tiene señales de ser root del proyecto.
        if (cur / "data").exists() or (cur / ".git").exists():
            return cur  # Encontrado, devolvemos inmediatamente.

        # Si llegamos a la raíz del sistema (ej: "/" en Linux),
        # el parent será igual a sí mismo. En ese caso paramos.
        if cur.parent == cur:
            break

        # Subimos un nivel.
        cur = cur.parent

    # Si no hemos encontrado nada, devolvemos el start resuelto.
    # Esto puede no ser el root real, pero evitamos que falle sin control.
    return start.resolve()


def _sql_escape_path(p: Path) -> str:
    """
    Escapa la ruta para usarla dentro de una string SQL delimitada por comillas simples.

    En SQL, una comilla simple se escapa duplicándola:
        '  ->  ''
    Esto evita que el SQL se rompa si la ruta contiene caracteres raros.
    """

    return p.as_posix().replace("'", "''")
    # as_posix() fuerza formato con "/" incluso en Windows.
    # replace duplica comillas simples para evitar errores de sintaxis.


def main() -> None:

    # ---------------------------------------------------------------------
    # 1) Localizamos el root del proyecto
    # ---------------------------------------------------------------------
    # Partimos del directorio actual (cwd) y subimos niveles si hace falta.
    root = find_project_root(Path.cwd())

    # ---------------------------------------------------------------------
    # 2) Definimos rutas de entrada y salida
    # ---------------------------------------------------------------------
    # Construimos rutas de forma declarativa, evitando strings hardcodeadas.
    gold_in = root / "data" / "gold" / "bicing_gold_final.parquet"
    fest_csv = root / "data" / "silver" / "festivos" / "festivos_bcn_2019_2025.csv"
    gold_out = root / "data" / "gold" / "bicing_gold_final_plus.parquet"

    # ---------------------------------------------------------------------
    # 3) Validaciones tempranas
    # ---------------------------------------------------------------------
    # Es mejor fallar aquí que dentro del SQL con un error críptico.
    if not gold_in.exists():
        raise FileNotFoundError(f"No existe: {gold_in}")

    if not fest_csv.exists():
        raise FileNotFoundError(f"No existe: {fest_csv}")

    # ---------------------------------------------------------------------
    # 4) Escapamos rutas para SQL
    # ---------------------------------------------------------------------
    # Aunque sea un script local, es buena práctica evitar que una comilla
    # rompa el SQL por accidente (esto ahorra sustos tontos).
    gold_in_sql = _sql_escape_path(gold_in)
    fest_csv_sql = _sql_escape_path(fest_csv)
    gold_out_sql = _sql_escape_path(gold_out)

    # ---------------------------------------------------------------------
    # 5) Construcción de la query principal
    # ---------------------------------------------------------------------
    # Esta query hace lo siguiente:
    #
    # - Lee el parquet GOLD como tabla virtual "g"
    # - Lee el CSV de festivos como tabla virtual "f"
    # - Hace un LEFT JOIN por fecha
    # - Añade columnas nuevas relacionadas con festivos
    # - Exporta el resultado a un nuevo parquet
    #
    # IMPORTANTE:
    # Si en el CSV hay varias filas para la misma fecha,
    # el LEFT JOIN duplicará filas de g. Esto puede distorsionar métricas
    # agregadas posteriores (por ejemplo conteos). Ojo con eso.
    #
    # También asumimos que g.date y f.date pueden convertirse a DATE.
    # Si el CSV trae fechas en formato extraño, el CAST puede petar.
    query = f"""
    COPY (
      SELECT
        g.*,

        -- Flag general de festivo.
        -- Si no hay coincidencia en el join (f es NULL),
        -- COALESCE fuerza a 0 en vez de dejar NULL.
        COALESCE(f.is_holiday, 0) AS is_holiday_new,

        -- Flags por ambito geografico.
        -- LOWER() permite comparar sin importar mayúsculas/minúsculas.
        -- LIKE '%texto%' busca coincidencia parcial.
        CASE WHEN LOWER(f.scope) LIKE '%barcelona%' THEN 1 ELSE 0 END AS is_holiday_barcelona,
        CASE WHEN LOWER(f.scope) LIKE '%catalunya%' THEN 1 ELSE 0 END AS is_holiday_catalunya,
        CASE WHEN LOWER(f.scope) LIKE '%spain%' THEN 1 ELSE 0 END AS is_holiday_spain,

        -- Información descriptiva adicional.
        -- Útil para trazabilidad, reporting o debugging.
        f.scope AS holiday_scope,
        f.name  AS holiday_name

      FROM read_parquet('{gold_in_sql}') g

      LEFT JOIN read_csv_auto('{fest_csv_sql}') f
        ON CAST(g.date AS DATE) = CAST(f.date AS DATE)

    ) TO '{gold_out_sql}' (FORMAT PARQUET);
    """

    print("🧠 Añadiendo festivos a GOLD...")

    # ---------------------------------------------------------------------
    # 6) Ejecución de la query
    # ---------------------------------------------------------------------
    # Usamos contexto `with` para asegurar que la conexión se cierra
    # automáticamente incluso si algo falla.
    with duckdb.connect() as con:
        con.execute(query)

    print("✅ OK")
    print("   IN :", gold_in)
    print("   OUT:", gold_out)

    # ---------------------------------------------------------------------
    # 7) Chequeo rápido de consistencia
    # ---------------------------------------------------------------------
    # Hacemos un pequeño resumen:
    #   - total filas
    #   - total marcadas como festivo
    #   - total festivos en ámbito Barcelona
    #
    # Esto no valida todo el pipeline, pero nos da una señal rápida
    # de si algo raro pasó (por ejemplo 0 festivos inesperadamente).
    with duckdb.connect() as con:
        chk = con.execute(f"""
          SELECT
            COUNT(*) AS rows,
            SUM(is_holiday_new) AS rows_holiday,
            SUM(is_holiday_barcelona) AS rows_holiday_bcn
          FROM read_parquet('{gold_out_sql}')
        """).fetchdf()

    # Mostramos el resultado como tabla sin índice.
    print(chk.to_string(index=False))


# Punto de entrada del script.
# Esto permite que el archivo pueda importarse como módulo
# sin ejecutar automáticamente el main (detalle importante).
if __name__ == "__main__":
    main()
