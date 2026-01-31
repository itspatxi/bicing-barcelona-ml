# src/gold/make_sample_duckdb.py
"""
Crea un sample aleatorio de 1M filas desde el parquet GOLD usando DuckDB.

✅ Cómo ejecutarlo (da igual desde qué carpeta lo lances):
    python .\\src\\gold\\make_sample_duckdb.py

Qué hace:
- Lee:  data/gold/bicing_gold_dedup_global.parquet
- Saca: 1,000,000 filas aleatorias (SAMPLE ... ROWS)
- Guarda: data/gold/bicing_gold_dedup_global_sample_1M.parquet
"""

from __future__ import annotations

import pathlib
import duckdb


def find_repo_root() -> pathlib.Path:
    """
    Encuentra la raíz del repo de forma robusta.

    Idea:
    - Este script vive en: <repo>/src/gold/make_sample_duckdb.py
    - Así que la raíz del repo es: script_dir -> sube 2 niveles.
    """
    script_path = pathlib.Path(__file__).resolve()
    repo_root = script_path.parents[2]  # .../<repo>
    return repo_root


def main() -> None:
    # 1) Detectar raíz del repo (NO depende del "cd" que tengas en PowerShell)
    root = find_repo_root()

    # 2) Definir rutas input/output dentro del repo
    inp = root / "data" / "gold" / "bicing_gold_dedup_global.parquet"
    outp = root / "data" / "gold" / "bicing_gold_dedup_global_sample_1M.parquet"

    # 3) Comprobaciones básicas
    if not inp.exists():
        # Mensaje con pistas útiles para depurar en 5 segundos
        raise FileNotFoundError(
            "No existe el input.\n"
            f"  Esperaba: {inp}\n"
            f"  root detectado: {root}\n"
            "Pista: comprueba que el fichero se llame exactamente "
            "'bicing_gold_dedup_global.parquet' dentro de data/gold/."
        )

    outp.parent.mkdir(parents=True, exist_ok=True)

    # 4) SQL DuckDB: SAMPLE N ROWS (aleatorio) y escribimos PARQUET
    # Nota: as_posix() mete / en vez de \ para evitar líos en SQL.
    sql = f"""
    COPY (
        SELECT *
        FROM read_parquet('{inp.as_posix()}')
        USING SAMPLE 1000000 ROWS
    )
    TO '{outp.as_posix()}'
    (FORMAT PARQUET);
    """

    # 5) Ejecutar
    con = duckdb.connect()
    con.execute(sql)
    con.close()

    print(f"✅ Sample creado: {outp}")


if __name__ == "__main__":
    main()
