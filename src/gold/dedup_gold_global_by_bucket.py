r"""
dedup_gold_global_by_bucket.py

OBJETIVO
--------
Eliminar duplicados por (station_id, time_hour) A NIVEL GLOBAL
(en todo el parquet), sin cargar 27M filas en pandas de golpe.

ESTRATEGIA (robusta)
--------------------
1) Leemos bicing_gold_dedup.parquet por row-groups (80 normalmente)
2) Escribimos filas a N buckets por hash(station_id) % N
3) Para cada bucket:
   - lo leemos
   - ordenamos por station_id,time_hour
   - drop_duplicates
   - lo guardamos como parte final
4) Unimos buckets dedup en un parquet final

VENTAJAS
--------
✅ No necesitas RAM bestia
✅ Evitas problemas de "solapes entre meses"
✅ Controlable y reproducible

EJECUCIÓN
---------
python .\src\gold\dedup_gold_global_by_bucket.py

SALIDAS
-------
- data/gold/bicing_gold_dedup_global.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# =========================
# CONFIG
# =========================

ROOT = Path(__file__).resolve().parents[2]

IN_FILE = ROOT / "data" / "gold" / "bicing_gold_dedup.parquet"
BUCKET_DIR = ROOT / "data" / "gold" / "buckets_tmp"
OUT_FILE = ROOT / "data" / "gold" / "bicing_gold_dedup_global.parquet"

# Número de buckets: más buckets => menos memoria por bucket, pero más ficheros.
# Con 64 suele ir bien en un PC normal.
N_BUCKETS = 64

# Clave de deduplicación global
KEYS = ["station_id", "time_hour"]


# =========================
# HELPERS
# =========================

def get_target_schema() -> pa.Schema:
    """
    Coge el schema del fichero de entrada y le quita metadata
    para evitar problemas al escribir con ParquetWriter.
    """
    pf = pq.ParquetFile(IN_FILE)
    schema = pf.schema_arrow
    return schema.with_metadata(None)


def ensure_dirs() -> None:
    BUCKET_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def clean_bucket_dir() -> None:
    """
    Borra buckets anteriores para no mezclar runs.
    """
    if BUCKET_DIR.exists():
        for f in BUCKET_DIR.glob("bucket_*.parquet"):
            try:
                f.unlink()
            except Exception:
                pass


def bucket_id_from_station_id(arr_station: pa.Array) -> pa.Array:
    """
    Calcula bucket = (station_id % N_BUCKETS) para int64.
    """
    # station_id es int64; usamos módulo con compute (rápido)
    return pc.mod(arr_station, pa.scalar(N_BUCKETS, type=pa.int64()))


def append_to_bucket(
    writers: Dict[int, pq.ParquetWriter],
    bucket: int,
    table: pa.Table,
    schema: pa.Schema,
    out_path: Path,
) -> None:
    """
    Escribe una tabla al bucket correspondiente usando un writer abierto.
    """
    if bucket not in writers:
        writers[bucket] = pq.ParquetWriter(out_path, schema, compression="snappy")
    writers[bucket].write_table(table)


def close_writers(writers: Dict[int, pq.ParquetWriter]) -> None:
    for w in writers.values():
        try:
            w.close()
        except Exception:
            pass


def dedup_bucket_file(path_in: Path, path_out: Path) -> Tuple[int, int]:
    """
    Deduplica UN bucket (normalmente mucho más pequeño que el dataset completo).
    Devuelve (rows_in, rows_out)
    """
    # Leemos todo el bucket (ya está troceado para que quepa)
    table = pq.read_table(path_in)
    df = table.to_pandas()

    rows_in = len(df)

    # Ordenamos para que drop_duplicates sea estable
    df = df.sort_values(KEYS, kind="mergesort")

    # Dedup global dentro de este bucket
    df2 = df.drop_duplicates(subset=KEYS, keep="first")
    rows_out = len(df2)

    # Volvemos a Arrow y escribimos
    out_table = pa.Table.from_pandas(df2, preserve_index=False).replace_schema_metadata(None)
    pq.write_table(out_table, path_out, compression="snappy")

    return rows_in, rows_out


def concat_parts_to_one(parts: List[Path], out_path: Path, schema: pa.Schema) -> int:
    """
    Une partes dedup en un único parquet final.
    """
    total = 0
    with pq.ParquetWriter(out_path, schema, compression="snappy") as writer:
        for p in parts:
            t = pq.read_table(p)
            t = t.replace_schema_metadata(None)
            writer.write_table(t)
            total += t.num_rows
    return total


# =========================
# MAIN
# =========================

def main() -> None:
    if not IN_FILE.exists():
        raise FileNotFoundError(f"No existe IN_FILE: {IN_FILE}")

    ensure_dirs()
    clean_bucket_dir()

    schema = get_target_schema()

    print(f"📌 IN:  {IN_FILE}")
    print(f"📦 BUCKET_DIR: {BUCKET_DIR}")
    print(f"🧱 N_BUCKETS: {N_BUCKETS}")
    print(f"📌 OUT: {OUT_FILE}")

    pf = pq.ParquetFile(IN_FILE)
    print(f"Row groups: {pf.metadata.num_row_groups} | rows: {pf.metadata.num_rows:,}")

    # 1) Particionar a buckets
    writers: Dict[int, pq.ParquetWriter] = {}
    try:
        for rg in range(pf.metadata.num_row_groups):
            table = pf.read_row_group(rg)
            table = table.replace_schema_metadata(None)

            # Calculamos bucket por fila: station_id % N_BUCKETS
            station = table["station_id"]
            b_ids = bucket_id_from_station_id(station)

            # Para cada bucket, filtramos filas y escribimos
            # (esto es N_BUCKETS filtros pequeños por row-group)
            for b in range(N_BUCKETS):
                mask = pc.equal(b_ids, pa.scalar(b, type=pa.int64()))
                # Si no hay filas, skip
                if pc.any(mask).as_py() is not True:
                    continue
                sub = table.filter(mask)
                out_path = BUCKET_DIR / f"bucket_{b:02d}.parquet"
                append_to_bucket(writers, b, sub, schema, out_path)

            print(f"   ✅ particionado row-group {rg+1}/{pf.metadata.num_row_groups}")

    finally:
        close_writers(writers)

    # 2) Dedup de cada bucket
    bucket_files = sorted(BUCKET_DIR.glob("bucket_*.parquet"))
    if not bucket_files:
        raise RuntimeError("No se han creado buckets. Algo ha ido mal.")

    dedup_parts: List[Path] = []
    total_in = 0
    total_out = 0

    print("\n🧹 Deduplicando buckets...")
    for i, bfile in enumerate(bucket_files, start=1):
        out_part = BUCKET_DIR / f"dedup_{bfile.name}"
        rows_in, rows_out = dedup_bucket_file(bfile, out_part)
        total_in += rows_in
        total_out += rows_out
        dedup_parts.append(out_part)
        print(f"   ✅ [{i}/{len(bucket_files)}] {bfile.name}: in={rows_in:,} -> out={rows_out:,} (removed={rows_in-rows_out:,})")

    # 3) Unir buckets dedup a parquet final
    print("\n🔗 Uniendo buckets dedup en parquet final...")
    final_rows = concat_parts_to_one(dedup_parts, OUT_FILE, schema)

    print("\n==============================")
    print("✅ DEDUP GLOBAL COMPLETADO")
    print(f"rows_in_total:  {total_in:,}")
    print(f"rows_out_total: {total_out:,}")
    print(f"rows_final:     {final_rows:,}")
    print(f"removed_total:  {total_in - total_out:,}")
    print(f"OUT -> {OUT_FILE}")
    print("==============================\n")


if __name__ == "__main__":
    main()
