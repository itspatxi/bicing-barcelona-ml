r"""
dedup_gold_by_parts.py

OBJETIVO
--------
Tienes 80 parquets mensuales en:
    data/gold/parts/gold_YYYYMM.parquet

Y quieres:
1) Deduplicar por (station_id, time_hour) DENTRO de cada parte
2) Guardar las partes deduplicadas en una carpeta intermedia
3) Unir todas las partes en un parquet final

POR QUÉ TE FALLABA ANTES
------------------------
1) En tu pyarrow no existe pyarrow.compute.unique_indices
   => AttributeError

2) Al unir parquets con ParquetWriter, TODAS las tablas tienen que tener
   EXACTAMENTE el mismo schema (mismo orden + mismos tipos + metadata compatible).
   En tu caso, relative_humidity_2m aparecía como int64 en algunas partes y float
   en otras => ValueError schema mismatch.

ESTA VERSIÓN (robusta y simple)
-------------------------------
- Dedup con pandas por parte (drop_duplicates) -> funciona siempre.
- Normaliza (castea) cada parte a un TARGET_SCHEMA fijo.
- Quita metadata "pandas" del schema para que no haya mismatch en el writer.

PROS / CONTRAS
--------------
✅ Pros:
- Compatible con versiones viejas de pyarrow
- Muy sencillo de entender y mantener
- Dedup por parte con ~300k filas suele ir bien

⚠️ Contras:
- Pandas es más lento que dedup 100% en Arrow, pero aquí es “seguro”.

EJECUCIÓN
---------
Desde la raíz del repo:
    python .\src\gold\dedup_gold_by_parts.py
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =========================
# CONFIGURACIÓN / RUTAS
# =========================

# Subimos 2 niveles: src/gold -> repo
ROOT = Path(__file__).resolve().parents[2]

PARTS_DIR = ROOT / "data" / "gold" / "parts"
DEDUP_DIR = ROOT / "data" / "gold" / "parts_dedup"

# Parquet final (después de unir todas las partes dedup)
OUT_GOLD = ROOT / "data" / "gold" / "bicing_gold_dedup.parquet"

# Clave de deduplicación (1 fila por estación y hora)
DEDUP_KEYS = ["station_id", "time_hour"]


# =========================
# SCHEMA OBJETIVO (FIJO)
# =========================
# Esto es CLAVE: si el schema cambia entre partes, el writer peta al unir.

TARGET_FIELDS: List[pa.Field] = [
    pa.field("station_id", pa.int64()),
    pa.field("time_hour", pa.timestamp("ms")),

    pa.field("bikes_available_mean", pa.float64()),
    pa.field("docks_available_mean", pa.float64()),
    pa.field("mechanical_mean", pa.float64()),
    pa.field("ebike_mean", pa.float64()),

    pa.field("obs_count", pa.int64()),
    pa.field("hour", pa.int32()),
    pa.field("dayofweek", pa.int32()),
    pa.field("month", pa.int32()),
    pa.field("date", pa.date32()),

    pa.field("is_weekend", pa.int8()),
    pa.field("is_holiday", pa.int8()),

    pa.field("temperature_2m", pa.float64()),
    pa.field("relative_humidity_2m", pa.float64()),  # forzamos float SIEMPRE
    pa.field("precipitation", pa.float64()),
    pa.field("wind_speed_10m", pa.float64()),
    pa.field("pressure_msl", pa.float64()),

    pa.field("lag_1h_bikes", pa.float64()),
    pa.field("lag_24h_bikes", pa.float64()),
    pa.field("roll3h_bikes_mean", pa.float64()),
]

TARGET_SCHEMA = pa.schema(TARGET_FIELDS).with_metadata(None)
TARGET_COLUMNS = [f.name for f in TARGET_FIELDS]


# =========================
# FUNCIONES AUXILIARES
# =========================

def list_gold_parts(parts_dir: Path) -> List[Path]:
    """Lista gold_*.parquet ordenados."""
    if not parts_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta: {parts_dir}")

    files = sorted(parts_dir.glob("gold_*.parquet"))
    if not files:
        raise RuntimeError(f"No he encontrado gold_*.parquet en: {parts_dir}")

    return files


def read_table_only_target_cols(path: Path) -> pa.Table:
    """
    Lee un parquet y se queda SOLO con las columnas que nos interesan (si existen).
    Si falta alguna columna, la rellenaremos después con nulls.
    """
    pf = pq.ParquetFile(path)
    file_cols = set(pf.schema_arrow.names)

    cols_to_read = [c for c in TARGET_COLUMNS if c in file_cols]
    return pq.read_table(path, columns=cols_to_read)


def ensure_all_columns_and_cast(table: pa.Table) -> pa.Table:
    """
    Garantiza:
    - todas las columnas TARGET_COLUMNS están presentes
    - mismo orden de columnas
    - mismo tipo (TARGET_SCHEMA)
    - metadata limpia (None)
    """
    existing = set(table.schema.names)

    arrays = []
    for field in TARGET_FIELDS:
        name = field.name
        if name in existing:
            arr = table[name]  # puede ser ChunkedArray, da igual para cast
        else:
            arr = pa.nulls(table.num_rows, type=field.type)
        arrays.append(arr)

    aligned = pa.table(arrays, names=TARGET_COLUMNS)

    # Cast seguro (safe=False permite int->float, etc.)
    casted = aligned.cast(TARGET_SCHEMA, safe=False)

    # Quitamos metadata para evitar mismatch por "pandas"
    casted = casted.replace_schema_metadata(None)
    return casted


def dedup_table_with_pandas_keep_first(table: pa.Table, keys: List[str]) -> Tuple[pa.Table, int]:
    """
    Deduplica usando pandas:
    - Convierte Arrow Table -> pandas DataFrame
    - drop_duplicates(subset=keys, keep='first')
    - vuelve a Arrow Table

    Devuelve (table_dedup, removed_rows)
    """
    n_in = table.num_rows

    # Pasamos a pandas (es por parte, ~300k filas normalmente)
    df = table.to_pandas()

    # drop_duplicates: se queda con la PRIMERA fila que aparece para cada key
    df2 = df.drop_duplicates(subset=keys, keep="first")

    removed = n_in - len(df2)

    # De vuelta a Arrow
    # preserve_index=False para no meter una columna extra del índice
    out = pa.Table.from_pandas(df2, preserve_index=False)

    # MUY IMPORTANTE: normalizamos a schema fijo para evitar problemas al unir
    out = ensure_all_columns_and_cast(out)

    return out, removed


def write_parquet(table: pa.Table, out_path: Path) -> None:
    """Escribe parquet con compresión snappy."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path, compression="snappy")


def concat_parquets_to_one(paths: List[Path], out_path: Path) -> int:
    """
    Une muchos parquets en uno usando ParquetWriter con schema fijo.

    Si alguna parte no coincide, la casteamos antes (por si acaso).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    with pq.ParquetWriter(out_path, TARGET_SCHEMA, compression="snappy") as writer:
        for p in paths:
            t = read_table_only_target_cols(p)
            t = ensure_all_columns_and_cast(t)

            writer.write_table(t)
            total_rows += t.num_rows

    return total_rows


# =========================
# MAIN
# =========================

def main() -> None:
    print(f"📁 ROOT: {ROOT}")
    print(f"📦 Partes GOLD: {PARTS_DIR}")

    parts = list_gold_parts(PARTS_DIR)
    print(f"Encontradas {len(parts)} partes: {parts[0].name} ... {parts[-1].name}")

    # Carpeta intermedia de dedup
    DEDUP_DIR.mkdir(parents=True, exist_ok=True)

    dedup_parts: List[Path] = []

    # 1) Deduplicar por parte
    for i, in_path in enumerate(parts, start=1):
        print(f"\n🧹 [{i}/{len(parts)}] Deduplicando: {in_path.name}")

        table = read_table_only_target_cols(in_path)
        table = ensure_all_columns_and_cast(table)

        deduped, removed = dedup_table_with_pandas_keep_first(table, DEDUP_KEYS)

        out_path = DEDUP_DIR / in_path.name
        write_parquet(deduped, out_path)

        print(
            f"   ✅ out={out_path.name} | in={table.num_rows:,} -> out={deduped.num_rows:,} | removed={removed:,}"
        )
        dedup_parts.append(out_path)

    # 2) Unir todas las partes dedup
    print(f"\n🔗 Uniendo {len(dedup_parts)} partes dedup en 1 parquet final...")
    total_rows = concat_parquets_to_one(dedup_parts, OUT_GOLD)

    print("\n==============================")
    print(f"✅ OK -> {OUT_GOLD}")
    print(f"Total filas (sumando partes): {total_rows:,}")
    print("==============================\n")

    # 3) Mini check rápido (sin cargar todo el parquet final a pandas)
    #    OJO: aquí solo comprobamos que el fichero existe, no el dedup global.
    if OUT_GOLD.exists():
        meta = pq.ParquetFile(OUT_GOLD).metadata
        print(f"📌 Parquet final creado. Row groups: {meta.num_row_groups} | rows: {meta.num_rows:,}")


if __name__ == "__main__":
    main()
