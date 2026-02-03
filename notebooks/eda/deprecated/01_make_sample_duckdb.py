# %%
from __future__ import annotations

from pathlib import Path
import duckdb
import pyarrow.parquet as pq

# %%
ROOT = Path().resolve()
if not (ROOT / "data").exists() and (ROOT.parent / "data").exists():
    ROOT = ROOT.parent
if not (ROOT / "data").exists() and (ROOT.parent.parent / "data").exists():
    ROOT = ROOT.parent.parent

DATA_GOLD = ROOT / "data" / "gold"
INP = DATA_GOLD / "bicing_gold_final.parquet"

OUT_SAMPLES = DATA_GOLD / "samples"
OUT_SAMPLES.mkdir(parents=True, exist_ok=True)

SAMPLE_1M = OUT_SAMPLES / "bicing_gold_final_sample_1M.parquet"

print("ROOT:", ROOT)
print("INP :", INP)
print("OUT :", SAMPLE_1M)

# %%
if not INP.exists():
    raise FileNotFoundError(f"No existe: {INP}")

print("Rows (metadata):", pq.ParquetFile(INP).metadata.num_rows)

# %%
def duckdb_path(p: Path) -> str:
    return str(p.resolve()).replace("\\", "/")

if not SAMPLE_1M.exists():
    con = duckdb.connect(database=":memory:")
    inp = duckdb_path(INP)
    outp = duckdb_path(SAMPLE_1M)

    con.execute(
        f"""
        COPY (
          SELECT *
          FROM read_parquet('{inp}')
          USING SAMPLE 1000000 ROWS
        )
        TO '{outp}' (FORMAT PARQUET);
        """
    )
    con.close()
    print("✅ Sample creado:", SAMPLE_1M)
else:
    print("✅ Sample ya existe:", SAMPLE_1M)

print("Rows(sample metadata):", pq.ParquetFile(SAMPLE_1M).metadata.num_rows)
