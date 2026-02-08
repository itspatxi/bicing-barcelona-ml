# notebooks/eda/00_make_sample_ml_features.py
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
inp = ROOT / "data" / "gold" / "bicing_gold_ml_features_tplus1.parquet"
out = ROOT / "data" / "gold" / "samples" / "bicing_gold_ml_features_tplus1_sample_1M.parquet"
out.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"""
COPY (
  SELECT *
  FROM read_parquet('{inp.as_posix()}')
  USING SAMPLE 1000000 ROWS
) TO '{out.as_posix()}' (FORMAT PARQUET);
""")
con.close()

print("✅", out)
