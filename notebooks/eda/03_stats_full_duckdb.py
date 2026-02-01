# %%
from __future__ import annotations

from pathlib import Path
import duckdb

# %%
ROOT = Path().resolve()
if not (ROOT / "data").exists() and (ROOT.parent / "data").exists():
    ROOT = ROOT.parent
if not (ROOT / "data").exists() and (ROOT.parent.parent / "data").exists():
    ROOT = ROOT.parent.parent

DATA_GOLD = ROOT / "data" / "gold"
INP = DATA_GOLD / "bicing_gold_final.parquet"

REPORTS = ROOT / "reports"
TABLES = REPORTS / "tables"
TABLES.mkdir(parents=True, exist_ok=True)

def p2(p: Path) -> str:
    return str(p.resolve()).replace("\\", "/")

print("INP:", INP)

# %%
con = duckdb.connect(database=":memory:")
inp = p2(INP)

# %%
# Perfilado global (sin pandas)
q = f"""
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT station_id) AS stations,
  MIN(time_hour) AS min_time,
  MAX(time_hour) AS max_time,
  SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END) AS null_temp,
  SUM(CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END) AS null_precip
FROM read_parquet('{inp}');
"""
res = con.execute(q).fetchdf()
print(res)

res.to_csv(TABLES / "eda_global_profile.csv", index=False)

# %%
# Top columnas con nulos (aprox para varias cols)
q = f"""
SELECT
  SUM(CASE WHEN lag_24h_bikes IS NULL THEN 1 ELSE 0 END) AS null_lag24,
  SUM(CASE WHEN lag_1h_bikes IS NULL THEN 1 ELSE 0 END) AS null_lag1,
  SUM(CASE WHEN relative_humidity_2m IS NULL THEN 1 ELSE 0 END) AS null_rh,
  SUM(CASE WHEN pressure_msl IS NULL THEN 1 ELSE 0 END) AS null_pressure
FROM read_parquet('{inp}');
"""
res2 = con.execute(q).fetchdf()
print(res2)
res2.to_csv(TABLES / "eda_nulls_full.csv", index=False)

# %%
con.close()
print("✅ OK")
