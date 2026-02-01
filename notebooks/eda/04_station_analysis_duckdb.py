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

inp = p2(INP)
con = duckdb.connect(database=":memory:")

# %%
# Ranking estaciones por media / variabilidad
q = f"""
SELECT
  station_id,
  AVG(bikes_available_mean) AS mean_bikes,
  STDDEV_SAMP(bikes_available_mean) AS std_bikes,
  COUNT(*) AS n
FROM read_parquet('{inp}')
GROUP BY station_id
ORDER BY std_bikes DESC
LIMIT 50;
"""
top_std = con.execute(q).fetchdf()
print(top_std.head(10))
top_std.to_csv(TABLES / "eda_top50_station_std_full.csv", index=False)

# %%
# Patrones por hora (agregado global)
q2 = f"""
SELECT
  hour,
  AVG(bikes_available_mean) AS mean_bikes,
  AVG(temperature_2m) AS mean_temp,
  AVG(precipitation) AS mean_precip
FROM read_parquet('{inp}')
GROUP BY hour
ORDER BY hour;
"""
by_hour = con.execute(q2).fetchdf()
print(by_hour.head())
by_hour.to_csv(TABLES / "eda_by_hour_full.csv", index=False)

# %%
con.close()
print("✅ OK")
