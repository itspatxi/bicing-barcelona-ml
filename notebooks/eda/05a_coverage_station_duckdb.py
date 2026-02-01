# %%
from pathlib import Path
import duckdb

# %%
ROOT = Path().resolve()
if not (ROOT / "data").exists() and (ROOT.parent / "data").exists():
    ROOT = ROOT.parent
if not (ROOT / "data").exists() and (ROOT.parent.parent / "data").exists():
    ROOT = ROOT.parent.parent

INP = ROOT / "data" / "gold" / "bicing_gold_final.parquet"
OUT = ROOT / "reports" / "tables"
OUT.mkdir(parents=True, exist_ok=True)

def p2(p: Path) -> str:
    return str(p.resolve()).replace("\\", "/")

con = duckdb.connect(database=":memory:")
inp = p2(INP)

# %%
# Cobertura por estación: min/max, n, %nulos lags
q = f"""
SELECT
  station_id,
  COUNT(*) AS n_rows,
  MIN(time_hour) AS min_time,
  MAX(time_hour) AS max_time,
  AVG(CASE WHEN lag_1h_bikes IS NULL THEN 1 ELSE 0 END) AS pct_null_lag1,
  AVG(CASE WHEN lag_24h_bikes IS NULL THEN 1 ELSE 0 END) AS pct_null_lag24,
  AVG(obs_count) AS mean_obs_count
FROM read_parquet('{inp}')
GROUP BY station_id
ORDER BY n_rows DESC;
"""
df = con.execute(q).fetchdf()
df.to_csv(OUT / "eda_station_coverage.csv", index=False)
print("✅", OUT / "eda_station_coverage.csv", "| rows=", len(df))

con.close()
