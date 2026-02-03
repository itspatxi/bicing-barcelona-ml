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
q = f"""
SELECT
  quantile_cont(bikes_available_mean, 0.01) AS p01_bikes,
  quantile_cont(bikes_available_mean, 0.05) AS p05_bikes,
  quantile_cont(bikes_available_mean, 0.50) AS p50_bikes,
  quantile_cont(bikes_available_mean, 0.95) AS p95_bikes,
  quantile_cont(bikes_available_mean, 0.99) AS p99_bikes,

  quantile_cont(temperature_2m, 0.01) AS p01_temp,
  quantile_cont(temperature_2m, 0.50) AS p50_temp,
  quantile_cont(temperature_2m, 0.99) AS p99_temp,

  quantile_cont(precipitation, 0.99) AS p99_precip,
  quantile_cont(wind_speed_10m, 0.99) AS p99_wind
FROM read_parquet('{inp}');
"""
df = con.execute(q).fetchdf()
df.to_csv(OUT / "eda_percentiles.csv", index=False)
print(df)
print("✅", OUT / "eda_percentiles.csv")

con.close()
