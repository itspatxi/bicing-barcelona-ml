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
# Señales: lluvia intensa (p99 aprox), viento fuerte (p99), frío/calor extremos (p01/p99)
# Aquí fijamos umbrales "a ojo" y luego los ajustas con 05b_percentiles.csv
q = f"""
SELECT
  COUNT(*) AS rows,
  AVG(CASE WHEN precipitation >= 1.0 THEN 1 ELSE 0 END) AS pct_rain_heavy,
  AVG(CASE WHEN wind_speed_10m >= 25 THEN 1 ELSE 0 END) AS pct_wind_strong,
  AVG(CASE WHEN temperature_2m <= 5 THEN 1 ELSE 0 END) AS pct_cold,
  AVG(CASE WHEN temperature_2m >= 30 THEN 1 ELSE 0 END) AS pct_hot,
  AVG(CASE WHEN is_holiday=1 THEN 1 ELSE 0 END) AS pct_holiday,
  AVG(CASE WHEN is_weekend=1 THEN 1 ELSE 0 END) AS pct_weekend
FROM read_parquet('{inp}');
"""
df = con.execute(q).fetchdf()
df.to_csv(OUT / "eda_feature_flag_rates.csv", index=False)
print(df)
print("✅", OUT / "eda_feature_flag_rates.csv")

con.close()
