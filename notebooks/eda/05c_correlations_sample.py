# %%
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

# %%
ROOT = Path().resolve()
if not (ROOT / "data").exists() and (ROOT.parent / "data").exists():
    ROOT = ROOT.parent
if not (ROOT / "data").exists() and (ROOT.parent.parent / "data").exists():
    ROOT = ROOT.parent.parent

SAMPLE = ROOT / "data" / "gold" / "samples" / "bicing_gold_final_sample_1M.parquet"
OUT = ROOT / "reports" / "tables"
OUT.mkdir(parents=True, exist_ok=True)

df = pq.read_table(SAMPLE).to_pandas()

cols = [
    "bikes_available_mean",
    "docks_available_mean",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "pressure_msl",
    "lag_1h_bikes",
    "lag_24h_bikes",
    "roll3h_bikes_mean",
    "obs_count",
]
df = df[cols].copy()

pearson = df.corr(numeric_only=True, method="pearson")
spearman = df.corr(numeric_only=True, method="spearman")

pearson.to_csv(OUT / "eda_corr_pearson_sample.csv")
spearman.to_csv(OUT / "eda_corr_spearman_sample.csv")

print("✅", OUT / "eda_corr_pearson_sample.csv")
print("✅", OUT / "eda_corr_spearman_sample.csv")
print(pearson["bikes_available_mean"].sort_values(ascending=False).head(10))
