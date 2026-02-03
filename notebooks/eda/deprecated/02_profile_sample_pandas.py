# %%
from __future__ import annotations

from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

pd.set_option("display.max_columns", 200)

# %%
ROOT = Path().resolve()
if not (ROOT / "data").exists() and (ROOT.parent / "data").exists():
    ROOT = ROOT.parent
if not (ROOT / "data").exists() and (ROOT.parent.parent / "data").exists():
    ROOT = ROOT.parent.parent

DATA_GOLD = ROOT / "data" / "gold"
SAMPLE_1M = DATA_GOLD / "samples" / "bicing_gold_final_sample_1M.parquet"

REPORTS = ROOT / "reports"
FIGURES = REPORTS / "figures"
TABLES = REPORTS / "tables"
FIGURES.mkdir(parents=True, exist_ok=True)
TABLES.mkdir(parents=True, exist_ok=True)

print("SAMPLE:", SAMPLE_1M)

# %%
if not SAMPLE_1M.exists():
    raise FileNotFoundError(
        f"No existe el sample: {SAMPLE_1M}\n"
        "Ejecuta antes: notebooks/eda/01_make_sample_duckdb.py"
    )

df = pq.read_table(SAMPLE_1M).to_pandas()

# asegurar datetime
df["time_hour"] = pd.to_datetime(df["time_hour"], errors="coerce")

print("rows=", len(df), "cols=", df.shape[1])
print("min=", df["time_hour"].min(), "max=", df["time_hour"].max())
print("dup_keys=", df.duplicated(["station_id", "time_hour"]).sum())

# %%
# Nulos
nulls = df.isna().sum().sort_values(ascending=False)
top_nulls = nulls.head(20)
print(top_nulls)

top_nulls.to_csv(TABLES / "eda_top_nulls_sample.csv")

# %%
# Histograma
p = FIGURES / "eda_hist_bikes_available_mean.png"
plt.figure()
df["bikes_available_mean"].clip(lower=0).plot(kind="hist", bins=60)
plt.title("Distribución bikes_available_mean (sample)")
plt.tight_layout()
plt.savefig(p, dpi=140)
plt.close()
print("✅", p)

# %%
# Scatter temp vs bikes (subsample)
p = FIGURES / "eda_scatter_temp_vs_bikes.png"
sub = df[["temperature_2m", "bikes_available_mean"]].dropna()
if len(sub) > 80000:
    sub = sub.sample(80000, random_state=42)

plt.figure()
plt.scatter(sub["temperature_2m"], sub["bikes_available_mean"], s=4)
plt.title("Temperatura vs bikes (subsample)")
plt.tight_layout()
plt.savefig(p, dpi=140)
plt.close()
print("✅", p)

# %%
# Media por hora
p = FIGURES / "eda_line_mean_bikes_by_hour.png"
by_hour = df.groupby("hour", as_index=False)["bikes_available_mean"].mean()

plt.figure()
plt.plot(by_hour["hour"], by_hour["bikes_available_mean"])
plt.title("Media bikes por hora")
plt.xticks(range(0, 24, 2))
plt.tight_layout()
plt.savefig(p, dpi=140)
plt.close()
print("✅", p)

by_hour.head()
