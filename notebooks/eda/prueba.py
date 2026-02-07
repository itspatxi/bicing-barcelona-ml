from pathlib import Path
import duckdb

def find_project_root(start: Path) -> Path:
    start = start.resolve()
    for p in [start, *start.parents]:
        if (p / "data").exists() and (p / ".git").exists():
            return p
        if (p / "data").exists() and (p / "pyproject.toml").exists():
            return p
        if (p / "data").exists() and (p / "requirements.txt").exists():
            return p
    # fallback: el primero que tenga /data
    for p in [start, *start.parents]:
        if (p / "data").exists():
            return p
    raise FileNotFoundError("No encuentro la raíz del proyecto (carpeta /data).")

ROOT = find_project_root(Path.cwd())
p = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"

print("CWD :", Path.cwd())
print("ROOT:", ROOT)
print("PARQ:", p)
print("EXISTS:", p.exists())

if not p.exists():
    raise FileNotFoundError(f"No existe: {p}")

con = duckdb.connect()
df_cov = con.execute("""
  SELECT station_id, COUNT(*) AS n_rows
  FROM read_parquet(?)
  GROUP BY 1
  ORDER BY n_rows
""", [str(p)]).df()
con.close()

print(df_cov.head(15))
print(df_cov.describe())
