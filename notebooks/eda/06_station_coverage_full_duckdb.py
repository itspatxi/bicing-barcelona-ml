from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt


def find_project_root() -> Path:
    """Encuentra la raíz del proyecto subiendo desde CWD hasta hallar /data o /.git."""
    cwd = Path.cwd().resolve()
    for p in [cwd, *cwd.parents]:
        if (p / "data").exists() or (p / ".git").exists():
            return p
    return cwd


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    ROOT = find_project_root()

    PARQ = ROOT / "data" / "gold" / "bicing_gold_final_plus.parquet"
    if not PARQ.exists():
        raise FileNotFoundError(f"No existe el parquet esperado: {PARQ}")

    out_csv = ROOT / "reports" / "tables" / "station_coverage_full.csv"
    out_hist = ROOT / "reports" / "figures" / "eda_station_coverage_hist.png"
    out_ecdf = ROOT / "reports" / "figures" / "eda_station_coverage_ecdf.png"
    out_first_seen = ROOT / "reports" / "figures" / "eda_station_first_seen_year.png"

    ensure_parent(out_csv)
    ensure_parent(out_hist)
    ensure_parent(out_ecdf)
    ensure_parent(out_first_seen)

    print("CWD :", Path.cwd().resolve())
    print("ROOT:", ROOT)
    print("PARQ:", PARQ)
    print("EXISTS:", PARQ.exists())

    con = duckdb.connect()

    # Coverage por estación: nº filas, nº horas distintas, rango temporal, año primera/última aparición.
    q_cov = f"""
    WITH base AS (
        SELECT
            station_id,
            time_hour,
            CAST(time_hour AS DATE) AS d
        FROM read_parquet('{PARQ.as_posix()}')
    )
    SELECT
        station_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT time_hour) AS n_distinct_hours,
        MIN(time_hour) AS min_time,
        MAX(time_hour) AS max_time,
        MIN(EXTRACT(year FROM time_hour))::INT AS first_year,
        MAX(EXTRACT(year FROM time_hour))::INT AS last_year,
        COUNT(DISTINCT d) AS n_days
    FROM base
    GROUP BY 1
    ORDER BY n_rows DESC
    """
    df = con.execute(q_cov).df()

    # Coverage ratio aproximado: horas distintas / horas esperadas entre min y max (inclusive)
    # (Esto lo hacemos en pandas por claridad y evitar pelear con timestamps en SQL)
    df["min_time"] = pd.to_datetime(df["min_time"])
    df["max_time"] = pd.to_datetime(df["max_time"])
    # horas esperadas inclusivas
    expected = ((df["max_time"] - df["min_time"]).dt.total_seconds() / 3600.0).round().astype("int64") + 1
    df["expected_hours"] = expected.clip(lower=1)
    df["coverage_ratio"] = (df["n_distinct_hours"] / df["expected_hours"]).astype("float64")

    # Guardar CSV
    df.to_csv(out_csv, index=False)
    print(f"\n✅ CSV coverage: {out_csv} | rows={len(df)}")

    # Resumen console: umbrales
    thresholds = [100, 1000, 10000, 20000]
    print("\n== Conteo de estaciones por umbral de filas ==")
    for t in thresholds:
        n = int((df["n_rows"] < t).sum())
        print(f"stations with n_rows < {t:>6}: {n}")

    # Top/bottom
    print("\n== TOP 10 estaciones por n_rows ==")
    print(df.sort_values("n_rows", ascending=False).head(10)[
        ["station_id", "n_rows", "n_distinct_hours", "coverage_ratio", "min_time", "max_time", "first_year", "last_year"]
    ].to_string(index=False))

    print("\n== BOTTOM 15 estaciones por n_rows ==")
    print(df.sort_values("n_rows", ascending=True).head(15)[
        ["station_id", "n_rows", "n_distinct_hours", "coverage_ratio", "min_time", "max_time", "first_year", "last_year"]
    ].to_string(index=False))

    # Histograma n_rows (cap para que se vea la distribución)
    # Cap al p99 para que no se “aplane” visualmente
    cap = float(df["n_rows"].quantile(0.99))
    x = df["n_rows"].clip(upper=cap)

    plt.figure()
    plt.hist(x, bins=50)
    plt.title("Distribución de n_rows por estación (cap p99)")
    plt.xlabel("n_rows por station_id")
    plt.ylabel("count stations")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_hist, dpi=160)
    plt.close()
    print(f"✅ Figura: {out_hist}")

    # ECDF de n_rows (para ver percentiles rápido)
    df_sorted = df.sort_values("n_rows")
    y = (pd.Series(range(1, len(df_sorted) + 1)) / len(df_sorted)).to_numpy()
    plt.figure()
    plt.plot(df_sorted["n_rows"].to_numpy(), y)
    plt.title("ECDF de n_rows por estación")
    plt.xlabel("n_rows por station_id")
    plt.ylabel("Proporción acumulada")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_ecdf, dpi=160)
    plt.close()
    print(f"✅ Figura: {out_ecdf}")

    # First seen year (bar)
    year_counts = df["first_year"].value_counts().sort_index()
    plt.figure()
    plt.bar(year_counts.index.astype(str), year_counts.values)
    plt.title("Año de primera aparición (first_year) por estación")
    plt.xlabel("first_year")
    plt.ylabel("count stations")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_first_seen, dpi=160)
    plt.close()
    print(f"✅ Figura: {out_first_seen}")

    con.close()

    # Recomendación simple basada en resultados (para siguiente paso)
    low_sparse = int((df["n_rows"] < 1000).sum())
    print("\n== Recomendación práctica ==")
    print(f"- Estaciones muy 'sparse' (n_rows < 1000): {low_sparse}")
    print("- Para dashboards: filtrar n_rows >= 10k")
    print("- Para ML series: filtrar n_rows >= 20k y coverage_ratio alto (p.ej. >= 0.9)")


if __name__ == "__main__":
    main()
