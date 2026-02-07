from pathlib import Path
import pandas as pd

def find_root():
    root = Path.cwd().resolve()
    for _ in range(6):
        if (root / "data").exists():
            return root
        root = root.parent
    raise FileNotFoundError("No encuentro la carpeta /data subiendo desde el CWD")

def main():
    ROOT = find_root()
    inp = ROOT / "reports" / "tables" / "station_coverage_full.csv"
    out = ROOT / "reports" / "tables" / "station_coverage_full_tagged.csv"

    if not inp.exists():
        raise FileNotFoundError(f"No existe: {inp}")

    df = pd.read_csv(inp)
    # Aseguramos tipos
    for c in ["n_rows", "n_distinct_hours", "coverage_ratio", "first_year", "last_year"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Etiquetas (ajusta umbrales si quieres)
    def tag_row(r):
        n = r.get("n_rows", 0)
        cy = r.get("coverage_ratio", 0.0)
        fy = r.get("first_year", None)

        # ruido extremo
        if pd.notna(n) and n < 1000:
            return "noise_very_sparse"

        # nuevas (aparecen tarde)
        if pd.notna(fy) and fy >= 2024 and pd.notna(n) and n < 20000:
            return "new_station"

        # intermitentes / poco útiles para ML
        if (pd.notna(n) and n < 20000) or (pd.notna(cy) and cy < 0.90):
            return "sparse_or_gappy"

        return "full_coverage"

    df["coverage_tag"] = df.apply(tag_row, axis=1)

    # Resumen
    print("\n== Resumen por etiqueta ==")
    print(df["coverage_tag"].value_counts())

    print("\n== Ejemplos (bottom 10 n_rows) ==")
    print(df.sort_values("n_rows").head(10)[["station_id", "n_rows", "coverage_ratio", "first_year", "last_year", "coverage_tag"]])

    print("\n== Ejemplos (top 10 n_rows) ==")
    print(df.sort_values("n_rows", ascending=False).head(10)[["station_id", "n_rows", "coverage_ratio", "first_year", "last_year", "coverage_tag"]])

    df.to_csv(out, index=False)
    print(f"\n✅ Guardado: {out}")

if __name__ == "__main__":
    main()
