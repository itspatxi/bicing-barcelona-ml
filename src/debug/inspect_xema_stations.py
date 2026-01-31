from io import StringIO
import requests
import pandas as pd

URL = "https://analisi.transparenciacatalunya.cat/resource/yqwd-vj5e.csv"

def main():
    r = requests.get(URL, params={"$limit": 50000}, timeout=180)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))

    print("COLUMNAS:")
    print(list(df.columns))

    muni_cols = [c for c in df.columns if "municip" in c.lower()]
    comarca_cols = [c for c in df.columns if "comarc" in c.lower()]

    print("\nCOL_MUNI:", muni_cols)
    print("COL_COMARCA:", comarca_cols)

    # Si hay columna municipio, enseñamos valores que contengan "barcel"
    if muni_cols:
        mcol = muni_cols[0]
        print(f"\nEjemplos {mcol} que contienen 'barcel':")
        print(
            df[df[mcol].astype(str).str.lower().str.contains("barcel", na=False)][[mcol]]
            .drop_duplicates()
            .head(50)
            .to_string(index=False)
        )
    else:
        print("\nNO HAY COLUMNA DE MUNICIPIO")

    # Si hay columna comarca, enseñamos valores que contengan "barcel"
    if comarca_cols:
        ccol = comarca_cols[0]
        print(f"\nEjemplos {ccol} que contienen 'barcel':")
        print(
            df[df[ccol].astype(str).str.lower().str.contains("barcel", na=False)][[ccol]]
            .drop_duplicates()
            .head(50)
            .to_string(index=False)
        )
    else:
        print("\nNO HAY COLUMNA DE COMARCA")

if __name__ == "__main__":
    main()
