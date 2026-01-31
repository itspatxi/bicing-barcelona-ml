"""
fetch_meteo_xema_sample.py
--------------------------

Qué hace:
- Descarga un "sample" pequeño del dataset XEMA (Socrata / Transparència Catalunya)
- Lo guarda en data/raw/meteo_xema/sample.csv para ver columnas reales

Por qué:
- XEMA tiene millones de registros; antes de filtrar bien (estaciones/variables),
  conviene validar nombres de columnas y formato de fechas.

Cómo se usa:
- python .\src\download\fetch_meteo_xema_sample.py
"""

from pathlib import Path
import requests

OUT = Path("data/raw/meteo_xema")
OUT.mkdir(parents=True, exist_ok=True)

# Endpoint Socrata (CSV). Este dataset se usa mucho como ejemplo para XEMA.
# Si en tu caso quisieras otro dataset XEMA, lo cambiamos después.
URL = "https://analisi.transparenciacatalunya.cat/resource/nzvn-apee.csv"

def main(limit: int = 5000):
    params = {
        "$limit": limit
    }
    print("Descargando sample XEMA...")
    r = requests.get(URL, params=params, timeout=120)
    r.raise_for_status()

    out_path = OUT / "sample.csv"
    out_path.write_bytes(r.content)

    print(f"✅ OK -> {out_path.resolve()} ({len(r.content):,} bytes)")

if __name__ == "__main__":
    main()
