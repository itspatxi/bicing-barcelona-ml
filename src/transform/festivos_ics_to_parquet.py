"""
festivos_ics_to_parquet.py
--------------------------

Qué hace:
- Lee ficheros .ics (calendario) descargados del Open Data BCN
- Extrae eventos (festivos)
- Genera una tabla plana:
    date (YYYY-MM-DD), name, lang, source_file
- Guarda en:
    data/bronze/festivos/festivos.parquet
    data/bronze/festivos/festivos.csv

Notas:
- En un .ics, los festivos suelen venir como eventos "all-day".
- Si un evento tiene rango (DTSTART/DTEND), lo convertimos a una fecha de inicio.
"""

from pathlib import Path
import pandas as pd
from icalendar import Calendar

RAW_DIR = Path("data/raw/festivos")
OUT_DIR = Path("data/bronze/festivos")


def detect_lang_from_filename(name: str) -> str:
    """
    Saca el idioma del nombre si viene como ..._es..., ..._ca..., etc.
    Si no lo detecta, devuelve 'unknown'.
    """
    lower = name.lower()
    for code in ["ca", "es", "en", "fr"]:
        if f"_{code}" in lower:
            return code
    return "unknown"


def read_ics_file(path: Path) -> list[dict]:
    """
    Parsea un archivo ICS y devuelve una lista de dicts con info básica de eventos.
    """
    data = path.read_bytes()
    cal = Calendar.from_ical(data)

    lang = detect_lang_from_filename(path.name)
    rows = []

    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        summary = component.get("SUMMARY")
        dtstart = component.get("DTSTART")
        if dtstart is None:
            continue

        # dtstart puede ser date o datetime
        dt_value = dtstart.dt
        if hasattr(dt_value, "date"):  # datetime -> date
            date_value = dt_value.date()
        else:  # ya es date
            date_value = dt_value

        rows.append(
            {
                "date": pd.to_datetime(date_value).date(),
                "name": str(summary) if summary is not None else "",
                "lang": lang,
                "source_file": path.name,
            }
        )

    return rows


def main():
    if not RAW_DIR.exists():
        raise RuntimeError(f"No existe {RAW_DIR}. Primero descarga los festivos.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Admitimos .ics, .ics.dat, etc.
    files = sorted([p for p in RAW_DIR.iterdir() if p.is_file() and ".ics" in p.name.lower()])
    if not files:
        raise RuntimeError(f"No he encontrado .ics en {RAW_DIR}")

    all_rows = []
    for f in files:
        all_rows.extend(read_ics_file(f))

    df = pd.DataFrame(all_rows)

    # Limpieza: quitamos duplicados exactos
    df = df.drop_duplicates(subset=["date", "name", "lang"]).sort_values(["date", "lang", "name"])

    # Guardamos en Bronze
    df.to_parquet(OUT_DIR / "festivos.parquet", index=False)
    df.to_csv(OUT_DIR / "festivos.csv", index=False, encoding="utf-8")

    print("✅ OK festivos ->", OUT_DIR.resolve())
    print(df.head(10).to_string(index=False))
    print(f"Total filas: {len(df):,}")


if __name__ == "__main__":
    main()
