from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


def find_project_root(start: Path) -> Path:
    """Sube hasta encontrar carpeta /data o /.git."""
    p = start.resolve()
    for _ in range(10):
        if (p / "data").exists() or (p / ".git").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start.resolve()


def unfold_ics_lines(text: str) -> list[str]:
    """
    iCalendar permite "folding": una línea que continúa empieza por espacio o tab.
    Aquí unimos esas continuaciones a la línea anterior.
    """
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    for ln in lines:
        if not ln:
            continue
        if ln.startswith((" ", "\t")) and out:
            out[-1] += ln[1:]
        else:
            out.append(ln)
    return out


def parse_dtstart_to_date(dtstart_line: str):
    """
    Ejemplos:
      DTSTART;VALUE=DATE:2024-01-01
      DTSTART;VALUE=DATE:20240101
      DTSTART:20240101T000000Z
    Nos quedamos con YYYY-MM-DD.
    """
    # quedarnos con lo que va tras ':'
    if ":" not in dtstart_line:
        return None
    val = dtstart_line.split(":", 1)[1].strip()

    # si viene con guiones
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", val)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # si viene como 8 dígitos al principio
    m = re.match(r"^(\d{4})(\d{2})(\d{2})", val)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    return None


def extract_events_from_ics(text: str) -> list[dict]:
    lines = unfold_ics_lines(text)
    events: list[dict] = []

    in_event = False
    cur = {"date": None, "name": None}

    for ln in lines:
        if ln == "BEGIN:VEVENT":
            in_event = True
            cur = {"date": None, "name": None}
            continue
        if ln == "END:VEVENT":
            in_event = False
            if cur["date"]:
                events.append(cur.copy())
            continue
        if not in_event:
            continue

        if ln.startswith("DTSTART"):
            cur["date"] = parse_dtstart_to_date(ln)
        elif ln.startswith("SUMMARY"):
            # SUMMARY:Nombre
            if ":" in ln:
                cur["name"] = ln.split(":", 1)[1].strip()

    return events


def main():
    ROOT = find_project_root(Path.cwd())
    fest_dir = ROOT / "data" / "raw" / "festivos"
    if not fest_dir.exists():
        raise FileNotFoundError(f"No existe carpeta: {fest_dir}")

    # Pillamos .ics, .ics.dat, etc.
    files = sorted([p for p in fest_dir.glob("**/*") if p.is_file() and "ics" in p.name.lower()])
    if not files:
        raise FileNotFoundError(f"No encuentro ficheros con 'ics' en el nombre dentro de: {fest_dir}")

    all_rows = []
    for fp in files:
        raw = fp.read_bytes()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1", errors="ignore")

        evs = extract_events_from_ics(text)
        for e in evs:
            all_rows.append(
                {
                    "date": e["date"],
                    "name": e["name"] or "",
                    "scope": "catalunya",
                    "source_file": fp.name,
                }
            )

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["date", "name"]).drop_duplicates(subset=["date", "name", "scope"])

    out_ca = fest_dir / "festivos_ca.csv"
    df[["date", "name", "scope"]].to_csv(out_ca, index=False)

    # Si quieres también este alias para tu pipeline actual:
    out_bcn = fest_dir / "festivos_bcn.csv"
    df[["date", "name", "scope"]].to_csv(out_bcn, index=False)

    print(f"✅ ICS leídos: {len(files)}")
    print("✅ CSV creado:", out_ca)
    print("✅ CSV alias creado:", out_bcn)
    print("rows=", len(df))
    print("min=", df["date"].min(), "max=", df["date"].max())
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
