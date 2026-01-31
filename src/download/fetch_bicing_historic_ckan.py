import os
from pathlib import Path
import re

import requests
from dotenv import load_dotenv
from tqdm import tqdm
import py7zr

load_dotenv()

CKAN_BASE = os.getenv("BCN_CKAN_BASE", "https://opendata-ajuntament.barcelona.cat/data/api/3/action")
DATASET_ID = "estat-estacions-bicing"  # slug típico del dataset


def ckan_action(action: str, **params):
    url = f"{CKAN_BASE}/{action}"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success", False):
        raise RuntimeError(payload)
    return payload["result"]


def download_file(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))


def extract_7z(archive_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        z.extractall(path=out_dir)


def main(out_dir="data/raw/bicing_historic", extract=True):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    pkg = ckan_action("package_show", id=DATASET_ID)

    # resources suelen incluir meses en .7z + algunos ficheros sueltos
    resources = pkg.get("resources", [])
    if not resources:
        raise RuntimeError("No se han encontrado resources en el dataset. Revisa el DATASET_ID.")

    for res in resources:
        res_url = res.get("url")
        name = res.get("name") or res.get("id")
        if not res_url:
            continue

        # limpia nombre de archivo
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
        # intenta conservar extensión del url
        ext = Path(res_url.split("?")[0]).suffix
        filename = safe if safe.endswith(ext) else f"{safe}{ext}"

        dest = out / filename
        if dest.exists():
            continue

        download_file(res_url, dest)

        if extract and dest.suffix.lower() == ".7z":
            extract_7z(dest, out / "extracted")

    print(f"OK -> {out}")


if __name__ == "__main__":
    main()
