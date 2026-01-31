"""
fetch_festivos_ckan.py
----------------------

Qué hace este script:
- Se conecta al portal de Open Data de Barcelona (CKAN).
- Busca el dataset de "calendari laboral / festivos".
- Descarga TODOS los recursos (files) asociados (normalmente .ics, .csv, etc.)
- Los guarda en: data/raw/festivos/

Por qué CKAN:
- El portal de Open Data BCN usa CKAN, que tiene una API estándar muy cómoda.
- Con package_show podemos obtener la lista de "resources" (archivos) del dataset.

Cómo se usa:
1) Activa tu venv
2) Ejecuta:
   python .\src\download\fetch_festivos_ckan.py
"""

import os
import re
from pathlib import Path

import requests
from dotenv import load_dotenv

# =========================
# 1) Configuración básica
# =========================

# Carga variables desde .env si existe
# (si no existe, no pasa nada: usamos valores por defecto)
load_dotenv()

# Base de la API CKAN del Open Data BCN
# Lo puedes sobreescribir en tu .env con BCN_CKAN_BASE=...
CKAN_BASE = os.getenv(
    "BCN_CKAN_BASE",
    "https://opendata-ajuntament.barcelona.cat/data/api/3/action"
)

# El "id" del dataset (slug). En CKAN suele ser el nombre que ves en la URL del dataset.
# Si alguna vez falla, lo normal es que haya cambiado el slug.
DATASET_ID = "calendari-festes-laborals"

# Carpeta donde guardaremos las descargas
OUT_DIR = Path("data/raw/festivos")


# =========================
# 2) Helpers (funciones)
# =========================

def ckan_action(action: str, **params) -> dict:
    """
    Llama a un endpoint de CKAN del tipo:
      GET {CKAN_BASE}/{action}?param1=...&param2=...

    Ejemplo:
      package_show?id=calendari-festes-laborals

    Devuelve:
      El campo "result" del JSON si success=True
    """
    url = f"{CKAN_BASE}/{action}"
    r = requests.get(url, params=params, timeout=60)  # timeout para que no se quede colgado
    r.raise_for_status()  # si hay 404/500/etc. lanza error

    payload = r.json()
    if not payload.get("success", False):
        # CKAN devuelve success=False cuando la acción falla (ej. dataset no existe)
        raise RuntimeError(f"CKAN action failed: {payload}")

    return payload["result"]


def safe_filename(name: str) -> str:
    """
    Limpia un texto para que sea un nombre de archivo válido en Windows.
    Cambia cualquier cosa rara por "_".
    """
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")


def guess_extension_from_url(url: str) -> str:
    """
    Intenta sacar extensión a partir de la URL.
    Si no hay, devuelve ".dat" como comodín.
    """
    # Quitamos querystring (lo que va después de ?)
    base = url.split("?")[0]
    ext = Path(base).suffix
    return ext if ext else ".dat"


def download_file(url: str, dest: Path) -> None:
    """
    Descarga un archivo (bytes) desde url y lo guarda en dest.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    # stream=True para descargas grandes (aunque festivos suele ser pequeño)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


# =========================
# 3) Lógica principal
# =========================

def main():
    """
    1) Busca el dataset en CKAN (package_show)
    2) Recorre todos los resources (archivos)
    3) Descarga cada uno a data/raw/festivos/
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Buscando dataset CKAN: {DATASET_ID}")
    pkg = ckan_action("package_show", id=DATASET_ID)

    resources = pkg.get("resources", [])
    if not resources:
        raise RuntimeError(
            "He encontrado el dataset, pero no tiene resources (archivos) asociados."
        )

    print(f"Resources encontrados: {len(resources)}")
    downloaded = 0
    skipped = 0

    for res in resources:
        res_url = res.get("url")
        if not res_url:
            continue

        # Nombre “humano” del recurso si lo tiene
        name = res.get("name") or res.get("id") or "resource"

        # Aseguramos nombre limpio y extensión
        filename = safe_filename(name)
        ext = guess_extension_from_url(res_url)

        # Evita doble extensión si name ya la trae
        if not filename.lower().endswith(ext.lower()):
            filename = f"{filename}{ext}"

        dest = OUT_DIR / filename

        # Si ya existe, no lo volvemos a bajar
        if dest.exists():
            skipped += 1
            continue

        print(f"Descargando: {filename}")
        download_file(res_url, dest)
        downloaded += 1

    print("\n===========================")
    print("✅ Descarga de festivos OK")
    print(f"Guardado en: {OUT_DIR.resolve()}")
    print(f"Descargados: {downloaded} | Omitidos (ya existían): {skipped}")
    print("===========================\n")


if __name__ == "__main__":
    main()
