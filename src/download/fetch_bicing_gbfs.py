import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# 1) Oficial (ahora mismo devuelve 503)
BSMSA_STATION_INFO = "https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_information"
BSMSA_STATION_STATUS = "https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_status"

# 2) Fallback (CityBikes) - GBFS v2.3
CITYBIKES_STATION_INFO = "https://api.citybik.es/gbfs/2/bicing/station_information.json"
CITYBIKES_STATION_STATUS = "https://api.citybik.es/gbfs/2/bicing/station_status.json"


RETRY_STATUS = {429, 500, 502, 503, 504}


def fetch_json(url: str, timeout: int = 60, max_retries: int = 5) -> dict:
    headers = {
        "User-Agent": "bicing-barcelona-ml/0.1 (+github; educational)",
        "Accept": "application/json",
    }

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code in RETRY_STATUS:
                # backoff exponencial suave
                sleep_s = min(2 ** attempt, 20)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            sleep_s = min(2 ** attempt, 20)
            time.sleep(sleep_s)

    raise RuntimeError(f"No se ha podido descargar {url}. Último error: {last_exc}")


def main(out_dir: str = "data/raw/bicing_gbfs"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    sources = [
        ("bsmsa", BSMSA_STATION_INFO, BSMSA_STATION_STATUS),
        ("citybikes", CITYBIKES_STATION_INFO, CITYBIKES_STATION_STATUS),
    ]

    info = status = None
    used = None
    errors = {}

    for name, info_url, status_url in sources:
        try:
            info = fetch_json(info_url)
            status = fetch_json(status_url)
            used = name
            break
        except Exception as e:
            errors[name] = str(e)

    if used is None:
        raise RuntimeError(f"No ha funcionado ninguna fuente. Errores: {errors}")

    (out / f"station_information_{used}_{ts}.json").write_text(
        json.dumps(info, ensure_ascii=False)
    )
    (out / f"station_status_{used}_{ts}.json").write_text(
        json.dumps(status, ensure_ascii=False)
    )

    meta = {
        "timestamp_utc": ts,
        "source_used": used,
        "errors": errors,
    }
    (out / f"_meta_{used}_{ts}.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))

    print(f"OK -> {out} | fuente={used} | ts={ts}")


if __name__ == "__main__":
    main()
