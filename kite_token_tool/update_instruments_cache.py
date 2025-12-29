# update_instruments_cache.py
#
# Generic, portable version.
# Assumes a project structure like:
#   <ROOT>/
#       historical_data/
#       kite_client.py
#       kite_token_tool/
#           update_instruments_cache.py
#
# If a "historical_data" folder exists in any parent directory, it will be used.
# Otherwise, it will create "historical_data" in the folder one level above this file.

from pathlib import Path
import json
import time
import sys

# -------------------------------------------------------------
# RESOLVE PROJECT ROOT + HISTORICAL_DATA LOCATION
# -------------------------------------------------------------

this_file = Path(__file__).resolve()
default_root = this_file.parent.parent  # one level above kite_token_tool/
root = default_root

# Try to find an existing "historical_data" folder in parents
historical_data_path = None
for p in this_file.parents:
    candidate = p / "historical_data"
    if candidate.exists() and candidate.is_dir():
        historical_data_path = candidate
        root = p
        break

# If not found, create it under default_root
if historical_data_path is None:
    historical_data_path = default_root / "historical_data"
    historical_data_path.mkdir(parents=True, exist_ok=True)

# Make ROOT available to imports (e.g., kite_client.py in ROOT)
ROOT = root
sys.path.append(str(ROOT))

from kite_client import get_kite_client  # expects kite_client.py in ROOT

# Final cache file path:
CACHE_PATH = ROOT / "instruments_nse.json"

def fetch_and_save(cache_path: Path, retries: int = 3, backoff: float = 1.5) -> bool:
    kite = get_kite_client()
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            instruments = kite.instruments("NSE")

            slim = []
            for i in instruments:
                slim.append({
                    "tradingsymbol": i.get("tradingsymbol"),
                    "instrument_token": i.get("instrument_token"),
                    "exchange": i.get("exchange"),
                })

            tmp = cache_path.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(slim, f, ensure_ascii=False, indent=2)
            tmp.replace(cache_path)

            print(f"[OK] Wrote cache: {cache_path} ({len(slim)} instruments)")
            return True

        except Exception as e:
            last_exc = e
            wait = backoff * attempt
            print(f"[WARN] instruments() failed (attempt {attempt}/{retries}): {e}. retry in {wait:.1f}s")
            time.sleep(wait)

    print(f"[ERR] Failed to refresh instruments cache after {retries} attempts: {last_exc}")
    return False

if __name__ == "__main__":
    ok = fetch_and_save(CACHE_PATH)
    if not ok:
        sys.exit(1)
