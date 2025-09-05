import requests
import json
import os
from pathlib import Path

URL = "https://hn.algolia.com/api/v1/search_by_date"

def extract_latest():
    params = {
        "tags": "(story,comment)",
        "hitsPerPage": 100,
        "page": 0
    }

    r = requests.get(URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    out_dir = Path("/opt/airflow/data/raw")  
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "hn_latest.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Saved into {out_file}")

if __name__ == "__main__":
    extract_latest()
