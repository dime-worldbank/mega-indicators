"""Refresh the committed WEO sample payload used by test_imf_sdmx.py.

Run this manually whenever you want the test fixture to reflect the current
IMF response shape (e.g. after a WEO release, or to verify the API still
returns the structure our parser expects). The git diff on
weo_sample_payload.json then shows exactly what changed in the API.

    cd <repo root>
    python budget/tests/fixtures/refresh_weo_fixture.py
"""
import json
from pathlib import Path

import requests

URL = 'https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/WEO/9.0.0/USA+TGO.GGX.A'
PARAMS = {
    'format': 'jsondata',
    'attributes': 'all',
    'detail': 'full',
    'limit': 1000,
}

resp = requests.get(URL, params=PARAMS, timeout=30)
resp.raise_for_status()
payload = resp.json()

out = Path(__file__).parent / 'weo_sample_payload.json'
out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))

dg_entries = payload['data']['dataSets'][0].get('dimensionGroupAttributes', {})
series = payload['data']['dataSets'][0].get('series', {})
print(f"Wrote {out}")
print(f"  series={len(series)}, dimensionGroupAttributes entries={len(dg_entries)}")
