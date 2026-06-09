"""Tests for budget/imf_sdmx.py — the SDMX parsing + WEO forecast annotation.

The fixture `fixtures/weo_sample_payload.json` is a hand-crafted minimal payload
shaped like a real WEO response, with two countries having different LAAD years
so the per-country forecast lookup is exercised:

    USA → LAAD = 2025 → 2024/2025 actual, 2026 forecast
    TGO → LAAD = 2023 → all three years (2024/2025/2026) forecast

To replace it with a real captured API response (recommended before each
release of WEO 9.x), run:

    python budget/tests/fixtures/refresh_weo_fixture.py

The tests don't depend on exact numeric values; they check structure and
forecast logic, so either fixture variant works.
"""
import copy
import json
from pathlib import Path

import pytest

from imf_sdmx import _parse_payload, _weo_annotate_forecast


FIXTURE_PATH = Path(__file__).parent / 'fixtures' / 'weo_sample_payload.json'


@pytest.fixture
def weo_payload():
    return json.loads(FIXTURE_PATH.read_text())


def test_parse_payload_yields_one_record_per_observation(weo_payload):
    records = list(_parse_payload(weo_payload))
    # 2 countries × 1 indicator × 3 years
    assert len(records) == 6
    assert {r['country_code'] for r in records} == {'USA', 'TGO'}
    assert {r['indicator'] for r in records} == {'GGX'}
    assert {r['year'] for r in records} == {2024, 2025, 2026}


def test_parse_payload_preserves_observation_values(weo_payload):
    records = list(_parse_payload(weo_payload))
    usa_2024 = next(r for r in records if r['country_code'] == 'USA' and r['year'] == 2024)
    assert usa_2024['value'] == 10_000_000_000_000.0


def test_parse_payload_handles_empty_dataset():
    payload = {'data': {'dataSets': [], 'structures': [{}]}}
    assert list(_parse_payload(payload)) == []


def test_weo_annotate_forecast_uses_per_country_laad(weo_payload):
    """USA LAAD=2025 → only 2026 is forecast.
       TGO LAAD=2023 → all three returned years (2024/2025/2026) are forecast.
    """
    records = list(_parse_payload(weo_payload))
    _weo_annotate_forecast(records, weo_payload)

    by_key = {(r['country_code'], r['year']): r['is_forecast'] for r in records}
    assert by_key[('USA', 2024)] is False
    assert by_key[('USA', 2025)] is False  # LAAD year itself is still actual
    assert by_key[('USA', 2026)] is True
    assert by_key[('TGO', 2024)] is True
    assert by_key[('TGO', 2025)] is True
    assert by_key[('TGO', 2026)] is True


def test_weo_annotate_forecast_raises_when_laad_missing(weo_payload):
    """If the schema drops LATEST_ACTUAL_ANNUAL_DATA, fail loud — silently
    defaulting every record to is_forecast=False would silently corrupt the
    pipeline."""
    payload = copy.deepcopy(weo_payload)
    payload['data']['structures'][0]['attributes']['dimensionGroup'] = [
        a for a in payload['data']['structures'][0]['attributes']['dimensionGroup']
        if a['id'] != 'LATEST_ACTUAL_ANNUAL_DATA'
    ]
    with pytest.raises(RuntimeError, match='LATEST_ACTUAL_ANNUAL_DATA'):
        _weo_annotate_forecast([], payload)


def test_weo_annotate_forecast_skips_blank_positional_keys(weo_payload):
    """The ':0::' entry (only INDICATOR populated, COUNTRY blank) is not a
    (country, indicator)-specific value and must not contribute a LAAD lookup
    that's then applied to all USA/TGO records."""
    # Add a misleading non-int value at the LAAD position of the blank-country
    # group; if it leaked into `last_actual`, int(...) would crash and we'd
    # never get to assertions. Test that nothing crashes and forecasts are
    # still set correctly per country.
    payload = copy.deepcopy(weo_payload)
    payload['data']['dataSets'][0]['dimensionGroupAttributes'][':0::'] = [
        None, ["NOT_A_YEAR"], None
    ]
    records = list(_parse_payload(payload))
    _weo_annotate_forecast(records, payload)
    by_key = {(r['country_code'], r['year']): r['is_forecast'] for r in records}
    assert by_key[('USA', 2026)] is True
    assert by_key[('TGO', 2024)] is True
