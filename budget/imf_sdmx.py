# Databricks notebook source
# Pure helpers for parsing IMF SDMX-JSON responses.
#
# This file is BOTH:
#   - A Databricks notebook, %run'd from government_budget.py so the functions
#     land in the notebook's namespace (sys.path doesn't reliably include
#     sibling directories in Git Folders, hence the %run pattern).
#   - A regular importable Python module, used by the unit tests in
#     budget/tests/test_imf_sdmx.py via `from imf_sdmx import ...`.
#
# The `# Databricks notebook source` header above is a comment in plain Python
# but tells Databricks to treat this as a notebook.

import re
import warnings


def _parse_payload(payload):
    """Generic SDMX-JSON parser. Yields one dict per (country, indicator, year) observation."""
    datasets = payload.get('data', {}).get('dataSets') or []
    if not datasets or not datasets[0].get('series'):
        return

    struct = payload['data']['structures'][0]
    dims = struct['dimensions']['series']
    pos = {d['id']: i for i, d in enumerate(dims)}
    countries = [v['id'] for v in dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in dims[pos['INDICATOR']]['values']]
    years = [int(v['value']) for v in struct['dimensions']['observation'][0]['values']]

    for series_key, series in datasets[0]['series'].items():
        idx = [int(i) for i in series_key.split(':')]
        country = countries[idx[pos['COUNTRY']]]
        indicator = indicators[idx[pos['INDICATOR']]]
        for time_idx, obs in series['observations'].items():
            yield {
                'country_code': country,
                'year': years[int(time_idx)],
                'indicator': indicator,
                'value': float(obs[0]) if obs[0] is not None else None,
            }


def _laad_to_year(raw):
    """LATEST_ACTUAL_ANNUAL_DATA -> integer calendar year, or None if unrecognized.

    LAAD is either a plain year ("2025") or a fiscal-year span ("FY2024/25"),
    which maps to its later year (2025) per the majority WEO FY->CY convention.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw) if raw.is_integer() else None
    m = re.match(r'\s*(?:FY)?\s*(\d{4})(/\d{2,4})?\s*$', str(raw), re.IGNORECASE)
    if not m:
        return None
    year = int(m.group(1))
    return year + 1 if m.group(2) else year  # fiscal-year span -> later year


def _weo_annotate_forecast(records, payload):
    """Set `is_forecast` on each record using WEO's LATEST_ACTUAL_ANNUAL_DATA attribute.

    LAAD is the most recent year of *actual* data for a given (country, indicator); any
    strictly-later year is a projection. WEO exposes it as a dimensionGroup attribute
    (relationship: COUNTRY + INDICATOR), with values under
    `dataSets[0].dimensionGroupAttributes`, keyed by positional strings like '0:0::'
    (COUNTRY_idx : INDICATOR_idx : FREQUENCY : TIME_PERIOD; blank positions mean
    "applies to all values in that dimension"). Values come wrapped in lists, e.g.
    `["2025"]` or a fiscal-year span `["FY2024/25"]` (see `_laad_to_year`).
    """
    struct = payload['data']['structures'][0]
    dims = struct['dimensions']['series']
    pos = {d['id']: i for i, d in enumerate(dims)}
    countries = [v['id'] for v in dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in dims[pos['INDICATOR']]['values']]

    attrs_meta = struct.get('attributes', {}).get('dimensionGroup', [])
    laad_idx = next(
        (i for i, a in enumerate(attrs_meta) if a.get('id') == 'LATEST_ACTUAL_ANNUAL_DATA'),
        None,
    )
    if laad_idx is None:
        available = [a.get('id') for a in attrs_meta]
        raise RuntimeError(
            f"WEO payload missing dimensionGroup attribute 'LATEST_ACTUAL_ANNUAL_DATA' for "
            f"chunk with countries {countries}. Available dimensionGroup attributes: {available}. "
            f"WEO schema may have changed, or the request is missing params — check that "
            f"`attributes=all`, `detail=full`, AND an explicit `limit` are all set (without "
            f"`limit`, the dimensionGroup attribute values are stripped from the response)."
        )

    country_pos = pos['COUNTRY']
    indicator_pos = pos['INDICATOR']
    last_actual = {}
    unrecognized = []
    for key_str, values in payload['data']['dataSets'][0].get('dimensionGroupAttributes', {}).items():
        parts = key_str.split(':')
        # COUNTRY and INDICATOR index strings from the positional key — e.g.
        # for "1:0::" with COUNTRY at pos 0, INDICATOR at pos 1: c_part="1", i_part="0".
        c_part = parts[country_pos] if country_pos < len(parts) else ''
        i_part = parts[indicator_pos] if indicator_pos < len(parts) else ''
        if not c_part or not i_part:
            # blank in either position means the value isn't (country, indicator)-specific
            continue
        raw = values[laad_idx] if laad_idx < len(values) else None
        if isinstance(raw, list) and raw:
            raw = raw[0]
        if raw is None:
            continue
        year = _laad_to_year(raw)
        if year is not None:
            last_actual[(countries[int(c_part)], indicators[int(i_part)])] = year
        else:
            # Unrecognized format — surface it instead of silently leaving the
            # whole series is_forecast=False (likely a WEO format change).
            unrecognized.append((countries[int(c_part)], indicators[int(i_part)], raw))

    if unrecognized:
        max_show = 10
        shown = unrecognized[:max_show]
        details = ', '.join(f'{c}/{ind}={raw!r}' for c, ind, raw in shown)
        if len(unrecognized) > max_show:
            details = f"{details}, ... (+{len(unrecognized) - max_show} more)"
        warnings.warn(
            f"WEO LATEST_ACTUAL_ANNUAL_DATA in an unrecognized format for "
            f"{len(unrecognized)} (country, indicator) pair(s); their records are "
            f"left is_forecast=False. Extend _laad_to_year to handle: {details}",
            stacklevel=2,
        )

    for r in records:
        laad = last_actual.get((r['country_code'], r['indicator']))
        r['is_forecast'] = bool(laad is not None and r['year'] > laad)
