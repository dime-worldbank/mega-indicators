"""The table store under DATA_ROOT, the shared fetch helpers, and the scripts run end to
end offline (requests / wbgapi calls are monkeypatched)."""
import io
import json
import runpy
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest
import requests
import wbgapi

REPO = Path(__file__).resolve().parent.parent
MODULES = ["config", "utils", "population.subnational_population_extraction_from_census_gov", "public_finance.imf_sdmx"]


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """A fresh DATA_ROOT and no country filter (tests that want one set COUNTRY_NAME)."""
    root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(root))
    monkeypatch.setenv("COUNTRY_NAME", "")
    return root


def fresh_utils():
    """utils (and config) imported against the current environment."""
    for name in MODULES:
        sys.modules.pop(name, None)
    import utils
    return vars(utils)


def run_script(relpath):
    """As run.py does: the script with fresh module state."""
    for name in MODULES:
        sys.modules.pop(name, None)
    return runpy.run_path(str(REPO / relpath), run_name="__main__")


class FakeResponse:
    def __init__(self, content, status_code=200):
        self.content = content
        self.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return json.loads(self.text)


def write_country(ns):
    ns["write_table"](pd.DataFrame({
        "country_name": ["Togo", "Albania", "World"],
        "country_code": ["TGO", "ALB", "WLD"],
        "region": ["SSF", "ECS", None],
        "income_level": ["LMC", "UMC", None],
        "is_aggregate": [False, False, True],
    }), "country")


def fake_wb_dataframe(series, skipBlanks=True):
    return pd.DataFrame({"YR2020": [10.0, 20.0], "YR2021": [11.0, None]},
                        index=pd.Index(["TGO", "ALB"], name="economy"))


# --- config.py ---------------------------------------------------------------

def test_defaults(monkeypatch):
    monkeypatch.delenv("DATA_ROOT", raising=False)
    monkeypatch.delenv("COUNTRY_NAME", raising=False)
    ns = fresh_utils()
    assert ns["DATA_ROOT"] == str(REPO / "data")
    assert ns["COUNTRY_NAME"] == "Togo"


def test_env_overrides(data_root):
    ns = fresh_utils()
    assert ns["DATA_ROOT"] == str(data_root)
    assert ns["VOLUME_ROOT_PATH"] == str(data_root) + "/raw_data"
    assert ns["COUNTRY_NAME"] == ""


# --- table IO ----------------------------------------------------------------

def test_write_then_read_table_roundtrip(data_root):
    ns = fresh_utils()
    df = pd.DataFrame({"country_name": ["Togo", "Congo, Dem. Rep."], "year": [2020, 2021], "value": [1.5, None]})
    assert not ns["table_exists"]("t")
    ns["write_table"](df, "t")
    assert (data_root / "prd_mega" / "indicator" / "t.csv").exists()
    assert ns["table_exists"]("t")
    pd.testing.assert_frame_equal(ns["read_table"]("t"), df)
    assert list(ns["read_table"]("t", columns=["year"]).columns) == ["year"]


def test_qualified_table_name_maps_to_catalog_schema_dirs(data_root):
    ns = fresh_utils()
    ns["write_table"](pd.DataFrame({"a": [1]}), "prd_corpdata.dm_reference_gold.fx")
    assert (data_root / "prd_corpdata" / "dm_reference_gold" / "fx.csv").exists()
    assert ns["read_table"]("prd_corpdata.dm_reference_gold.fx")["a"].tolist() == [1]


def test_read_table_null_handling(data_root):
    """Blanks and 'null' are nulls; a literal 'NA' (Namibia's ISO2 code) is a value."""
    ns = fresh_utils()
    path = data_root / "prd_mega" / "indicator" / "country.csv"
    path.parent.mkdir(parents=True)
    path.write_text("country_code,country_code_iso2,region\nNAM,NA,\nTGO,TG,null\n")
    df = ns["read_table"]("country")
    assert df["country_code_iso2"].tolist() == ["NA", "TG"]
    assert df["region"].isna().all()


def test_write_table_country_filter(data_root, monkeypatch):
    frame = pd.DataFrame({"country_name": ["Togo", "Albania", "Togo"], "year": [2020, 2020, 2021]})
    monkeypatch.setenv("COUNTRY_NAME", "Togo")
    ns = fresh_utils()
    ns["write_table"](frame, "t")
    assert ns["read_table"]("t")["country_name"].tolist() == ["Togo", "Togo"]
    ns["write_table"](pd.DataFrame({"economy": ["TGO", "ALB"]}), "u")  # no country_name column: untouched
    assert len(ns["read_table"]("u")) == 2


def test_update_version_flag_reads_env(data_root, monkeypatch):
    ns = fresh_utils()
    assert ns["update_version_flag"]("census_population_update_version") is False
    monkeypatch.setenv("CENSUS_POPULATION_UPDATE_VERSION", " True ")
    assert ns["update_version_flag"]("census_population_update_version") is True


def test_versioned_dataframe_fetches_once_then_serves_cache(data_root, monkeypatch):
    ns = fresh_utils()
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, **kw: calls.append(url) or FakeResponse(b"a,b\n1,2\n"))

    first = ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=False)
    assert list(first.columns) == ["a", "b"]  # fetched_at stripped
    assert len(calls) == 1
    ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=False)
    assert len(calls) == 1, "cached snapshot should be served without refetching"
    ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=True)
    assert len(calls) == 2


def test_wbgapi_fetch_joins_country_table(data_root, monkeypatch):
    ns = fresh_utils()
    write_country(ns)
    monkeypatch.setattr(wbgapi.data, "DataFrame", fake_wb_dataframe)
    df = ns["wbgapi_fetch"](["S1"], ["v1"], "src", extra_col_names_from_country_table=["income_level"])
    assert list(df.columns) == ["country_name", "country_code", "region", "income_level", "year", "v1", "data_source"]
    togo_2021 = df[(df.country_code == "TGO") & (df.year == 2021)].iloc[0]
    assert togo_2021.v1 == 11.0 and togo_2021.country_name == "Togo" and togo_2021.income_level == "LMC"
    assert len(df) == 3  # ALB 2021 was blank and dropped


# --- run.py ------------------------------------------------------------------

def test_scripts_exist_and_are_in_dependency_order():
    import run
    names = run.SCRIPTS
    assert not [n for n in names if not (REPO / n).is_file()]
    assert len(set(names)) == len(names)
    assert names.index("gdp.py") < names.index("health/health_expenditure.py")
    assert names.index("geo/admin_boundaries_extract.py") < names.index("geo/admin_boundaries_gold.py")
    assert names.index("population/tgo_subnational_population.py") < names.index("population/subnational_population_gold.py")


# --- scripts, end to end, offline ---------------------------------------------

def _wb_indicator_zip(indicator):
    csv = (
        '"Data Source","World Development Indicators",\n'
        '\n'
        '"Last Updated Date","2025-07-01",\n'
        '"Country Name","Country Code","Indicator Name","Indicator Code","2020","2021",\n'
        f'"Togo","TGO","Consumer price index (2010 = 100)","{indicator}","120.5","125.1",\n'
        f'"Albania","ALB","Consumer price index (2010 = 100)","{indicator}","110.0","",\n'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"API_{indicator}_DS2_en_csv_v2_1.csv", csv)
        zf.writestr(f"Metadata_Indicator_API_{indicator}_DS2_en_csv_v2_1.csv", "x")
    return buf.getvalue()


def test_consumer_price_index(data_root, monkeypatch):
    monkeypatch.setenv("COUNTRY_NAME", "Togo")
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(_wb_indicator_zip("FP.CPI.TOTL")))
    run_script("consumer_price_index.py")
    out = fresh_utils()["read_table"]("consumer_price_index")
    assert list(out.columns) == ["country_name", "country_code", "year", "cpi"]
    assert out.country_name.unique().tolist() == ["Togo"]
    assert out.set_index("year")["cpi"].to_dict() == {2020: 120.5, 2021: 125.1}


def test_gdp(data_root, monkeypatch):
    write_country(fresh_utils())
    monkeypatch.setattr(wbgapi.data, "DataFrame", fake_wb_dataframe)
    run_script("gdp.py")
    gdp = fresh_utils()["read_table"]("gdp")
    assert gdp.columns[:3].tolist() == ["country_name", "country_code", "region"]
    assert "gdp_current_lcu" in gdp.columns and gdp.country_code.isin(["TGO", "ALB"]).all()


def _census_gov_workbook(country, regions, years):
    """A census.gov international-programs workbook as _read_census_gov_excel expects it."""
    cols = ["COUNTRY", "ADM1_NAME", "ADM_LEVEL"] + [f"BTOTL_{y}" for y in years]
    rows = [["x"] * len(cols), ["x"] * len(cols), ["x"] * len(cols), cols, ["x"] * len(cols)]
    rows.append([country, country, 0] + [0] * len(years))
    for i, region in enumerate(regions):
        rows.append([country, region.upper(), 1] + [1000 * (i + 1) + y for y in years])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame(rows).to_excel(xw, sheet_name="2000-2025", header=False, index=False)
    return buf.getvalue()


def test_togo_subnational_population_then_union(data_root, monkeypatch):
    regions = ["Centrale", "Kara", "Maritime", "Plateaux", "Savanes"]
    years = list(range(2000, 2016))
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(_census_gov_workbook("TOGO", regions, years)))
    run_script("population/tgo_subnational_population.py")
    run_script("population/subnational_population_gold.py")
    ns = fresh_utils()
    silver = ns["read_table"]("tgo_subnational_population_silver")
    assert list(silver.columns) == ["country_name", "adm1_name", "year", "population", "data_source"]
    assert silver.country_name.unique().tolist() == ["Togo"] and sorted(silver.adm1_name.unique()) == regions
    assert len(silver) == 5 * len(years)
    pd.testing.assert_frame_equal(ns["read_table"]("subnational_population"), silver)


def test_admin_boundaries_gold(data_root):
    ns = fresh_utils()
    geojson = Path(ns["VOLUME_ROOT_PATH"]) / "auxiliary_data" / "admin1geoboundaries" / "World Bank Official Boundaries - Admin 1.geojson"
    geojson.parent.mkdir(parents=True)
    square = {"type": "Polygon", "coordinates": [[[0, 6], [2, 6], [2, 11], [0, 11], [0, 6]]]}
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"NAM_0": "Togo", "ISO_A3": "TGO", "ISO_A2": "TG", "NAM_1": "Kara", "WB_REGION": "AFR"}, "geometry": square},
        {"type": "Feature", "properties": {"NAM_0": "Democratic Republic of Congo", "ISO_A3": "COD", "ISO_A2": "CD", "NAM_1": "Kinshasa", "WB_REGION": "AFR"}, "geometry": square},
    ]}))
    run_script("geo/admin_boundaries_gold.py")
    gold = ns["read_table"]("admin1_boundaries_gold")
    assert list(gold.columns) == ["country_name", "country_code", "country_code_iso2", "admin1_region", "boundary"]
    assert gold.country_name.tolist() == ["Togo", "Congo, Dem. Rep."]
    assert json.loads(gold.boundary[0]) == square
    disputed = ns["read_table"]("admin0_disputed_boundaries_gold")
    assert disputed.empty and list(disputed.columns) == ["country_name", "region_name", "boundary", "country_code_iso2"]


def test_pefa_by_pillar(data_root):
    ns = fresh_utils()
    header = "Country,Year,Framework,PI-01,PI-02,PI-03,PI-04\n"
    ns["write_table"](pd.read_csv(io.StringIO(header + "Togo,2016,2016,B,C+,A,D\nAlbania,2017,2016,A,B,B+,C\n")), "pefa_2016_bronze")
    ns["write_table"](pd.read_csv(io.StringIO(header + "Togo,2008,2011,C,D+,B,C\n")), "pefa_2011_bronze")
    run_script("pefa/pefa_transform_load.py")
    gold = ns["read_table"]("pefa_by_pillar")
    assert list(gold.columns[:3]) == ["country_name", "year", "framework"]
    togo_2016 = gold[(gold.country_name == "Togo") & (gold.year == 2016)].iloc[0]
    assert togo_2016.pillar1_budget_reliability == pytest.approx((3 + 2.5 + 4) / 3)  # PI-01..03
    assert togo_2016.pillar2_transparency == pytest.approx(1.0)  # only PI-04 present
    assert set(gold.framework) == {2011, 2016}
