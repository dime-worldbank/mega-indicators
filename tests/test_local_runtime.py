"""Off-Databricks runtime: config.py's DATA_ROOT fallback, utils.py's CSV-backed table
IO, and notebooks run the way local_runner.py runs them. Everything runs in a temp
directory with no network (requests / wbgapi calls are monkeypatched)."""
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

import local_runner

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """A local-mode environment: no Databricks runtime, tables under tmp_path/data."""
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("BUNDLE_TARGET", raising=False)
    monkeypatch.delenv("COUNTRY_NAME", raising=False)
    root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(root))
    return root


def load_config():
    sys.modules.pop("config", None)
    import config
    return vars(config)


def load_shared():
    """utils (and config) freshly imported against the current environment."""
    for name in ("utils", "config"):
        sys.modules.pop(name, None)
    import utils
    return vars(utils)


def run_notebook(path):
    """As local_runner.py does: the notebook with utils' names in scope."""
    return runpy.run_path(str(path), init_globals=load_shared(), run_name="__main__")


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
        import json
        return json.loads(self.text)


# --- config.py ---------------------------------------------------------------

def test_config_requires_data_root_off_databricks(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATA_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="DATA_ROOT"):
        load_config()


def test_config_local_defaults_to_prod_schema(data_root):
    ns = load_config()
    assert ns["IS_DATABRICKS"] is False
    assert ns["DATA_ROOT"] == str(data_root)
    assert ns["INDICATOR_SCHEMA"] == "prd_mega.indicator"
    assert ns["VOLUME_ROOT_PATH"] == str(data_root) + "/raw_data"


def test_config_local_honours_bundle_target(data_root, monkeypatch):
    monkeypatch.setenv("BUNDLE_TARGET", "dev")
    ns = load_config()
    assert ns["INDICATOR_SCHEMA"] == "prd_mega.indicator_dev"

    monkeypatch.setenv("BUNDLE_TARGET", "nope")
    with pytest.raises(RuntimeError, match="Unknown bundle target"):
        load_config()


# --- utils.py table IO ---------------------------------------------------------

def test_write_then_read_table_roundtrip(data_root):
    ns = load_shared()
    df = pd.DataFrame({
        "country_name": ["Togo", "Congo, Dem. Rep."],
        "year": [2020, 2021],
        "value": [1.5, None],
    })
    assert not ns["table_exists"]("t")
    ns["write_table"](df, "t")
    assert (data_root / "prd_mega" / "indicator" / "t.csv").exists()
    assert ns["table_exists"]("t")
    pd.testing.assert_frame_equal(ns["read_table"]("t"), df)
    assert list(ns["read_table"]("t", columns=["year"]).columns) == ["year"]


def test_write_table_country_filter(data_root, monkeypatch):
    frame = pd.DataFrame({"country_name": ["Togo", "Albania", "Togo"], "year": [2020, 2020, 2021]})
    ns = load_shared()  # COUNTRY_NAME unset: everything is written
    ns["write_table"](frame, "t")
    assert len(ns["read_table"]("t")) == 3

    monkeypatch.setenv("COUNTRY_NAME", "Togo")
    ns = load_shared()
    ns["write_table"](frame, "t")
    assert ns["read_table"]("t")["country_name"].tolist() == ["Togo", "Togo"]
    ns["write_table"](pd.DataFrame({"economy": ["TGO", "ALB"]}), "u")  # no country_name column: untouched
    assert len(ns["read_table"]("u")) == 2


def test_qualified_table_name_maps_to_catalog_schema_dirs(data_root):
    ns = load_shared()
    ns["write_table"](pd.DataFrame({"a": [1]}), "prd_corpdata.dm_reference_gold.fx")
    assert (data_root / "prd_corpdata" / "dm_reference_gold" / "fx.csv").exists()
    assert ns["table_exists"]("prd_corpdata.dm_reference_gold.fx")
    assert ns["read_table"]("prd_corpdata.dm_reference_gold.fx")["a"].tolist() == [1]


def test_read_table_null_handling(data_root):
    """Blanks and 'null' are nulls; a literal 'NA' (Namibia's ISO2 code) is a value."""
    ns = load_shared()
    path = data_root / "prd_mega" / "indicator" / "country.csv"
    path.parent.mkdir(parents=True)
    path.write_text("country_code,country_code_iso2,region\nNAM,NA,\nTGO,TG,null\n")
    df = ns["read_table"]("country")
    assert df["country_code_iso2"].tolist() == ["NA", "TG"]
    assert df["region"].isna().all()


def test_update_version_flag_reads_env(data_root, monkeypatch):
    ns = load_shared()
    assert ns["update_version_flag"]("census_population_update_version") is False
    monkeypatch.setenv("CENSUS_POPULATION_UPDATE_VERSION", " True ")
    assert ns["update_version_flag"]("census_population_update_version") is True


def test_get_secret_reads_env(data_root, monkeypatch):
    ns = load_shared()
    monkeypatch.delenv("EMBER_ENERGY_KEY", raising=False)
    with pytest.raises(RuntimeError, match="EMBER_ENERGY_KEY"):
        ns["get_secret"]("DIMEBOOSTKEYVAULT", "ember_energy_key")
    monkeypatch.setenv("EMBER_ENERGY_KEY", "k3y")
    assert ns["get_secret"]("DIMEBOOSTKEYVAULT", "ember_energy_key") == "k3y"


def test_versioned_dataframe_fetches_once_then_serves_cache(data_root, monkeypatch):
    ns = load_shared()
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(b"a,b\n1,2\n")

    monkeypatch.setattr(requests, "get", fake_get)

    first = ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=False)
    assert list(first.columns) == ["a", "b"]  # fetched_at stripped
    assert "fetched_at" in pd.read_csv(data_root / "prd_mega" / "indicator" / "x_raw.csv").columns
    assert len(calls) == 1

    ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=False)
    assert len(calls) == 1, "cached snapshot should be served without refetching"

    ns["versioned_dataframe"]("http://example/x.csv", "x_raw", update_version=True)
    assert len(calls) == 2, "update_version=True should refetch"


def write_country(ns):
    ns["write_table"](pd.DataFrame({
        "country_name": ["Togo", "Albania", "World"],
        "country_code": ["TGO", "ALB", "WLD"],
        "region": ["Sub-Saharan Africa", "Europe & Central Asia", None],
        "income_level": ["LIC", "UMC", None],
        "is_aggregate": [False, False, True],
    }), "country")


def fake_wb_dataframe(series, skipBlanks=True):
    return pd.DataFrame({"YR2020": [10.0, 20.0], "YR2021": [11.0, None]},
                        index=pd.Index(["TGO", "ALB"], name="economy"))


def test_wbgapi_fetch_joins_local_country_table(data_root, monkeypatch):
    ns = load_shared()
    write_country(ns)
    monkeypatch.setattr(wbgapi.data, "DataFrame", fake_wb_dataframe)

    df = ns["wbgapi_fetch"](["S1"], ["v1"], "src", extra_col_names_from_country_table=["income_level"])
    assert list(df.columns) == ["country_name", "country_code", "region", "income_level", "year", "v1", "data_source"]
    togo_2021 = df[(df.country_code == "TGO") & (df.year == 2021)].iloc[0]
    assert togo_2021.v1 == 11.0 and togo_2021.country_name == "Togo" and togo_2021.income_level == "LIC"
    assert len(df) == 3  # ALB 2021 was blank and dropped


# --- local_runner's default list ------------------------------------------------------

def test_default_notebooks_exist_and_gdp_runs_before_its_readers():
    names = local_runner.NOTEBOOKS
    missing = [n for n in names if not (REPO / n).is_file()]
    assert not missing, missing
    assert len(set(names)) == len(names)
    assert names.index("gdp.py") < names.index("health/health_expenditure.py")


# --- converted producers, end to end, offline ------------------------------------------

def _census_gov_workbook(country, regions, years):
    """A census.gov international-programs workbook as _read_census_gov_excel expects it:
    sheet named by a year, two junk rows, one junk row, the header row, one more junk row, then data."""
    cols = ["COUNTRY", "ADM1_NAME", "ADM_LEVEL"] + [f"BTOTL_{y}" for y in years]
    rows = [["x"] * len(cols), ["x"] * len(cols), ["x"] * len(cols), cols, ["x"] * len(cols)]
    rows.append([country, country, 0] + [0] * len(years))
    for i, region in enumerate(regions):
        rows.append([country, region.upper(), 1] + [1000 * (i + 1) + y for y in years])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame(rows).to_excel(xw, sheet_name="2000-2025", header=False, index=False)
    return buf.getvalue()


def test_togo_subnational_population_then_union_run_locally(data_root, monkeypatch):
    regions = ["Centrale", "Kara", "Maritime", "Plateaux", "Savanes"]
    years = list(range(2000, 2016))
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(_census_gov_workbook("TOGO", regions, years)))

    run_notebook(REPO / "population" / "TGO" / "tgo_subnational_population.py")
    run_notebook(REPO / "population" / "subnational_population_gold.py")

    ns = load_shared()
    silver = ns["read_table"]("tgo_subnational_population_silver")
    assert list(silver.columns) == ["country_name", "adm1_name", "year", "population", "data_source"]
    assert silver.country_name.unique().tolist() == ["Togo"] and sorted(silver.adm1_name.unique()) == regions
    assert len(silver) == 5 * len(years)
    gold = ns["read_table"]("subnational_population")
    pd.testing.assert_frame_equal(gold, silver)


def _excel(sheets):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        for name, frame in sheets.items():
            frame.to_excel(xw, sheet_name=name, index=False)
    return buf.getvalue()


def test_subnational_poverty_notebooks_run_locally(data_root, monkeypatch):
    """SPID + GSAP stubs through both notebooks: GSAP's lineup year replaces SPID's, the poverty
    line follows the income group, a COL region is initcap'ed and an explicit fix wins."""
    spid = _excel({"Data": pd.DataFrame({
        "code": ["TGO", "TGO", "ALB", "COL", "COL"], "sample": ["Maritime", "Maritime", "Tirane", "Guajira", "NARINO"],
        "year": [2018, 2024, 2024, 2024, 2024], "survname": ["EHCVM", "EHCVM", "SILC", "GEIH", "GEIH"],
        "poor300": [0.4, 0.3, 0.01, 0.1, 0.2], "poor420": [0.6, 0.5, 0.05, 0.2, 0.3], "poor830": [0.9, 0.8, 0.3, 0.5, 0.6],
        "data_group": ["ALL"] * 5,
    })})
    gsap = _excel({"Metadata": pd.DataFrame({"note": ["x"]}), "Data": pd.DataFrame({
        "code": ["TGO", "ALB", "COL", "COL"], "sample": ["Maritime", "Tirane", "Guajira", "NARINO"], "lineupyear": [2024] * 4,
        "survname": ["EHCVM", "SILC", "GEIH", "GEIH"], "poor300_ln": [0.31, 0.011, 0.11, 0.21],
        "poor420_ln": [0.51, 0.051, 0.21, 0.31], "poor830_ln": [0.81, 0.31, 0.51, 0.61],
    })})
    files = {"https://datacatalogfiles.worldbank.org/ddh-published/0012345/DR0092191/spid.xlsx": spid, "https://datacatalogfiles.worldbank.org/ddh-published/0012345/DR0052555/gsap.xlsx": gsap}

    def fake_get(url, **kw):
        if url.endswith("DR0092191"):
            return FakeResponse(json.dumps({"distribution": {"url": "https://datacatalogfiles.worldbank.org/ddh-published/0012345/DR0092191/spid.xlsx"}}))
        if url.endswith("DR0052555"):
            return FakeResponse(json.dumps({"distribution": {"url": "https://datacatalogfiles.worldbank.org/ddh-published/0012345/DR0052555/gsap.xlsx"}}))
        return FakeResponse(files[url])

    monkeypatch.setattr(requests, "get", fake_get)
    ns = load_shared()
    ns["write_table"](pd.DataFrame({
        "country_name": ["Togo", "Albania", "Colombia"], "country_code": ["TGO", "ALB", "COL"],
        "region": ["SSF", "ECS", "LCN"], "income_level": ["LMC", "UMC", "UMC"], "is_aggregate": [False] * 3,
    }), "country")
    run_notebook(REPO / "poverty" / "subnational_poverty" / "subnational_poverty_index_extract_transform.py")
    run_notebook(REPO / "poverty" / "subnational_poverty" / "subnational_poverty_index_transform_load.py")

    gold = load_shared()["read_table"]("subnational_poverty_rate").sort_values(["country_code", "region_name", "year"])
    togo = gold[gold.country_code == "TGO"]
    assert togo.year.tolist() == [2018, 2024] and togo.data_source.tolist() == ["SPID", "GSAP"]
    assert togo.poverty_rate.tolist() == [0.6, 0.51]  # LMC: the 4.20 line; 2024 from GSAP replaces SPID's
    assert togo.earliest_year.unique().tolist() == [2018] and togo.latest_year.unique().tolist() == [2024]
    assert gold[gold.country_code == "ALB"].poverty_rate.tolist() == [0.31]  # UMC: the 8.30 line
    assert sorted(gold[gold.country_code == "COL"].region_name) == ["La Guajira", "Narino"]  # 'Guajira' hits an explicit fix (matched on the raw name), 'NARINO' gets Colombia's initcap


    ns = load_shared()
    geojson = Path(ns["VOLUME_ROOT_PATH"]) / "auxiliary_data" / "admin1geoboundaries" / "World Bank Official Boundaries - Admin 1.geojson"
    geojson.parent.mkdir(parents=True)
    square = {"type": "Polygon", "coordinates": [[[0, 6], [2, 6], [2, 11], [0, 11], [0, 6]]]}
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"NAM_0": "Togo", "ISO_A3": "TGO", "ISO_A2": "TG", "NAM_1": "Kara", "WB_REGION": "AFR"}, "geometry": square},
        {"type": "Feature", "properties": {"NAM_0": "Democratic Republic of Congo", "ISO_A3": "COD", "ISO_A2": "CD", "NAM_1": "Kinshasa", "WB_REGION": "AFR"}, "geometry": square},
    ]}))

    run_notebook(REPO / "geo" / "admin_boundaries_gold.py")

    gold = ns["read_table"]("admin1_boundaries_gold")
    assert list(gold.columns) == ["country_name", "country_code", "country_code_iso2", "admin1_region", "boundary"]
    assert gold.country_name.tolist() == ["Togo", "Congo, Dem. Rep."]
    assert json.loads(gold.boundary[0]) == square
    disputed = ns["read_table"]("admin0_disputed_boundaries_gold")
    assert disputed.empty and list(disputed.columns) == ["country_name", "region_name", "boundary", "country_code_iso2"]


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


def test_consumer_price_index_notebook_runs_locally(data_root, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(_wb_indicator_zip("FP.CPI.TOTL")))

    run_notebook(REPO / "consumer_price_index.py")

    out = pd.read_csv(data_root / "prd_mega" / "indicator" / "consumer_price_index.csv")
    assert list(out.columns) == ["country_name", "country_code", "year", "cpi"]
    togo = out[out.country_code == "TGO"].set_index("year")["cpi"]
    assert togo[2020] == 120.5 and togo[2021] == 125.1
    assert pd.isna(out[(out.country_code == "ALB") & (out.year == 2021)]["cpi"].iloc[0])


def test_gdp_then_edu_private_spending_notebooks_chain_locally(data_root, monkeypatch):
    """gdp.py (wbgapi + country) writes gdp.csv; education_private_spending.py then
    reads it back through read_table from a sub-directory notebook."""
    write_country(load_shared())
    monkeypatch.setattr(wbgapi.data, "DataFrame", fake_wb_dataframe)
    run_notebook(REPO / "gdp.py")

    gdp = pd.read_csv(data_root / "prd_mega" / "indicator" / "gdp.csv")
    assert gdp.columns[:3].tolist() == ["country_name", "country_code", "region"]
    assert "gdp_current_lcu" in gdp.columns and gdp.country_code.isin(["TGO", "ALB"]).all()

    oecd_csv = (
        "REF_AREA,EDUCATION_LEV,TIME_PERIOD,OBS_VALUE\n"
        "TGO,ISCED11_0,2020,0.5\n"
        "TGO,ISCED11_1T8,2020,1.5\n"
        "ALB,ISCED11_1T8,2021,2.0\n"
    )
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(oecd_csv.encode()))
    run_notebook(REPO / "education" / "education_private_spending.py")

    out = pd.read_csv(data_root / "prd_mega" / "indicator" / "edu_private_spending.csv")
    togo = out[out.country_code == "TGO"].iloc[0]
    assert togo.year == 2020 and togo.edu_private_spending_share_gdp == pytest.approx(0.02)
    assert togo.edu_private_spending_current_lcu == pytest.approx(0.02 * 10.0)
    assert "ALB" not in out.country_code.tolist()  # ALB 2021 GDP was blank -> inner merge drops it
