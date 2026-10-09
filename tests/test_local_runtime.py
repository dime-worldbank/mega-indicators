"""Off-Databricks runtime: config.py's DATA_ROOT fallback, utils.py's CSV-backed table
IO and environment fallbacks, local_runner.py's list and resume, and notebooks run the
way the runner runs them. Everything runs in a temp directory with no network
(requests / wbgapi calls are monkeypatched)."""
import io
import json
import re
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
    """As local_runner.py does: the notebook with utils' (and config's) names in scope."""
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


def test_http_get_retries_transient_failures_then_gives_up(data_root, monkeypatch, capsys):
    ns = load_shared()
    monkeypatch.setattr(ns["time"], "sleep", lambda s: None)
    outcomes = [requests.exceptions.ChunkedEncodingError("cut short"), FakeResponse(b"", 502),
                requests.exceptions.ConnectionError("reset"), FakeResponse(b"ok")]

    def fake_get(url, **kwargs):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(requests, "get", fake_get)
    assert ns["http_get"]("http://example/x").text == "ok"
    assert outcomes == [] and capsys.readouterr().out.count("retry") == 3

    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(b"", 404))
    assert ns["http_get"]("http://example/x").status_code == 404  # not transient: no retry
    assert "retry" not in capsys.readouterr().out

    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(b"", 503))
    assert ns["http_get"]("http://example/x", retries=2).status_code == 503  # the last response, for raise_for_status
    assert capsys.readouterr().out.count("retry") == 2

    def always_reset(url, **kw):
        raise requests.exceptions.ConnectionError("reset")
    monkeypatch.setattr(requests, "get", always_reset)
    with pytest.raises(requests.exceptions.ConnectionError):
        ns["http_get"]("http://example/x", retries=1)

    # a timeout means the server is up but slow: one more try, then fail
    outcomes[:] = [requests.exceptions.ReadTimeout("slow"), FakeResponse(b"ok")]
    monkeypatch.setattr(requests, "get", fake_get)
    assert ns["http_get"]("http://example/x").text == "ok"
    outcomes[:] = [requests.exceptions.ReadTimeout("slow"), requests.exceptions.ReadTimeout("slow"), FakeResponse(b"never")]
    with pytest.raises(requests.exceptions.ReadTimeout):
        ns["http_get"]("http://example/x")
    assert len(outcomes) == 1  # the third attempt was not made


def test_wbgapi_requests_retry_transient_failures_but_not_slow_pages_for_long(data_root):
    """utils points wbgapi at a session that retries resets and 5xx several times but a read timeout once."""
    shared = load_shared()
    session = wbgapi.requests
    assert isinstance(session, requests.Session)
    retry = session.get_adapter("https://api.worldbank.org/v2/country").max_retries
    assert retry.total >= 3 and 503 in retry.status_forcelist and "GET" in retry.allowed_methods
    assert retry.read == 1
    assert wbgapi.get_options["timeout"] == shared["DEFAULT_TIMEOUT_SECONDS"]


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
    df = ns["wbgapi_fetch"](["SP.X"], ["v1"], "WB", extra_col_names_from_country_table=["income_level"])
    assert list(df.columns) == ["country_name", "country_code", "region", "income_level", "year", "v1", "data_source"]
    togo_2021 = df[(df.country_code == "TGO") & (df.year == 2021)].iloc[0]
    assert togo_2021.v1 == 11.0 and togo_2021.country_name == "Togo" and togo_2021.income_level == "LIC"
    assert len(df) == 3  # ALB 2021 was blank and dropped


# --- local_runner.py -------------------------------------------------------------------

def test_default_notebooks_exist_and_producers_run_before_their_readers():
    names = local_runner.NOTEBOOKS
    placeholder = local_runner.SUBNATIONAL_POPULATION
    missing = [n for n in names if n != placeholder and not (REPO / n).is_file()]
    assert not missing, missing
    assert len(set(names)) == len(names)
    assert names.index("gdp.py") < names.index("health/health_expenditure.py")
    # country.py reads admin1_boundaries_gold for the map centroids
    assert names.index("geo/admin_boundaries_transform_load.py") < names.index("country.py")
    assert names[-1] == "indicator_data_availability.py"
    # the placeholder resolves to population/<ISO3>/<iso3>_subnational_population.py, which exists for
    # every country the union stacks, and the union comes after it
    union = REPO / local_runner.SUBNATIONAL_POPULATION_UNION
    codes = re.findall(r"'([a-z]{3})'", re.search(r"^country_codes = .*$", union.read_text(), re.M).group())
    assert len(codes) >= 18
    assert all((REPO / "population" / code.upper() / f"{code}_subnational_population.py").is_file() for code in codes)
    assert names.index(placeholder) < names.index(local_runner.SUBNATIONAL_POPULATION_UNION)


def test_runner_resumes_from_a_notebook(data_root):
    load_shared()  # no COUNTRY_NAME
    names = local_runner.NOTEBOOKS
    assert local_runner.notebooks_from() == names
    assert local_runner.notebooks_from("gdp.py") == names[names.index("gdp.py"):]
    assert local_runner.notebooks_from("./gdp.py") == names[names.index("gdp.py"):]
    assert local_runner.notebooks_from(str(REPO / "gdp.py")) == names[names.index("gdp.py"):]
    placeholder = local_runner.SUBNATIONAL_POPULATION  # a per-country notebook resumes at its placeholder
    assert local_runner.notebooks_from("population/TGO/tgo_subnational_population.py") == names[names.index(placeholder):]
    with pytest.raises(SystemExit):
        local_runner.notebooks_from("nope.py")


def test_runner_picks_the_country_population_notebook_from_country_name(data_root, monkeypatch, capsys):
    ns = load_shared()
    assert local_runner.subnational_population_notebook() is None  # no COUNTRY_NAME
    ns["write_table"](pd.DataFrame({"country_name": ["Togo", "Nigeria"], "country_code": ["TGO", "NGA"]}), "country")
    monkeypatch.setenv("COUNTRY_NAME", "Nigeria")
    load_shared()
    assert local_runner.subnational_population_notebook() == "population/NGA/nga_subnational_population.py"
    with pytest.raises(SystemExit, match="is not COUNTRY_NAME's notebook"):
        local_runner.notebooks_from("population/TGO/tgo_subnational_population.py")
    monkeypatch.setenv("COUNTRY_NAME", "Nowhere")
    load_shared()
    with pytest.raises(SystemExit, match="not a country_name"):
        local_runner.subnational_population_notebook()

    ran = []
    monkeypatch.setattr(local_runner.subprocess, "run",
                        lambda cmd: (ran.append(str(Path(cmd[-1]).relative_to(REPO))), local_runner.subprocess.CompletedProcess(cmd, 0))[1])
    union = local_runner.SUBNATIONAL_POPULATION_UNION
    monkeypatch.delenv("COUNTRY_NAME")
    load_shared()
    local_runner.run_in_order([local_runner.SUBNATIONAL_POPULATION, union, "gdp.py"])
    assert ran == ["gdp.py"]  # without COUNTRY_NAME the two subnational population entries are skipped, loudly
    assert capsys.readouterr().out.count("skipped, COUNTRY_NAME is not set") == 2
    monkeypatch.setenv("COUNTRY_NAME", "Togo")
    load_shared()
    ran.clear()
    local_runner.run_in_order([local_runner.SUBNATIONAL_POPULATION, union])
    assert ran == ["population/TGO/tgo_subnational_population.py", union]


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


def test_runner_stops_at_the_first_failure_and_says_how_to_resume(monkeypatch):
    ran = []

    def fake_run(cmd):
        ran.append(Path(cmd[-1]).name)
        return local_runner.subprocess.CompletedProcess(cmd, 1 if cmd[-1].endswith("gdp.py") else 0)

    monkeypatch.setattr(local_runner.subprocess, "run", fake_run)
    with pytest.raises(SystemExit, match="--from gdp.py"):
        local_runner.run_in_order(["country.py", "gdp.py", "consumer_price_index.py"])
    assert ran == ["country.py", "gdp.py"]


# --- converted notebooks, end to end, offline ------------------------------------------

def test_public_sector_employment_notebook_runs_locally(data_root):
    ns = load_shared()
    ns["write_table"](pd.DataFrame({"country_name": ["Togo", "Nigeria", "Sub-Saharan Africa"], "country_code": ["TGO", "NGA", "SSF"],
                                    "region": ["SSF", "SSF", None]}), "country")
    ns["write_table"](pd.DataFrame({"economy": ["TGO", "NGA", "XXX"], "year": [2020] * 3, "wage_percent_gdp": [6.0, 4.0, 1.0],
                                    "wage_percent_expenditure": [30.0, 20.0, 1.0], "wage_premium": [0.1, None, 1.0], "data_source": ["WWBI"] * 3}),
                     "public_sector_employment_silver")
    run_notebook(REPO / "public_sector_employment" / "wwbi_transform_load.py")
    out = ns["read_table"]("public_sector_employment")
    assert list(out.columns) == ["country_code", "year", "wage_percent_gdp", "wage_percent_expenditure", "wage_premium", "data_source", "country_name", "region"]
    assert sorted(out.country_code) == ["NGA", "SSF", "TGO"]  # XXX is not in country; SSF is the regional mean
    ssf = out.set_index("country_code").loc["SSF"]
    assert (ssf.wage_percent_gdp, ssf.wage_percent_expenditure, ssf.wage_premium, ssf.country_name) == (5.0, 25.0, 0.1, "Sub-Saharan Africa")


def test_global_data_lab_hdi_notebooks_run_locally(data_root, monkeypatch):
    """Two datasets, every year since 1990, two countries and two regions each: the extract
    collapses to one row per region and year; the transform renames, fixes region names,
    joins country and scales attendance."""
    monkeypatch.setenv("GDL_API_TOKEN", "t0k3n")
    calls = []

    def fake_get(url, params=None, **kw):
        calls.append(url)
        assert params["token"] == "t0k3n" and "/download/" in url
        dataset = url.split("/download/")[0].rsplit("/", 1)[1]
        year = int(url.split("/download/")[1].split("/")[0])
        head = "Country,ISO_Code,Level,GDLCODE,Region,Year"
        rows = [("Togo", "TGO", "Total", "TGOt", 0), ("Togo", "TGO", "Maritime (incl. Lome)", "TGOr101", 1),
                ("Nigeria", "NGA", "Total", "NGAt", 0), ("Nigeria", "NGA", "Nassarawa", "NGAr120", 1)]
        if dataset == "shdi":
            body = "\n".join(f"{c},{iso},{lvl},{code},{reg},{year},0.5,0.6,0.7" for c, iso, reg, code, lvl in rows)
            return FakeResponse(f"{head},healthindex,edindex,incindex\n{body}\n")
        body = "\n".join(f"{c},{iso},{lvl},{code},{reg},{year},80,70,60,50" for c, iso, reg, code, lvl in rows)
        return FakeResponse(f"{head},lprimary,uprimary,lsecondary,usecondary\n{body}\n")

    monkeypatch.setattr(requests, "get", fake_get)
    ns = load_shared()
    ns["write_table"](pd.DataFrame({"country_name": ["Togo", "Nigeria"], "country_code": ["TGO", "NGA"],
                                    "region": ["SSF", "SSF"], "income_level": ["LMC", "LMC"], "is_aggregate": [False, False]}), "country")
    extract = run_notebook(REPO / "human_development" / "global_data_lab_hdi_extract.py")
    run_notebook(REPO / "human_development" / "global_data_lab_hdi_transform_load.py")

    bronze = load_shared()["read_table"]("global_data_lab_hd_index_bronze")
    assert "Year" in bronze.columns and not bronze.duplicated().any()  # as the API names it; identical downloads collapsed
    gold = load_shared()["read_table"]("global_data_lab_hd_index")
    assert list(gold.columns) == ["country_name", "adm1_name", "year", "education_index", "health_index", "income_index", "attendance", "attendance_6to17yo"]
    assert sorted(gold[gold.country_name == "Togo"].adm1_name.unique()) == ["Maritime", "Total"]  # parenthetical stripped
    assert sorted(gold[gold.country_name == "Nigeria"].adm1_name.unique()) == ["Nasarawa", "Total"]  # explicit fix
    assert not gold.duplicated(["country_name", "adm1_name", "year"]).any()
    row = gold[(gold.adm1_name == "Maritime") & (gold.year == 1990)].iloc[0]
    assert (row.health_index, row.education_index, row.income_index) == (0.5, 0.6, 0.7)
    assert row.attendance == 65.0 and row.attendance_6to17yo == 0.65
    assert len(calls) == 2 * (extract["END_YEAR"] - 1990 + 1)


def test_indicator_data_availability_notebook_runs_locally(data_root):
    ns = load_shared()
    w = ns["write_table"]
    togo = {"country_name": ["Togo"] * 3, "year": [2000, 2010, 2020]}
    w(pd.DataFrame({**togo, "health_index": [0.4, None, 0.6], "education_index": [0.3, 0.5, 0.5], "attendance_6to17yo": [None, 0.7, 0.8]}), "global_data_lab_hd_index")
    w(pd.DataFrame({"country_name": ["Togo", "Togo"], "year": [2015, 2019]}), "learning_poverty_rate")
    w(pd.DataFrame({"country_name": ["Togo"] * 2, "year": [2006, 2024], "poverty_rate": [0.9, 0.6]}), "subnational_poverty_rate")
    w(pd.DataFrame({"country_name": ["Togo"] * 2, "year": [2000, 2021], "universal_health_coverage_index": [None, 45.0]}), "universal_health_coverage_index_GHO")
    w(pd.DataFrame({"country_name": ["Togo", "Togo"], "year": [2008, 2016]}), "pefa_by_pillar")
    w(pd.DataFrame({"country_name": ["Togo"] * 2, "year": [2000, 2022], "oop_per_capita_usd": [20.0, 40.0]}), "health_expenditure")
    w(pd.DataFrame({"country_name": ["Togo"] * 2, "year": [2011, 2021], "poverty_rate": [0.5, None]}), "poverty_rate")
    # any-of columns: 2010 counts on tertiary alone, 2020 has nothing
    w(pd.DataFrame({**togo, "pupil_teacher_ratio_pre_primary": [30.0, None, None], "pupil_teacher_ratio_primary": [None] * 3,
                    "pupil_teacher_ratio_secondary": [None] * 3, "pupil_teacher_ratio_lower_secondary": [None] * 3,
                    "pupil_teacher_ratio_upper_secondary": [None] * 3, "pupil_teacher_ratio_tertiary": [None, 12.0, None]}), "pupil_teacher_ratio")
    w(pd.DataFrame({**togo, **{c: [1.0, 2.0, 3.0] for c in [
        "schools_with_electricity_primary", "schools_with_electricity_lower_secondary", "schools_with_electricity_upper_secondary",
        "schools_with_internet_primary", "schools_with_internet_lower_secondary", "schools_with_internet_upper_secondary",
        "schools_with_computers_primary", "schools_with_computers_lower_secondary", "schools_with_computers_upper_secondary",
        "schools_with_basic_water_primary", "schools_with_basic_water_lower_secondary", "schools_with_basic_water_upper_secondary"]}}), "school_basic_services")
    w(pd.DataFrame({**togo, **{c: [None, 5.0, 6.0] for c in ["teacher_salary_pre_primary", "teacher_salary_primary", "teacher_salary_lower_secondary", "teacher_salary_upper_secondary"]}}), "teacher_salaries")
    w(pd.DataFrame({**togo, **{c: [7.0, 8.0, None] for c in ["completion_rate_primary", "completion_rate_lower_secondary", "completion_rate_upper_secondary"]}}), "completion_rates")

    run_notebook(REPO / "indicator_data_availability.py")

    out = ns["read_table"]("indicator_data_availability")
    assert list(out.columns) == ["country_name", "indicator_key", "earliest_year", "latest_year", "source_url"]
    assert len(out) == 12 and out.earliest_year.dtype.kind == "i"
    span = out.set_index("indicator_key")[["earliest_year", "latest_year"]].apply(tuple, axis=1).to_dict()
    assert span["global_data_lab_hd_index"] == (2000, 2020)  # 2010 dropped: health_index null
    assert span["global_data_lab_attendance"] == (2010, 2020)
    assert span["universal_health_coverage_index_gho"] == (2021, 2021)
    assert span["poverty_rate"] == (2011, 2011)
    assert span["pupil_teacher_ratio"] == (2000, 2010)
    assert span["teacher_salaries"] == (2010, 2020) and span["completion_rates"] == (2000, 2010)
    assert span["learning_poverty_rate"] == (2015, 2019) and span["pefa_by_pillar"] == (2008, 2016)
    assert out.source_url.str.startswith("https://").all()


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


def _square(x0, y0, size=1):
    return {"type": "Polygon", "coordinates": [[[x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]]}


def test_admin_boundaries_notebook_runs_locally(data_root):
    """Name corrections, the DRC rename, Albania districts unioned into a county, Ghana's new
    regions into an old one, and the Ilemi Triangle attributed to both claimants."""
    from shapely.geometry import shape
    ns = load_shared()
    raw = Path(ns["VOLUME_ROOT_PATH"]) / "auxiliary_data"
    (raw / "admin1geoboundaries").mkdir(parents=True)
    (raw / "admin0geoboundaries").mkdir(parents=True)

    def feature(nam0, iso3, iso2, nam1, geom):
        return {"type": "Feature", "properties": {"NAM_0": nam0, "ISO_A3": iso3, "ISO_A2": iso2, "NAM_1": nam1, "WB_REGION": "X"}, "geometry": geom}
    (raw / "admin1geoboundaries" / "World Bank Official Boundaries - Admin 1.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": [
        feature("Togo", "TGO", "TG", "Kara", _square(0, 0)),
        feature("Democratic Republic of Congo", "COD", "CD", "Kinshasa", _square(10, 0)),
        feature("Nigeria", "NGA", "NG", "Nassarawa", _square(20, 0)),
        feature("Albania", "ALB", "AL", "Kolonje", _square(30, 0)),   # both map to Korce
        feature("Albania", "ALB", "AL", "Devoll", _square(31, 0)),
        feature("Albania", "ALB", "AL", "Berat", _square(40, 0)),
        feature("Ghana", "GHA", "GH", "Bono", _square(50, 0)),        # both map to Brong Ahafo
        feature("Ghana", "GHA", "GH", "Ahafo", _square(51, 0)),
    ]}))
    (raw / "admin0geoboundaries" / "World Bank Official Boundaries - Admin 0_all_layers.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"NAM_0": "Ilemi Triangle", "ISO_A2": "", "WB_REGION": "AFR", "WB_STATUS": "Non-determined legal status area"}, "geometry": _square(60, 0)},
        {"type": "Feature", "properties": {"NAM_0": "Kenya", "ISO_A2": "KE", "WB_REGION": "AFR", "WB_STATUS": "Member State"}, "geometry": _square(70, 0)},
    ]}))

    run_notebook(REPO / "geo" / "admin_boundaries_transform_load.py")

    r = ns["read_table"]
    assert len(r("admin1_boundaries_bronze")) == 8
    gold = r("admin1_boundaries_gold")
    assert list(gold.columns) == ["country_name", "country_code", "country_code_iso2", "admin1_region", "boundary"]
    by = gold.set_index(["country_name", "admin1_region"])
    assert ("Congo, Dem. Rep.", "Ville Province De Kinshasa") in by.index and ("Nigeria", "Nasarawa") in by.index
    assert ("Togo", "Kara") in by.index and len(gold) == 6
    assert sorted(gold[gold.country_name == "Albania"].admin1_region) == ["Berat", "Korce"]
    assert shape(json.loads(by.loc[("Albania", "Korce"), "boundary"])).area == 2.0  # two unit squares unioned
    assert gold[gold.country_name == "Ghana"].admin1_region.tolist() == ["Brong Ahafo"]
    disputed = r("admin0_disputed_boundaries_gold")
    assert list(disputed.columns) == ["country_name", "region_name", "boundary", "country_code_iso2"]
    assert sorted(zip(disputed.country_name, disputed.region_name)) == [("Kenya", "Ilemi Triangle"), ("South Sudan", "Ilemi Triangle")]


def _wb_economies():
    return pd.DataFrame({
        "name": ["Togo", "World"], "aggregate": [False, True], "longitude": [1.2255, None], "latitude": [6.1228, None],
        "region": ["SSF", "NA"], "adminregion": ["SSA", ""], "lendingType": ["IDX", ""], "incomeLevel": ["LMC", "NA"], "capitalCity": ["Lome", ""],
    }, index=pd.Index(["TGO", "WLD"], name="id"))


def test_country_notebook_runs_locally(data_root, monkeypatch):
    """World Bank metadata, the map centroid of the boundaries, the zoom, and the currency
    from the corporate table when present, else from the dictionary in the notebook."""
    monkeypatch.setattr(wbgapi.economy, "DataFrame", _wb_economies)
    ns = load_shared()
    ns["write_table"](pd.DataFrame({"country_name": ["Togo", "Togo"], "country_code": ["TGO", "TGO"], "country_code_iso2": ["TG", "TG"],
                                    "admin1_region": ["A", "B"], "boundary": [json.dumps(_square(0, 0)), json.dumps(_square(1, 0))]}), "admin1_boundaries_gold")
    run_notebook(REPO / "country.py")  # no corporate table: the fallback
    country = ns["read_table"]("country")
    assert list(country.columns) == ["country_name", "country_code", "longitude", "latitude", "region", "lending_type", "income_level",
                                     "capital_city", "is_aggregate", "country_code_iso2", "display_lon", "display_lat", "zoom",
                                     "currency_name", "currency_code", "country_code_iso3"]
    togo = country.set_index("country_code").loc["TGO"]
    assert (togo.country_name, togo.country_code_iso2, togo.income_level, togo.zoom, togo.country_code_iso3) == ("Togo", "TG", "LMC", 5.0, "TGO")
    assert (togo.display_lon, togo.display_lat) == (1.0, 0.5)  # centroid of two unit squares side by side
    assert (togo.currency_code, togo.currency_name) == ("XOF", "C.F.A. Francs BCEAO")
    world = country.set_index("country_code").loc["WLD"]
    assert world.is_aggregate == True and pd.isna(world.display_lon) and pd.isna(world.currency_code)  # noqa: E712

    ns["write_table"](pd.DataFrame({"cntry_code": ["TG", "TG"], "ccy_src_name": ["Old name", "C.F.A. Francs BCEAO"],
                                    "ccy_src_code": ["XOF", "XOF"], "ccy_exch_rate_ref_date": ["2020-01-01", "2024-01-01"]}),
                     "prd_corpdata.dm_reference_gold.v_dim_country_currency_exchange_rate")
    run_notebook(REPO / "country.py")  # the corporate table wins, latest row per country
    assert ns["read_table"]("country").set_index("country_code").loc["TGO", "currency_name"] == "C.F.A. Francs BCEAO"


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


def test_no_notebook_calls_spark_directly():
    """Every notebook goes through utils' read_table / write_table, so one storage path serves both sides."""
    offenders = [str(p) for p in REPO.rglob("*.py")
                 if "data" not in p.parts and ".pytest_cache" not in p.parts and p.name not in ("utils.py", "config.py") and "tests" not in p.parts
                 and re.search(r"\bspark\.|\bdbutils\.", p.read_text())]
    assert offenders == []


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


def test_togo_subnational_population_notebook_runs_locally(data_root, monkeypatch):
    regions = ["Centrale", "Kara", "Maritime", "Plateaux", "Savanes"]
    years = list(range(2000, 2016))
    monkeypatch.setattr(requests, "get", lambda url, **kw: FakeResponse(_census_gov_workbook("TOGO", regions, years)))
    run_notebook(REPO / "population" / "TGO" / "tgo_subnational_population.py")

    silver = load_shared()["read_table"]("tgo_subnational_population_silver")
    assert list(silver.columns) == ["country_name", "adm1_name", "year", "population", "data_source"]
    assert silver.country_name.unique().tolist() == ["Togo"] and sorted(silver.adm1_name.unique()) == regions
    assert len(silver) == 5 * len(years)

    with pytest.raises(FileNotFoundError):
        run_notebook(REPO / "population" / "subnational_population.py")  # a full run needs every listed country's table
    monkeypatch.setenv("COUNTRY_NAME", "Togo")
    run_notebook(REPO / "population" / "subnational_population.py")  # a one-country run stacks what it has
    pd.testing.assert_frame_equal(load_shared()["read_table"]("subnational_population"), silver)
