# Togo indicators

Builds the indicator tables that the Togo public-finance dashboard and the Togo BOOST
aggregate ([mega-boost](https://github.com/dime-worldbank/mega-boost), `Togo/TGO_aggregate.py`)
read: GDP, CPI, population, poverty, education and health outcomes, government revenue
and expenditure, PEFA scores, and the admin-1 boundaries and population behind the
regional maps. Everything is plain Python and pandas; each table is a CSV file.

The scripts are copies of the notebooks in
[mega-indicators](https://github.com/dime-worldbank/mega-indicators), which produce
the same tables on Databricks for all countries, with the Databricks parts removed and
`from utils import *` in place of the `%run` cells. Keep them otherwise identical so
fixes can be ported in either direction with a plain diff.

## Setup

Python 3.11 or newer, then:

```bash
pip install pandas requests wbgapi openpyxl
```

## Configuration

Environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `DATA_ROOT` | `./data` next to this README | Where everything is read and written |
| `COUNTRY_NAME` | `Togo` | Every table written keeps only this country's rows. Set it to an empty string to keep all countries. |
| `GDL_API_TOKEN` | none | Required by the Global Data Lab script only. A free token from [globaldatalab.org](https://globaldatalab.org) (create an account, then an API token). |

Tables are CSV files at `$DATA_ROOT/indicator/<table>.csv`, the folder the BOOST
aggregate reads as `INDICATOR_DIR`. Files that are not tables (the downloaded
boundaries GeoJSON, the source PDFs) go under `$DATA_ROOT/raw_data/`.

## Required inputs

The PEFA scores cannot be fetched from an API. Put the two tables in place before
running, as CSV files named after the table, e.g. `./data/indicator/pefa_2016_bronze.csv`.
Nulls may be blank or `null`.

| Table | Where it comes from | Columns |
|---|---|---|
| `pefa_2016_bronze` | [pefa.org](https://www.pefa.org/assessments/batch-downloads), Assessments, Batch downloads: Framework "2016 Framework", Country Togo, Type National, Status Final, Download. Save as CSV with the header as downloaded. | `Country`, `Year`, `Framework`, `PI-01` to `PI-31`; other columns are ignored |
| `pefa_2011_bronze` | Same, with Framework "2011 Framework". | `Country`, `Year`, `Framework`, `PI-01` to `PI-28` |

## Running

```bash
python run.py              # every script in run.py's SCRIPTS, in order
python run.py gdp.py       # one script
```

Each script runs in its own process and the run stops at the first failure; scripts
overwrite their tables, so rerunning is always safe. A full run takes a few minutes,
most of it waiting on the World Bank API, plus a 254 MB download for the boundaries.
Scripts in sub-folders import `utils.py` from this folder, so run them through `run.py`
(or with `PYTHONPATH=.`) rather than from inside their folder.

| Script | Table(s) written |
|---|---|
| `country.py` | `country` (codes, capital and coordinates from the World Bank API; map view and currency are constants in the script) |
| `gdp.py` | `gdp` |
| `consumer_price_index.py` | `consumer_price_index` |
| `population/national_population.py` | `population` |
| `poverty/poverty.py` | `poverty_rate` |
| `education/education_spending_icp.py` | `edu_spending` |
| `education/learning_poverty.py` | `learning_poverty_rate` |
| `health/health_expenditure.py` | `health_expenditure` (reads `gdp`) |
| `health/sdg_health.py` | `maternal_mortality_ratio_WHO`, `universal_health_coverage_index_GHO` |
| `pefa/pefa_transform_load.py` | `pefa_by_pillar` (reads the two PEFA inputs) |
| `public_finance/government_revenue_expenditure.py` | `government_revenue_expenditure` |
| `public_finance/togo/togo_revenue_budget.py` | `togo_revenue_budget` (figures from the DGB reports, see `public_finance/togo/README.md`; `togo_finance_report_extract.py` downloads the PDFs) |
| `geo/admin_boundaries_extract.py`, then `geo/admin_boundaries_gold.py` | `admin1_boundaries_gold`, `admin0_disputed_boundaries_gold` (empty: Togo has none) |
| `population/tgo_subnational_population.py`, then `population/subnational_population_gold.py` | `tgo_subnational_population_silver`, then `subnational_population` |
| `human_development/global_data_lab_hd_index_extract.py`, then `human_development/global_data_lab_hd_index_gold.py` | `global_data_lab_hd_index_bronze`, `global_data_lab_hd_index_silver`, then `global_data_lab_hd_index` (subnational human development indices and 6-17 school attendance; one API call per dataset and year, about 70 in all) |
| `indicator_data_availability.py` (last) | `indicator_data_availability`: earliest and latest year per indicator, for the dashboard's source notes |
| `poverty/subnational_poverty_extract.py`, then `poverty/subnational_poverty_gold.py` | `poverty_rate_SPID_GSAP_silver`, then `subnational_poverty_rate`. SPID lists Golfe (the Lomé area) as a sixth region that has no polygon in the boundaries, so it is absent from the map. |

Sources that are downloaded once and cached (`togo_census_raw`) are refreshed with the
matching environment variable, e.g. `CENSUS_POPULATION_UPDATE_VERSION=true`.

## Tests

`pytest -v` runs offline (the API calls are stubbed) against both current pandas and
pandas 1.5, the version the Databricks side uses.
