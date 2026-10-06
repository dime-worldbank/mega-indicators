# mega-indicators
A collection of notebooks to fetch and store indicator datasets

## Deployment

The jobs and DLT pipelines are defined as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/)
(`databricks.yml` + `resources/`) and deployed with the Databricks CLI. All targets
(dev, staging, prod) are deployed *and* run as the `RPF-ADBSvc-PROD` service principal,
so deploys, resource ownership, and monitoring aren't tied to any one person's account.

### One-time setup: service-principal profile

Ask a workspace admin for an OAuth secret for the service principal, then add this
profile to `~/.databrickscfg`:

```ini
[RPF-ADBSvc-PROD]
host          = <workspace url>
client_id     = <service principal application id>
client_secret = <oauth secret from the admin>
auth_type     = oauth-m2m
```

### Deploying

```bash
# lint the bundle
databricks bundle validate -t staging -p RPF-ADBSvc-PROD

# deploy your current working tree as [staging] copies (schedules paused) and run one
databricks bundle deploy -t staging -p RPF-ADBSvc-PROD
databricks bundle run indicators_weekly -t staging -p RPF-ADBSvc-PROD
```

The same commands work with `-t dev` and `-t prod`. Each target writes to its own
schema *and* its own volume, isolated from prod's:

| Target | Schema | Volume | Purpose |
|---|---|---|---|
| `dev` | `prd_mega.indicator_dev` | `vboost4_dev` | Testing work-in-progress branches |
| `staging` | `prd_mega.indicator_staging` | `vboost4_staging` | Pre-prod validation (paused schedules) |
| `prod` | `prd_mega.indicator` | `vboost4` | The real thing (live schedules + failure emails) |


Prod is bound to the existing jobs/pipelines (no duplicates) and deploys to the team's
`/Workspace/Repos/boostprocessed` folder, with `CAN_MANAGE` granted to the
`ITSDA-LKHS-DAP-PROD-boostprocessed` group. The GDL token is read from the existing
`DIMEBOOSTKEYVAULT` secret scope — no setup needed.

## Contributing

To add more indicators, please open a pull request after you've tested your code in Databricks.

- See [consumer_price_index.py](consumer_price_index.py) as a Python example of fetching data from WB API
- See [global_data_lab.r](global_data_lab.r) as an R example of fetching data using a R package from an external data source. Note running this as a job will require setting the `GDL_API_TOKEN` environment variable. Follow the instructions [here](https://docs.globaldatalab.org/gdldata/) to obtain the API token.
- If your source is a single external site (a national stats agency, etc.) rather than a
  well-established API, fetch it through `utils.py`'s `versioned_dataframe`/`fetch_raw`
  instead of calling `requests`/`pd.read_csv` directly — it caches the parsed result as a
  Delta table, so a temporarily unreachable source serves last-known-good data instead of
  failing the pipeline. See [pry_subnational_population.py](population/PRY/pry_subnational_population.py)
  for a CSV example and [alb_subnational_population.py](population/ALB/alb_subnational_population.py)
  for Excel (`parse=`).

## Running without Databricks

The notebooks also run as plain Python, with each table stored as a CSV at
`$DATA_ROOT/<catalog>/<schema>/<table>.csv` instead of a Delta table, so
`./data/prd_mega/indicator/gdp.csv` mirrors `prd_mega.indicator.gdp`. This is how a
counterpart without a Databricks workspace (e.g. Togo) refreshes the indicator tables.

```bash
pip install pandas requests wbgapi openpyxl

export DATA_ROOT=./data        # tables land under ./data/prd_mega/indicator/
# export BUNDLE_TARGET=dev     # optional: mirror the indicator_dev schema instead

python local_runner.py gdp.py consumer_price_index.py population/national_population.py
```

The notebooks are plain Python files; the `# MAGIC %run ./config` / `./utils` cells
that load the shared helpers on Databricks are comments elsewhere, so `local_runner.py`
imports `config` and `utils` itself and runs the notebook with their names in scope.
Any other `%run` is a comment too, so a notebook that needs another helper imports it
under a guard, as `government_revenue_expenditure.py` does for `imf_sdmx`. The pieces
that make this work:

- [config.py](config.py) detects the runtime. On Databricks it resolves the schema
  from the `bundle_target` widget as before; otherwise it reads `DATA_ROOT` (required)
  and `BUNDLE_TARGET` (default `prod`).
- [utils.py](utils.py) provides `read_table`, `write_table`, `table_exists` and
  `versioned_dataframe`, which use Delta tables on Databricks and CSVs locally. A table
  name is either bare (`gdp`, qualified with `INDICATOR_SCHEMA`) or `catalog.schema.table`.
  Notebooks should use these instead of `spark.table` / `saveAsTable`; a notebook that
  still calls `spark` directly has not been converted yet and only runs on Databricks.
- Job widgets such as `census_population_update_version` are read from the upper-cased
  environment variable of the same name (`CENSUS_POPULATION_UPDATE_VERSION=true`), and
  so are secrets (`get_secret("DIMEBOOSTKEYVAULT", "ember_energy_key")` reads `EMBER_ENERGY_KEY`).
- Files a notebook would write to the Unity Catalog volume go under
  `$DATA_ROOT/Volumes/...`, mirroring the Databricks path.

Tables a notebook reads but that are not produced locally (see below) are exported from
Databricks as CSV into the matching path, e.g. `prd_mega.indicator.country` to
`$DATA_ROOT/prd_mega/indicator/country.csv`. Nulls may be blank or `null`. Most
notebooks read `country`, so export it first; `gdp.py` must run before
`education/education_private_spending.py` and `health/health_expenditure.py`.

Tests for the local runtime live in [tests/](tests/) and run offline: `pytest -v`.

### What runs locally

| Notebook | Table(s) written |
|---|---|
| `consumer_price_index.py` | `consumer_price_index` |
| `gdp.py` | `gdp` |
| `population/national_population.py` | `population` |
| `poverty/poverty.py` | `poverty_rate` |
| `education/education_spending_icp.py` | `edu_spending` |
| `education/learning_poverty.py` | `learning_poverty_rate` |
| `education/education_sdg.py` | `youth_literacy_rate_unesco` |
| `education/education_private_spending.py` | `edu_private_spending` (reads `gdp`) |
| `education/education_public_spending.py` | `edu_gov_spending` |
| `education/completion_rates.py`, `pupil_teacher_ratio.py`, `school_basic_services.py`, `teacher_salaries.py` | `completion_rates`, `pupil_teacher_ratio`, `school_basic_services`, `teacher_salaries` |
| `health/health_expenditure.py` | `health_expenditure` (reads `gdp`) |
| `health/sdg_health.py` | `maternal_mortality_ratio_WHO`, `universal_health_coverage_index_GHO` |
| `public_finance/government_revenue_expenditure.py` | `government_revenue_expenditure` |
| `public_finance/togo/togo_finance_report_transform_load_dlt.py` | `togo_revenue_budget` |
| `pefa/pefa_transform_load.py` | `pefa_by_pillar` (reads the hand-uploaded `pefa_2011_bronze`, `pefa_2016_bronze`) |
| `energy/energy_generation_consumption.py` | `energy_generation` (needs `EMBER_ENERGY_KEY`) |
| `public_sector_employment/wwbi_extract.py` | `public_sector_employment_silver` (the gold table is DLT, below) |

### Databricks only

These tables are not produced by the local runtime; export them from Databricks if a
local consumer needs them.

| Table | Why it needs special handling |
|---|---|
| `country` | [country.py](country.py) uses pyspark UDFs over `admin1_boundaries_gold` for map centroids and joins `prd_corpdata.dm_reference_gold.v_dim_country_currency_exchange_rate`. Planned as a hand-written per-country module. |
| `admin1_boundaries_gold`, `admin0_disputed_boundaries_gold` | DLT pipeline ([geo/admin_boundaries_dlt.py](geo/admin_boundaries_dlt.py)) over GeoJSON in the volume. Subnational. |
| `subnational_population` | DLT union of the 18 `population/<ISO3>/` silver notebooks, which also depend on the Global Data Lab R extract and `wb_subnational_population_extract.py`. Subnational. |
| `subnational_poverty_rate` | DLT ([poverty/subnational_poverty/](poverty/subnational_poverty/)) over `poverty_rate_SPID_GSAP_silver`. Subnational. |
| `global_data_lab_hd_index` | R extract ([human_development/global_data_lab_hdi_extract.r](human_development/global_data_lab_hdi_extract.r), needs the `gdldata` R package and `GDL_API_TOKEN`) plus a DLT transform. Subnational. |
| `public_sector_employment` | DLT gold ([public_sector_employment/wwbi_transform_load_dlt.py](public_sector_employment/wwbi_transform_load_dlt.py)) over the silver table above. |
| `indicator_data_availability` | DLT SQL materialized view ([indicator_data_availability_dlt.sql](indicator_data_availability_dlt.sql)) across 12 indicator tables. |
