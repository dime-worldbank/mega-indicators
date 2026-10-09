# mega-indicators
A collection of notebooks to fetch and store indicator datasets

## Deployment

The jobs are defined as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/)
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


Prod is bound to the existing jobs (no duplicates) and deploys to the team's
`/Workspace/Repos/boostprocessed` folder, with `CAN_MANAGE` granted to the
`ITSDA-LKHS-DAP-PROD-boostprocessed` group. The GDL token is read from the existing
`DIMEBOOSTKEYVAULT` secret scope — no setup needed.

The DLT pipelines were replaced by notebooks (`admin_boundaries_transform_load.py`,
`subnational_poverty_index_transform_load.py`, `global_data_lab_hdi_transform_load.py`,
`subnational_population.py`, `wwbi_transform_load.py`, `indicator_data_availability.py`).
The first deploy of this version deletes the pipelines, and Unity Catalog drops the tables a
deleted pipeline owned; the notebooks recreate them as plain Delta tables when their jobs run,
and a notebook that runs while a pipeline still owns its table cannot overwrite it. So right
after that deploy run `indicators_on_demand` (it has no schedule), then `indicators_weekly` and
`indicators_monthly`, before the dashboard is next read:

```bash
databricks bundle run indicators_on_demand -t prod -p RPF-ADBSvc-PROD
```

## Contributing

To add more indicators, please open a pull request after you've tested your code in Databricks.

- See [consumer_price_index.py](consumer_price_index.py) as a Python example of fetching data from WB API
- See [global_data_lab_hdi_extract.py](human_development/global_data_lab_hdi_extract.py) for a source that needs an API token (`GDL_API_TOKEN`: the `DIMEBOOSTKEYVAULT` secret on Databricks, the environment variable of the same name otherwise; get one at [globaldatalab.org](https://globaldatalab.org)).
- If your source is a single external site (a national stats agency, etc.) rather than a
  well-established API, fetch it through `utils.py`'s `versioned_dataframe`/`fetch_raw`
  instead of calling `requests`/`pd.read_csv` directly — it caches the parsed result as a
  Delta table, so a temporarily unreachable source serves last-known-good data instead of
  failing the pipeline. See [pry_subnational_population.py](population/PRY/pry_subnational_population.py)
  for a CSV example and [alb_subnational_population.py](population/ALB/alb_subnational_population.py)
  for Excel (`parse=`).
- For an API, call `utils.py`'s `http_get` rather than `requests.get`: it retries connection
  errors, timeouts, responses cut short and 429/5xx answers with a backoff, which these
  sources produce now and then.
- Read and write tables through `utils.py`'s `read_table` / `write_table` (and
  `table_exists`, `get_secret`), never `spark.table` / `saveAsTable` / `dbutils` directly: on
  Databricks they are Delta tables in `INDICATOR_SCHEMA`, off it CSVs under `DATA_ROOT`, so the
  same notebook runs in both. A test fails if a notebook calls spark or dbutils.

## Running without Databricks

The notebooks also run as plain Python, with each table stored as a CSV at
`$DATA_ROOT/<catalog>/<schema>/<table>.csv` instead of a Delta table, so
`./data/prd_mega/indicator/gdp.csv` mirrors `prd_mega.indicator.gdp`. This is how a
counterpart without a Databricks workspace (e.g. Togo) refreshes the indicator tables.

```bash
pip install pandas requests wbgapi openpyxl shapely

export DATA_ROOT=./data        # tables land under ./data/prd_mega/indicator/
export GDL_API_TOKEN=...       # the Global Data Lab notebooks need it; get one at https://globaldatalab.org
export COUNTRY_NAME=Togo       # optional: keep only this country's rows in every table written
# export BUNDLE_TARGET=dev     # optional: mirror the indicator_dev schema instead
# export EMBER_ENERGY_KEY=...  # only for energy/energy_generation_consumption.py, which the runner does not include

python local_runner.py           # the notebooks in NOTEBOOKS, in order
python local_runner.py gdp.py    # one notebook
python local_runner.py --from health/sdg_health.py   # resume: that notebook and the ones after it
```

The notebooks are plain Python files; the `# MAGIC %run ./config` / `./utils` cells
that load the shared helpers on Databricks are comments elsewhere, so `local_runner.py`
imports `config` and `utils` itself and runs the notebook with their names in scope.
Any other `%run` is a comment too, so a notebook that needs another helper imports it
under a guard, as `government_revenue_expenditure.py` does for `imf_sdmx`.

With no argument the runner refreshes the notebooks listed in its `NOTEBOOKS`, which
are the ones behind the tables the Togo BOOST aggregate and the dashboard read, each
in its own process. Any other notebook runs one at a time. The run stops at the first
notebook that fails; fix the cause and continue from that notebook with `--from`, the
tables already written are kept. The subnational population step is the one place the
runner looks at `COUNTRY_NAME`: it runs the shared extract that country's notebook reads
(`wb_subnational_population_extract.py` or `global_data_lab_subnational_population.py`, if any),
then the country's own notebook, `population/<ISO3>/<iso3>_subnational_population.py`
(the code looked up in `country`), then the union; without `COUNTRY_NAME` it skips the step,
since the union would then want every listed country's table and those are built one country
at a time. `COUNTRY_NAME` is the World Bank API's spelling (`Congo, Dem. Rep.`), the one the
`country` table carries; the runner checks it before writing anything, as a name matching no
row would leave every table empty. The pieces that make this work:

- [config.py](config.py) detects the runtime. On Databricks it resolves the schema
  from the `bundle_target` widget as before; otherwise it reads `DATA_ROOT` (required),
  `BUNDLE_TARGET` (default `prod`) and `COUNTRY_NAME` (optional: `write_table` then keeps
  only that country's rows of any table with a `country_name` column).
- [utils.py](utils.py) provides `read_table`, `write_table`, `table_exists` and
  `versioned_dataframe`, which use Delta tables on Databricks and CSVs locally. A table
  name is either bare (`gdp`, qualified with `INDICATOR_SCHEMA`) or `catalog.schema.table`.
- Job widgets such as `census_population_update_version` are read from the upper-cased
  environment variable of the same name (`CENSUS_POPULATION_UPDATE_VERSION=true`), and
  so are secrets: `get_secret("DIMEBOOSTKEYVAULT", "GDL_API_TOKEN")` reads `GDL_API_TOKEN`
  and `get_secret("DIMEBOOSTKEYVAULT", "ember_energy_key")` reads `EMBER_ENERGY_KEY`. A
  notebook that needs a secret stops with a message naming the variable when it is unset.
- Any country's population notebook under `population/<ISO3>/` also runs on its own
  (`COUNTRY_NAME` unset, or set to that country), after `population/wb_subnational_population_extract.py`
  for the ones that read the World Bank subnational database and
  `population/global_data_lab_subnational_population.py` (needs `GDL_API_TOKEN`) for Congo DR and Liberia.
- Files a notebook would write to the Unity Catalog volume (downloaded GeoJSON, PDFs)
  go under `$DATA_ROOT/raw_data/`.

Tests for the local runtime live in [tests/](tests/) and run offline: `pytest -v`.

### Required inputs

The PEFA scores cannot be fetched from an API. Before running, put the two tables in the table store
as CSV files named after the table, at `$DATA_ROOT/prd_mega/indicator/<table>.csv`;
with `DATA_ROOT=./data` that is `./data/prd_mega/indicator/pefa_2016_bronze.csv`. Nulls may be
blank or `null`.

| Table | Where it comes from | Columns |
|---|---|---|
| `pefa_2016_bronze` | [pefa.org](https://www.pefa.org/assessments/batch-downloads), Assessments, Batch downloads: Framework "2016 Framework", Country Togo, Type National, Status Final, Download. Save as CSV with the header as downloaded. | `Country`, `Year`, `Framework`, `PI-01` to `PI-31`; other columns are ignored |
| `pefa_2011_bronze` | Same, with Framework "2011 Framework". | `Country`, `Year`, `Framework`, `PI-01` to `PI-28` |

### What runs locally

| Notebook | Table(s) written |
|---|---|
| `geo/admin_boundaries_extract.py` then `geo/admin_boundaries_transform_load.py` | `admin1_boundaries_bronze`/`_silver`/`_gold` and `admin0_disputed_boundaries_bronze`/`_silver`/`_gold`, with the region-name corrections and the Albania and Ghana polygon unions. The extract downloads two GeoJSON files, 254 MB and 174 MB; loading them takes about 3 GB of memory. |
| `country.py` | `country` (World Bank API metadata; map centroids from `admin1_boundaries_gold`, so the boundaries notebooks run first; currency from the corporate table where reachable, else a dictionary in the notebook) |
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
| `public_sector_employment/wwbi_extract.py` then `public_sector_employment/wwbi_transform_load.py` | `public_sector_employment_silver`, then `public_sector_employment` (with regional means) |
| `population/<ISO3>/<iso3>_subnational_population.py` (the runner picks it from `COUNTRY_NAME`) then `population/subnational_population.py` | `<iso3>_subnational_population_silver`, then `subnational_population` (the countries listed in the notebook stacked; with `COUNTRY_NAME` set, only the silver tables present) |
| `poverty/subnational_poverty/subnational_poverty_index_extract_transform.py` then `subnational_poverty_index_transform_load.py` | `poverty_rate_SPID_GSAP_silver`, then `subnational_poverty_rate` |
| `human_development/global_data_lab_hdi_extract.py` then `global_data_lab_hdi_transform_load.py` | `global_data_lab_hd_index_bronze`, `global_data_lab_hd_index_silver`, then `global_data_lab_hd_index` (needs `GDL_API_TOKEN`; about 70 API calls) |
| `indicator_data_availability.py` (last) | `indicator_data_availability`: earliest and latest year per indicator and country, for the dashboard's source notes |
