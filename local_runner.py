"""Run the indicator notebooks off Databricks.

The notebooks are plain Python files. On Databricks their `# MAGIC %run ./config` and
`# MAGIC %run ./utils` cells load those two files into the notebook's namespace; off
Databricks every `# MAGIC` line is a comment, so this script imports utils (which
imports config) and runs the notebook with those names in scope.

With no argument it runs NOTEBOOKS below, in order, each in its own process (some
notebooks change module state such as `wb.db`, and on Databricks every task starts
fresh). The inputs listed under "Required inputs" in the README must be in place.
COUNTRY_NAME picks the per-country subnational population notebook (see
subnational_population_notebook) and the shared extract it reads; without it that step
is skipped.

Usage:
    DATA_ROOT=./data python local_runner.py           # everything in NOTEBOOKS
    DATA_ROOT=./data python local_runner.py gdp.py    # one notebook
    DATA_ROOT=./data python local_runner.py --from health/sdg_health.py
                                                      # resume: that notebook and the ones after it
"""
import re
import runpy
import subprocess
import sys
from pathlib import Path

# Stands in NOTEBOOKS for the COUNTRY_NAME country's own notebook; subnational_population_notebook() resolves it.
SUBNATIONAL_POPULATION = 'population/<ISO3>/<iso3>_subnational_population.py'
SUBNATIONAL_POPULATION_UNION = 'population/subnational_population.py'
# The shared extract a country's notebook reads, run just before it; the other countries
# fetch their own source (census.gov, a national statistics file).
SUBNATIONAL_POPULATION_EXTRACTS = {
    'population/wb_subnational_population_extract.py': ['ALB', 'BDI', 'BTN', 'CHL', 'TUN', 'ZAF'],
    'population/global_data_lab_subnational_population.py': ['COD', 'LBR'],  # needs GDL_API_TOKEN
}

# The notebooks behind the tables the Togo BOOST aggregate and the dashboard read, in
# dependency order. The other notebooks still run one at a time.
NOTEBOOKS = [
    'geo/admin_boundaries_extract.py',  # first: country.py's map centroids come from these boundaries; downloads the World Bank Admin 1 (254 MB) and Admin 0 (174 MB) GeoJSON files
    'geo/admin_boundaries_transform_load.py',
    'country.py',  # every other notebook joins to it
    'gdp.py',
    'consumer_price_index.py',
    'population/national_population.py',
    'poverty/poverty.py',
    'education/education_spending_icp.py',
    'education/learning_poverty.py',
    'health/health_expenditure.py',  # reads gdp
    'health/sdg_health.py',
    'pefa/pefa_transform_load.py',  # reads the hand-uploaded pefa_2011_bronze / pefa_2016_bronze
    'public_finance/government_revenue_expenditure.py',
    'public_finance/togo/togo_finance_report_transform_load_dlt.py',
    SUBNATIONAL_POPULATION,  # the COUNTRY_NAME country's; without COUNTRY_NAME this and the union below are skipped
    SUBNATIONAL_POPULATION_UNION,  # stacks the per-country silver tables (with COUNTRY_NAME set, the ones present)
    'poverty/subnational_poverty/subnational_poverty_index_extract_transform.py',
    'poverty/subnational_poverty/subnational_poverty_index_transform_load.py',
    'human_development/global_data_lab_hdi_extract.py',  # needs GDL_API_TOKEN
    'human_development/global_data_lab_hdi_transform_load.py',
    'education/completion_rates.py',  # these four only feed indicator_data_availability
    'education/pupil_teacher_ratio.py',
    'education/school_basic_services.py',
    'education/teacher_salaries.py',
    'indicator_data_availability.py',  # last: summarises the tables above
]

HERE = Path(__file__).resolve().parent


def check_country_name():
    """Stop before anything is written when COUNTRY_NAME names no economy: write_table would
    keep no rows, and every table would come out empty. The names are the World Bank API's
    (e.g. "Congo, Dem. Rep."), which country.py writes as country_name."""
    import utils
    if utils.COUNTRY_NAME and utils.COUNTRY_NAME not in set(utils.wb.economy.DataFrame()['name']):
        sys.exit(f'COUNTRY_NAME={utils.COUNTRY_NAME!r} is not a World Bank economy name (the country table\'s country_name, '
                 f'e.g. "Congo, Dem. Rep."); nothing was written')


def subnational_population_notebook():
    """The COUNTRY_NAME country's own notebook, population/<ISO3>/<iso3>_subnational_population.py,
    or None when COUNTRY_NAME is not set. The ISO3 code is looked up in the country table, so
    country.py must have run."""
    import utils  # here rather than at the top: it needs DATA_ROOT, and the tests re-import it per environment
    if not utils.COUNTRY_NAME:
        return None
    if not utils.table_exists('country'):
        sys.exit('COUNTRY_NAME is set but the country table is not built yet; run country.py first')
    countries = utils.read_table('country', columns=['country_name', 'country_code'])
    codes = countries.loc[countries['country_name'] == utils.COUNTRY_NAME, 'country_code']
    if codes.empty:
        sys.exit(f'COUNTRY_NAME={utils.COUNTRY_NAME!r} is not a country_name in the country table')
    code = codes.iloc[0]
    return f'population/{code}/{code.lower()}_subnational_population.py'


def resolve(entry):
    """The notebooks to run for an entry of NOTEBOOKS, in order; empty to skip it. The two
    subnational population entries need COUNTRY_NAME: without it the union would want every
    listed country's table, and those are built one country at a time. The country's own
    notebook comes after the shared extract it reads, if any."""
    if entry not in (SUBNATIONAL_POPULATION, SUBNATIONAL_POPULATION_UNION):
        return [entry]
    country_notebook = subnational_population_notebook()
    if country_notebook is None:
        return []
    if entry == SUBNATIONAL_POPULATION_UNION:
        return [entry]
    code = country_notebook.split('/')[1]
    return [extract for extract, codes in SUBNATIONAL_POPULATION_EXTRACTS.items() if code in codes] + [country_notebook]


def notebooks_from(start=None):
    """NOTEBOOKS, or its tail from `start` on, to resume a run that failed there."""
    if start is None:
        return list(NOTEBOOKS)
    path = Path(start)
    try:
        name = str(path.resolve().relative_to(HERE)) if path.is_absolute() else str(path)
    except ValueError:
        name = start
    if name in SUBNATIONAL_POPULATION_EXTRACTS or re.fullmatch(r'population/[A-Z]{3}/[a-z]{3}_subnational_population\.py', name):
        country_notebook = subnational_population_notebook()
        if country_notebook is None:
            sys.exit(f"{start} is a country's subnational population step: set COUNTRY_NAME to that country to resume from it")
        if name not in resolve(SUBNATIONAL_POPULATION):
            sys.exit(f"{start} is not COUNTRY_NAME's notebook, {country_notebook}")
        name = SUBNATIONAL_POPULATION
    if name not in NOTEBOOKS:
        sys.exit(f'{start} is not in NOTEBOOKS, so there is nothing to resume from. The list:\n  '
                 + '\n  '.join(NOTEBOOKS))
    return NOTEBOOKS[NOTEBOOKS.index(name):]


def run_in_order(entries):
    """Each notebook in its own process; stop at the first failure and say how to resume."""
    for entry in entries:
        notebooks = resolve(entry)
        if not notebooks:
            print(f'==> {entry}: skipped, COUNTRY_NAME is not set', flush=True)
            continue
        for notebook in notebooks:
            print(f'==> {notebook}', flush=True)
            if subprocess.run([sys.executable, __file__, str(HERE / notebook)]).returncode:
                sys.exit(f'{notebook} failed; the notebooks after it were not run.\n'
                         f'Once the cause is fixed, resume with: python {Path(__file__).name} --from {notebook}')


if __name__ == '__main__':
    from utils import *  # what the %run cells provide on Databricks
    args = sys.argv[1:]
    if not args:
        check_country_name()
        run_in_order(notebooks_from())
    elif args[0] == '--from' and len(args) == 2:
        check_country_name()
        run_in_order(notebooks_from(args[1]))
    elif len(args) == 1:
        runpy.run_path(args[0], init_globals=globals(), run_name='__main__')
    else:
        sys.exit(__doc__)
