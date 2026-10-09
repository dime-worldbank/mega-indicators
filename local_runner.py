"""Run the indicator notebooks off Databricks.

The notebooks are plain Python files. On Databricks their `# MAGIC %run ./config` and
`# MAGIC %run ./utils` cells load those two files into the notebook's namespace; off
Databricks every `# MAGIC` line is a comment, so this script imports utils (which
imports config) and runs the notebook with those names in scope.

With no argument it runs NOTEBOOKS below, in order, each in its own process (some
notebooks change module state such as `wb.db`, and on Databricks every task starts
fresh), then the subnational population step of the country COUNTRY_NAME names (see
notebooks()); without COUNTRY_NAME that step is left out. The inputs listed under
"Required inputs" in the README must be in place.

Usage:
    DATA_ROOT=./data python local_runner.py           # everything in NOTEBOOKS
    DATA_ROOT=./data python local_runner.py gdp.py    # one notebook
    DATA_ROOT=./data python local_runner.py --from health/sdg_health.py
                                                      # resume: that notebook and the ones after it
"""
import os
import runpy
import subprocess
import sys
from pathlib import Path

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
    'poverty/subnational_poverty/subnational_poverty_index_extract_transform.py',
    'poverty/subnational_poverty/subnational_poverty_index_transform_load.py',
    'human_development/global_data_lab_hdi_extract.py',  # needs GDL_API_TOKEN
    'human_development/global_data_lab_hdi_transform_load.py',
    'education/completion_rates.py',  # these four only feed indicator_data_availability
    'education/pupil_teacher_ratio.py',
    'education/school_basic_services.py',
    'education/teacher_salaries.py',
    'indicator_data_availability.py',  # summarises the tables above (the subnational population step below is not among them)
]

# The subnational population step: the shared extract the country's notebook reads, if any,
# population/<ISO3>/<iso3>_subnational_population.py, then the union of the per-country tables.
# It runs for the one country COUNTRY_NAME names, after NOTEBOOKS; without COUNTRY_NAME the
# union would want every country's table, and those are built one country at a time.
SUBNATIONAL_POPULATION_UNION = 'population/subnational_population.py'
WB_SUBNATIONAL_EXTRACT = 'population/wb_subnational_population_extract.py'
GDL_SUBNATIONAL_EXTRACT = 'population/global_data_lab_subnational_population.py'  # needs GDL_API_TOKEN
# ISO3 code of each country with a notebook, by its name in the World Bank API (what
# COUNTRY_NAME is set to, and what write_table filters rows on)
COUNTRIES = {
    'Albania': 'ALB', 'Bangladesh': 'BGD', 'Bhutan': 'BTN', 'Burkina Faso': 'BFA', 'Burundi': 'BDI',
    'Chile': 'CHL', 'Colombia': 'COL', 'Congo, Dem. Rep.': 'COD', 'Ghana': 'GHA', 'Kenya': 'KEN',
    'Liberia': 'LBR', 'Mozambique': 'MOZ', 'Nigeria': 'NGA', 'Pakistan': 'PAK', 'Paraguay': 'PRY',
    'South Africa': 'ZAF', 'Togo': 'TGO', 'Tunisia': 'TUN',
}
# the shared extract a country's notebook reads; the other notebooks fetch their own source
EXTRACTS = {
    'ALB': WB_SUBNATIONAL_EXTRACT, 'BDI': WB_SUBNATIONAL_EXTRACT, 'BTN': WB_SUBNATIONAL_EXTRACT,
    'CHL': WB_SUBNATIONAL_EXTRACT, 'TUN': WB_SUBNATIONAL_EXTRACT, 'ZAF': WB_SUBNATIONAL_EXTRACT,
    'COD': GDL_SUBNATIONAL_EXTRACT, 'LBR': GDL_SUBNATIONAL_EXTRACT,
}

HERE = Path(__file__).resolve().parent


def notebooks():
    """What a full run runs, in order: NOTEBOOKS, then COUNTRY_NAME's subnational population step."""
    country = os.environ.get('COUNTRY_NAME')
    if not country:
        print('COUNTRY_NAME is not set: the subnational population step is left out', flush=True)
        return list(NOTEBOOKS)
    if country not in COUNTRIES:
        sys.exit(f'COUNTRY_NAME={country!r} is not a country with a subnational population notebook. '
                 f'As the World Bank API spells them: {", ".join(COUNTRIES)}')
    code = COUNTRIES[country]
    extract = [EXTRACTS[code]] if code in EXTRACTS else []
    return NOTEBOOKS + extract + [f'population/{code}/{code.lower()}_subnational_population.py', SUBNATIONAL_POPULATION_UNION]


def notebooks_from(start):
    """notebooks() from `start` on, to resume a run that failed there."""
    names = notebooks()
    path = Path(start)
    try:
        name = str(path.resolve().relative_to(HERE)) if path.is_absolute() else str(path)
    except ValueError:
        name = start
    if name not in names:
        sys.exit(f'{start} is not in the list to run, so there is nothing to resume from. The list:\n  ' + '\n  '.join(names))
    return names[names.index(name):]


def run_in_order(names):
    """Each notebook in its own process; stop at the first failure and say how to resume."""
    for notebook in names:
        print(f'==> {notebook}', flush=True)
        if subprocess.run([sys.executable, __file__, str(HERE / notebook)]).returncode:
            sys.exit(f'{notebook} failed; the notebooks after it were not run.\n'
                     f'Once the cause is fixed, resume with: python {Path(__file__).name} --from {notebook}')


if __name__ == '__main__':
    from utils import *  # what the %run cells provide on Databricks
    args = sys.argv[1:]
    if not args:
        run_in_order(notebooks())
    elif args[0] == '--from' and len(args) == 2:
        run_in_order(notebooks_from(args[1]))
    elif len(args) == 1:
        runpy.run_path(args[0], init_globals=globals(), run_name='__main__')
    else:
        sys.exit(__doc__)
