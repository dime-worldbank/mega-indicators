"""Run the indicator notebooks off Databricks.

The notebooks are plain Python files. On Databricks their `# MAGIC %run ./config` and
`# MAGIC %run ./utils` cells load those two files into the notebook's namespace; off
Databricks every `# MAGIC` line is a comment, so this script imports utils (which
imports config) and runs the notebook with those names in scope.

With no argument it runs NOTEBOOKS below, in order, each in its own process (some
notebooks change module state such as `wb.db`, and on Databricks every task starts
fresh). The inputs listed under "Required inputs" in the README must be in place.

Usage:
    DATA_ROOT=./data python local_runner.py           # everything in NOTEBOOKS
    DATA_ROOT=./data python local_runner.py gdp.py    # one notebook
"""
import runpy
import subprocess
import sys
from pathlib import Path

# The notebooks behind the tables the Togo BOOST aggregate and the dashboard read, in
# dependency order. The other notebooks still run one at a time.
NOTEBOOKS = [
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
    'geo/admin_boundaries_extract.py',  # country.py's map centroids come from these boundaries; downloads the World Bank Admin 1 (254 MB) and Admin 0 (174 MB) GeoJSON files
    'geo/admin_boundaries_transform_load.py',
    'population/TGO/tgo_subnational_population.py',
    'population/subnational_population.py',
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

if __name__ == '__main__':
    from utils import *  # what the %run cells provide on Databricks
    if len(sys.argv) == 1:
        here = Path(__file__).resolve().parent
        for notebook in NOTEBOOKS:
            print(f'==> {notebook}', flush=True)
            if subprocess.run([sys.executable, __file__, str(here / notebook)]).returncode:
                sys.exit(f'{notebook} failed; the notebooks after it were not run.')
    elif len(sys.argv) == 2:
        runpy.run_path(sys.argv[1], init_globals=globals(), run_name='__main__')
    else:
        sys.exit(__doc__)
