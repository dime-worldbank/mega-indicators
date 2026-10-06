"""Run the Togo indicator scripts.

    python run.py                 # every script in SCRIPTS, in order, each in its own process
    python run.py gdp.py          # just that one

Each script imports utils.py (table IO under DATA_ROOT, API fetchers), so a script in a
sub-folder is run from here rather than directly. The required inputs listed in the
README must be in place first.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# In dependency order: gdp before health_expenditure, the boundaries extract before its
# gold step, the Togo population before the subnational_population step.
SCRIPTS = [
    'gdp.py',
    'consumer_price_index.py',
    'population/national_population.py',
    'poverty/poverty.py',
    'education/education_spending_icp.py',
    'education/learning_poverty.py',
    'health/health_expenditure.py',
    'health/sdg_health.py',
    'pefa/pefa_transform_load.py',  # reads the hand-supplied pefa_2011_bronze / pefa_2016_bronze
    'public_finance/government_revenue_expenditure.py',
    'public_finance/togo/togo_revenue_budget.py',
    'geo/admin_boundaries_extract.py',  # downloads the 254 MB World Bank Admin 1 GeoJSON
    'geo/admin_boundaries_gold.py',
    'population/tgo_subnational_population.py',
    'population/subnational_population_gold.py',
]


def run(script):
    print(f'==> {script}', flush=True)
    env = {**os.environ, 'PYTHONPATH': str(HERE) + os.pathsep + os.environ.get('PYTHONPATH', '')}
    return subprocess.run([sys.executable, str(HERE / script)], cwd=HERE, env=env).returncode


if __name__ == '__main__':
    for script in sys.argv[1:] or SCRIPTS:
        if run(script):
            sys.exit(f'{script} failed; the scripts after it were not run.')
