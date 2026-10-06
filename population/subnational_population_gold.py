from utils import *

# Writes subnational_population from the per-country silver tables; for Togo that is
# just tgo_subnational_population_silver. (On Databricks a DLT pipeline unions 18 countries.)
import pandas as pd

silver_tables = ['tgo_subnational_population_silver']
write_table(pd.concat([read_table(name) for name in silver_tables], ignore_index=True), 'subnational_population')
