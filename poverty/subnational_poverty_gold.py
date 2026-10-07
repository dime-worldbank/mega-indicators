from utils import *

import numpy as np
import pandas as pd

# Subnational poverty rate per region and year from the SPID/GSAP silver table: the
# poverty line follows the country's income group, as for the national poverty_rate.
# (On Databricks a DLT pipeline also applies per-country region-name fixes so regions
# match admin1_boundaries_gold; Togo's names already match.)
silver = read_table('poverty_rate_SPID_GSAP_silver')
countries = read_table('country', columns=['country_name', 'country_code', 'income_level'])
df = silver.merge(countries, on='country_code', how='inner')

df['poverty_rate'] = np.select(
    [df['income_level'].isin(['LIC', 'INX']), df['income_level'] == 'LMC', df['income_level'].isin(['UMC', 'HIC'])],
    [df['poor300'], df['poor420'], df['poor830']],
    np.nan,
)
assert df['poverty_rate'].notna().all(), 'poverty rates for the country income level should be present'

by_region = df.groupby(['country_name', 'region_name'])['year']
df['earliest_year'] = by_region.transform('min')
df['latest_year'] = by_region.transform('max')

write_table(df, 'subnational_poverty_rate')
