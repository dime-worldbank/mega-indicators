# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

import unicodedata

import pandas as pd

COUNTRY_NAME = "Moldova"
COUNTRY_CODE = "MDA"

# Population with usual residence by locality (village/commune, town/municipality),
# published by the National Bureau of Statistics of the Republic of Moldova.
# Downloaded to the volume by mda_subnational_population_extract.py.
VOLUME_ROOT_PATH = get_volume_root_path()
NBS_POPULATION_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/population/mda/Populatia_sate_comune_orase_2014-2023.xlsx'
NBS_SOURCE = "National Bureau of Statistics of the Republic of Moldova"

# 32 raions + 2 municipalities (Chisinau, Balti) + the autonomous region of Gagauzia.
# Excludes Transnistria and Bender, which are not covered by the NBS estimates.
EXPECTED_ADM1_COUNT = 35
EXPECTED_YEARS = list(range(2014, 2024))

# Each yearly sheet has 5 header/title rows, then one row per locality, then footnotes.
HEADER_ROWS = 5
COLUMN_NAMES = ['adm1_name_raw', 'locality_code', 'locality_name', 'population', 'male', 'female']
COUNTRY_TOTAL_LABEL = 'total pe tara'
# NBS rounds its commune level estimates, so the localities do not add up to the
# published country total exactly. The gap is a couple of hundred people at most.
COUNTRY_TOTAL_TOLERANCE = 0.001

# adm1 names that do not follow the title cased spelling used by the
# WB subnational population database
ADM1_NAME_OVERRIDES = {
    'CHISINAU': 'Chisinau Municipality',
    'CALARASI': 'Calaras',
}
# Prefixes denoting the type of the adm1 unit: municipality, raion, autonomous region
ADM1_PREFIXES = ('MUN.', 'R-UL', 'UTA')


def remove_accents(input_str: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(input_str))
        if unicodedata.category(c) != 'Mn'
    )


def clean_adm1_name(raw_name: str) -> str:
    name = remove_accents(raw_name).strip().upper()
    for prefix in ADM1_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
            break
    return ADM1_NAME_OVERRIDES.get(name, name.title())

# COMMAND ----------

xls = pd.ExcelFile(NBS_POPULATION_PATH)

# One sheet per year, named after the year
sheet_years = sorted(int(sheet_name) for sheet_name in xls.sheet_names)
assert sheet_years == EXPECTED_YEARS, f'Expected sheets for years {EXPECTED_YEARS}, got {sheet_years}'

# COMMAND ----------

# Aggregate the locality level population up to adm1 (raion/municipality) for each year
yearly_dfs = []
for year in sheet_years:
    df_raw = pd.read_excel(xls, sheet_name=str(year), header=None, skiprows=HEADER_ROWS, names=COLUMN_NAMES)

    country_total_rows = df_raw[df_raw.adm1_name_raw.astype(str).str.strip().str.lower() == COUNTRY_TOTAL_LABEL]
    assert len(country_total_rows) == 1, f'{year}: expected 1 country total row, got {len(country_total_rows)}'
    country_total = int(country_total_rows.population.iloc[0])

    # Localities are the only rows with a locality code; footnotes and blank rows have none
    df_localities = df_raw[df_raw.locality_code.notnull() & df_raw.population.notnull()].copy()
    df_localities['adm1_name'] = df_localities.adm1_name_raw.map(clean_adm1_name)

    df_year = df_localities.groupby('adm1_name', as_index=False).population.sum()
    df_year['year'] = year

    assert df_year.adm1_name.nunique() == EXPECTED_ADM1_COUNT, \
        f'{year}: expected {EXPECTED_ADM1_COUNT} adm1 regions, got {df_year.adm1_name.nunique()}'
    relative_diff = abs(df_year.population.sum() - country_total) / country_total
    assert relative_diff <= COUNTRY_TOTAL_TOLERANCE, \
        f'{year}: aggregated population differs from the published country total by {relative_diff:.2%}'

    yearly_dfs.append(df_year)

df_pop = pd.concat(yearly_dfs, ignore_index=True)
df_pop['country_name'] = COUNTRY_NAME
df_pop['data_source'] = NBS_SOURCE
df_pop['population'] = df_pop.population.round().astype(int)

columns_ordered = ['country_name', 'adm1_name', 'year', 'population', 'data_source']
df_pop = df_pop.reindex(columns=columns_ordered).sort_values(['adm1_name', 'year']).reset_index(drop=True)
df_pop

# COMMAND ----------

expected_rows = EXPECTED_ADM1_COUNT * len(EXPECTED_YEARS)
assert df_pop.shape[0] == expected_rows, f'Expect {expected_rows} rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), \
    f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
assert all(df_pop.population > 0), 'Expect all populations to be positive'

# Check year coverage for each region
for adm1_name, group in df_pop.groupby('adm1_name')['year']:
    assert list(group) == EXPECTED_YEARS, f'Missing years for region {adm1_name}'

# COMMAND ----------

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.mda_subnational_population_silver")
