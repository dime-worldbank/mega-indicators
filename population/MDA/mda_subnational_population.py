# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../geo/admin1_harmonization

# COMMAND ----------

import unicodedata

import pandas as pd

COUNTRY_NAME = "Moldova"
COUNTRY_CODE = "MDA"

# NBS population by locality, one cumulative workbook with a sheet per year, downloaded by
# mda_subnational_population_extract.py. A new edition replaces it: update NBS_FILENAME and
# LAST_YEAR here and FILENAME there.
NBS_FILENAME = 'Populatia_sate_comune_orase_2014-2023.xlsx'
NBS_POPULATION_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/population/mda/{NBS_FILENAME}'
NBS_SOURCE = "National Bureau of Statistics of the Republic of Moldova"

# 32 raions, Chisinau, Balti and Gagauzia; Transnistria and Bender are not in the NBS estimates
EXPECTED_ADM1_COUNT = len(MOLDOVA_UNIT_TO_REGION)
FIRST_YEAR, LAST_YEAR = 2014, 2023  # as the filename says
assert f'{FIRST_YEAR}-{LAST_YEAR}' in NBS_FILENAME, 'FIRST_YEAR/LAST_YEAR must match the range in NBS_FILENAME'
EXPECTED_YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))

# Each yearly sheet has 5 header/title rows, then one row per locality, then footnotes.
HEADER_ROWS = 5
COLUMN_NAMES = ['adm1_name_raw', 'locality_code', 'locality_name', 'population', 'male', 'female']
COUNTRY_TOTAL_LABEL = 'total pe tara'
# NBS rounds its commune level estimates, so the localities do not add up to the
# published country total exactly. The gap is a couple of hundred people at most.
COUNTRY_TOTAL_TOLERANCE = 0.001

# adm1 names that do not follow the title cased spelling used by the
# WB subnational population database. Keyed on the upper cased name, which is what
# clean_adm1_name has in hand at the point it consults this map.
ADM1_NAME_OVERRIDES = {
    'GAGAUZIA': 'Unitate Teritoriala Autonoma Gagauzia',
}
# Prefixes denoting the type of the adm1 unit: municipality, raion, autonomous region
ADM1_PREFIXES = ('MUN.', 'R-UL', 'UTA')

# BOOST reports Moldova at the four statistical regions, so raions are rolled up with the
# grouping shared with the boundaries notebook (clean_adm1_name yields its spellings).
ADM1_TO_REGION = MOLDOVA_UNIT_TO_REGION
EXPECTED_REGION_COUNT = len(MOLDOVA_REGIONS)


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

    # Roll the raions up to the four regions. Mapping every raion is required rather than
    # best effort: an unmapped one would silently drop its population from the region total.
    unmapped = sorted(set(df_year.adm1_name) - set(ADM1_TO_REGION))
    assert not unmapped, f'{year}: raions missing from ADM1_TO_REGION: {unmapped}'
    df_year['adm1_name'] = df_year.adm1_name.map(ADM1_TO_REGION)
    df_year = df_year.groupby(['adm1_name', 'year'], as_index=False).population.sum()
    assert df_year.adm1_name.nunique() == EXPECTED_REGION_COUNT, \
        f'{year}: expected {EXPECTED_REGION_COUNT} regions, got {df_year.adm1_name.nunique()}'

    yearly_dfs.append(df_year)

df_pop = pd.concat(yearly_dfs, ignore_index=True)
df_pop['country_name'] = COUNTRY_NAME
df_pop['data_source'] = NBS_SOURCE
df_pop['population'] = df_pop.population.round().astype(int)

columns_ordered = ['country_name', 'adm1_name', 'year', 'population', 'data_source']
df_pop = df_pop.reindex(columns=columns_ordered).sort_values(['adm1_name', 'year']).reset_index(drop=True)
df_pop

# COMMAND ----------

expected_rows = EXPECTED_REGION_COUNT * len(EXPECTED_YEARS)
assert df_pop.shape[0] == expected_rows, f'Expect {expected_rows} rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), \
    f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
assert all(df_pop.population > 0), 'Expect all populations to be positive'

# Check year coverage for each region
for adm1_name, group in df_pop.groupby('adm1_name')['year']:
    assert list(group) == EXPECTED_YEARS, f'Missing years for region {adm1_name}'

# COMMAND ----------

write_table(df_pop, 'mda_subnational_population_silver')
