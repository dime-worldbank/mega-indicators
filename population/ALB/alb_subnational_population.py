# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

import pandas as pd
import unicodedata

COUNTRY_NAME = "Albania"
COUNTRY_CODE = "ALB"

INSTAT_2018_2023_URL = "https://www.instat.gov.al/media/9831/tab2.xlsx"
INSTAT_2024_LATER_URL = "https://www.instat.gov.al/media/qqofjboc/popullsia-m%C3%AB-1-janar-sipas-qarkut-dhe-gjinis%C3%AB.xlsx"

# Per-row provenance — the documented exception to the drop-data_source rule: rows
# come from different upstream sources and the 2017 rows are imputed, so no single
# table-level source describes them. Kept as clean source_id codes (not free text).
WB_SUBNATIONAL_POPULATION_SOURCE_ID = "wb_subnational_population"
INSTAT_SOURCE_ID = "alb_instat"
IMPUTED_SOURCE_ID = "imputed"
EXPECTED_ADM1_COUNT = 12

def remove_accents(input_str: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(input_str))
        if unicodedata.category(c) != 'Mn'
    )

# Extract 2016 and earlier data from WB subnational Population — shared download+parse
# across countries (wb_subnational_population_extract.py).
df_wb_long = (
    spark.table(f'{INDICATOR_SCHEMA}.wb_subnational_population_silver')
    .where(f"country_code = '{COUNTRY_CODE}'")
    .drop('country_code')
    .toPandas()
)
df_wb_long['country_name'] = COUNTRY_NAME
df_wb_long['source_id'] = WB_SUBNATIONAL_POPULATION_SOURCE_ID

assert df_wb_long.shape[0] >= 204, f'Expect at least 204 rows, got {df_wb_long.shape[0]}'
assert all(df_wb_long.population.notnull()), f'Expect no missing values in population field, got {sum(df_wb_long.population.isnull())} null values'
assert df_wb_long.adm1_name.nunique() == EXPECTED_ADM1_COUNT, f'Expected {EXPECTED_ADM1_COUNT} counties, got {df_wb_long.adm1_name.nunique()}'

# COMMAND ----------

def _flatten_excel_headers(buf, **kwargs):
    """pd.read_excel's MultiIndex header as single 'year::category' strings —
    Delta can't store tuple column names."""
    df = pd.read_excel(buf, sheet_name=0, header=[3, 4], **kwargs)
    df.columns = ['::'.join(str(level) for level in col) for col in df.columns]
    return df

def _select_total_by_year(df, lo, hi):
    """admin1 column + the 'total' column for each year in [lo, hi]."""
    adm1_name_column = [c for c in df.columns if 'prefectures' in c.lower()][0]
    year_columns = [
        (int(c.split('::')[0]), c) for c in df.columns
        if c.split('::')[0].isdigit()
        and lo <= int(c.split('::')[0]) <= hi
        and 'total' in c.lower()
    ]
    selected = df[[adm1_name_column] + [c for _, c in year_columns]].copy()
    selected.columns = ['adm1_name'] + [year for year, _ in year_columns]
    return selected

update_version = update_version_flag('alb_population_update_version')

# Extract the data from 2018 to 2023
df_instat = versioned_dataframe(INSTAT_2018_2023_URL, 'alb_instat_2018_2023_raw', update_version,
                                 parse=_flatten_excel_headers)
df_instat = _select_total_by_year(df_instat, 2018, 2023)

df_instat = (
    df_instat
    .dropna()
    .loc[lambda df: ~df['adm1_name'].str.contains('total', case=False, na=False)]
    .assign(adm1_name=lambda df: df['adm1_name'].apply(remove_accents))
)

df_instat_long = df_instat.melt(
    id_vars=['adm1_name'], 
    var_name='year', 
    value_name='population'
)

df_instat_long['country_name'] = COUNTRY_NAME
df_instat_long['source_id'] = INSTAT_SOURCE_ID

# COMMAND ----------

# Extract the data from 2024 and later
df_instat_2024 = versioned_dataframe(INSTAT_2024_LATER_URL, 'alb_instat_2024_later_raw', update_version,
                                      parse=_flatten_excel_headers)
df_instat_2024 = _select_total_by_year(df_instat_2024, 2024, 9999)

df_instat_2024 = (
    df_instat_2024
    .dropna()
    .loc[lambda df: ~df['adm1_name'].str.contains('total', case=False, na=False)]
    .assign(adm1_name=lambda df: df['adm1_name'].apply(remove_accents))
)

df_instat_2024_long = df_instat_2024.melt(
    id_vars=['adm1_name'], 
    var_name='year', 
    value_name='population'
)

df_instat_2024_long['country_name'] = COUNTRY_NAME
df_instat_2024_long['source_id'] = INSTAT_SOURCE_ID
df_instat_long = pd.concat([df_instat_long, df_instat_2024_long], ignore_index=True)

assert all(df_instat_long.population.notnull()), f'Expected no missing values in population field, got {sum(df_instat_long.population.isnull())} null values'
assert df_instat_long.adm1_name.nunique() == EXPECTED_ADM1_COUNT, f'Expected {EXPECTED_ADM1_COUNT} counties, got {df_instat_long.adm1_name.nunique()}'

# COMMAND ----------

# combine the two data sources and impute the values for the missing year 2017
df = pd.concat([df_wb_long, df_instat_long])

df['year'] = pd.to_numeric(df['year'], errors='coerce')

pivot_df = df[df['year'].isin([2016, 2018])].pivot_table(
    index='adm1_name', 
    columns='year', 
    values='population'
)

pivot_df[2017] = ((pivot_df[2016] + pivot_df[2018]) / 2)

imputed_df = pivot_df.reset_index().melt(
    id_vars='adm1_name', 
    value_vars=[2016, 2017, 2018], 
    var_name='year', 
    value_name='population'
)
imputed_df.loc[imputed_df['year'] == 2017, 'country_name'] = COUNTRY_NAME
imputed_df.loc[imputed_df['year'] == 2017, 'source_id'] = IMPUTED_SOURCE_ID

df_pop = pd.concat([df, imputed_df], ignore_index=True).drop_duplicates(subset=['adm1_name', 'year'])

df_pop = df_pop.sort_values(['adm1_name', 'year'])
df_pop['population'] = df_pop['population'].astype(int)

# COMMAND ----------

database_name = INDICATOR_SCHEMA

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.alb_subnational_population_silver")
