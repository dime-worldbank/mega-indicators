# Databricks notebook source
# TODO: Add PEFA score extraction step. Currently the data is imported manually for prototyping
# Data source: https://www.pefa.org/assessments/batch-downloads 
# Download once for 2016 framework then uploaded to prd_mega.indicator_intermediate.pefa_2016_bronze, 
#      and once for 2011 framework then uploaded to prd_mega.indicator_intermediate.pefa_2011_bronze

import pandas as pd
from sklearn.preprocessing import StandardScaler
import re

SCORE_MAPPING = {
    "A": 4, "A+": 4.5,
    "B": 3, "B+": 3.5,
    "C": 2, "C+": 2.5,
    "D": 1, "D+": 1.5
}

PILLAR_MAPPING_2016 = {
    'pillar1_budget_reliability': ['PI-01', 'PI-02', 'PI-03'],
    'pillar2_transparency': ['PI-04', 'PI-05', 'PI-06', 'PI-07', 'PI-08', 'PI-09'],
    'pillar3_asset_liability': ['PI-10', 'PI-11', 'PI-12', 'PI-13'],
    'pillar4_policy_based_budget': ['PI-14', 'PI-15', 'PI-16', 'PI-17', 'PI-18'],
    'pillar5_predictability_and_control': ['PI-19', 'PI-20', 'PI-21', 'PI-22', 'PI-23', 'PI-24', 'PI-25', 'PI-26'],
    'pillar6_accounting_and_reporting': ['PI-27', 'PI-28', 'PI-29'],
    'pillar7_external_audit': ['PI-30', 'PI-31']
}

PILLAR_MAPPING_2011 = {
    'pillar1_budget_reliability': ['PI-01', 'PI-02', 'PI-03', 'PI-04'],
    'pillar2_transparency': ['PI-05', 'PI-06', 'PI-07', 'PI-08', 'PI-09', 'PI-10'],
    'pillar3_asset_liability': [],
    'pillar4_policy_based_budget': ['PI-11', 'PI-12'],
    'pillar5_predictability_and_control': [
        'PI-13', 'PI-14', 'PI-15', 'PI-16', 'PI-17', 'PI-18', 'PI-19', 'PI-20', 'PI-21'
    ],
    'pillar6_accounting_and_reporting': ['PI-22', 'PI-23', 'PI-24', 'PI-25'],
    'pillar7_external_audit': ['PI-26', 'PI-27', 'PI-28']
}

COUNTRY_NAME_MAPPING = {
    'The Gambia': 'Gambia, The',
    'Macedonia': 'North Macedonia',
    'Democratic Republic of Congo': 'Congo, Dem. Rep.',
    'The Bahamas': 'Bahamas, The',
}

INTERMEDIATE_SCHEMA = 'prd_mega.indicator_intermediate'

def process_pefa_silver(year):
    pefa_data = spark.table(f'{INTERMEDIATE_SCHEMA}.pefa_{year}_bronze').toPandas()

    # Only apply the clean_score to top level PI columns
    pi_columns = [col for col in pefa_data.columns if re.fullmatch(r'PI-\d{2}', col)]
    pefa_data[pi_columns] = pefa_data[pi_columns].applymap(clean_score)

    # Map scores & country
    pefa_data[pi_columns] = pefa_data[pi_columns].applymap(SCORE_MAPPING.get)
    pefa_data['country_name'] = pefa_data['Country'].map(COUNTRY_NAME_MAPPING).fillna(pefa_data['Country'])

    # Select relevant columns
    columns_to_keep = ['country_name', 'Year', 'Framework'] + pi_columns
    pefa_data = pefa_data[columns_to_keep]

    # Calculate the average score for each pillar
    for pillar_name, indicators in get_pillar_mapping(year).items():
        relevant_columns = [col for col in indicators if col in pefa_data.columns]
        pefa_data[pillar_name] = pefa_data[relevant_columns].mean(axis=1)

    return pefa_data

def get_pillar_mapping(year):
    return {
        2016: PILLAR_MAPPING_2016,
        2011: PILLAR_MAPPING_2011,
    }[year]

def clean_score(score):
    if pd.isnull(score):
        return None
    # Coerce A*, B*, etc.
    return re.sub(r'[^A-Za-z+]', '', str(score))

# COMMAND ----------

def write_pefa_silver_table(df, year):
    sdf = spark.createDataFrame(df)
    sdf.write.mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"{INTERMEDIATE_SCHEMA}.pefa_{year}_silver")
    return sdf

# COMMAND ----------

pefa_silver_2011 = process_pefa_silver(2011)
write_pefa_silver_table(pefa_silver_2011, 2011)

# COMMAND ----------

pefa_silver_2016 = process_pefa_silver(2016)
write_pefa_silver_table(pefa_silver_2016, 2016)

# COMMAND ----------

columns_to_keep = ['country_name', 'Year', 'Framework'] + list(PILLAR_MAPPING_2016.keys())
pefa_gold = pd.concat([pefa_silver_2011[columns_to_keep], pefa_silver_2016[columns_to_keep]], axis=0, ignore_index=True)
pefa_gold = pefa_gold.rename(columns={'Year': 'year', 'Framework': 'framework'})
pefa_gold

# COMMAND ----------

sdf = spark.createDataFrame(pefa_gold)
sdf.write.mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("prd_mega.indicator.pefa_by_pillar")
