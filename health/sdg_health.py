# Databricks notebook source
import wbgapi as wb
import pandas as pd

# COMMAND ----------

wb.source.info()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Maternal Mortality Ratio
# MAGIC
# MAGIC Maternal mortality ratio is the number of women who die from pregnancy-related causes while pregnant or within 42 days of pregnancy termination per 100,000 live births. The data are estimated with a regression model using information on the proportion of maternal deaths among non-AIDS deaths in women ages 15-49, fertility, birth attendants, and GDP measured using purchasing power parities (PPPs).
# MAGIC
# MAGIC Source: WHO, UNICEF, UNFPA, World Bank Group, and UNDESA/Population Division. Trends in Maternal Mortality 2000 to 2020. Geneva, World Health Organization, 2023
# MAGIC
# MAGIC #### Universal Health Coverage (composite index)
# MAGIC
# MAGIC Coverage index for essential health services (based on tracer interventions that include reproductive, maternal, newborn and child health, infectious diseases, noncommunicable diseases and service capacity and access). It is presented on a scale of 0 to 100.

# COMMAND ----------

country_df = spark.table(f'prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

wb.db = 16 # Health Nutrition and Population Statistics

outcome_series_to_col_name = {
    'SH.STA.MMRT': ['maternal_mortality_ratio', 'WHO', 'maternal_mortality_ratio_WHO'], # SDG 3.1.1
    'SH.UHC.SRVS.CV.XD':['universal_health_coverage_index', 'Global Health Observatory', 'universal_health_coverage_index_GHO'] # SDG 3.8.1
}

for key, val in outcome_series_to_col_name.items():
    outcome_df = wb.data.DataFrame([key], skipBlanks=True).reset_index()
    # quick check of data availability
    countries = ['COL', 'PRY', 'KEN', 'MOZ', 'BFA', 'PAK', 'COD']
    print(outcome_df[outcome_df.economy.isin(countries)])

    indicator_name, data_source, db_name = val
    long_df = outcome_df.melt(id_vars='economy', var_name='year', value_name=indicator_name)
    long_df['year'] = long_df['year'].str.replace('YR', '')
    long_df = long_df.astype({'year': 'int'})
    long_df['data_source'] = data_source
    long_df = long_df.dropna(subset=[indicator_name]).sort_values(by=['economy', 'year'])

    df_indicator = pd.merge(long_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', 'year', indicator_name, 'data_source']]
    sdf = spark.createDataFrame(df_indicator)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'prd_mega.indicator.{db_name}')

