# Databricks notebook source

import pandas as pd

# Togo state budget execution metrics, in whole CFA francs.
# Source: DGB / Direction Générale du Budget budget execution reports.
# To add a new year (or fix a value), follow the update-togo-budget skill:
#   .assistant/skills/update_togo_report
BUDGET_DATA = {
    2024: {
        'revenue_current_lcu': 1_336_110_000_000,
        'expenditure_current_lcu': 1_674_240_000_000,
        'tax_expenditure': 216_940_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/609',
        'source_page': 42,
    },
    2023: {
        'revenue_current_lcu': 1_317_230_000_000,
        'expenditure_current_lcu': 1_681_630_000_000,
        'tax_expenditure': 217_540_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/3',
        'source_page': 43,
    },
    2022: {
        'revenue_current_lcu': 1_051_360_000_000,
        'expenditure_current_lcu': 1_432_880_000_000,
        'tax_expenditure': 160_340_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/449',
        'source_page': 44,
    },
    2021: {
        'revenue_current_lcu': 650_670_000_000,
        'expenditure_current_lcu': 710_970_000_000,
        'tax_expenditure': 99_990_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/360',
        'source_page': 40,
    },
    2020: {
        'revenue_current_lcu': 830_450_000_000,
        'expenditure_current_lcu': 1_082_770_000_000,
        'tax_expenditure': 76_060_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/320',
        'source_page': 31,
    },
    2019: {
        'revenue_current_lcu': 818_400_000_000,
        'expenditure_current_lcu': 840_430_000_000,
        'tax_expenditure': 84_150_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/302',
        'source_page': 32,
    },
}

# COMMAND ----------

df = pd.DataFrame([
    {
        'country_name': 'Togo',
        'country_code': 'TGO',
        'year': year,
        'revenue_current_lcu': d['revenue_current_lcu'],
        'expenditure_current_lcu': d['expenditure_current_lcu'],
        'tax_expenditure': d['tax_expenditure'],
        'data_source': 'Togo DGB Budget Execution Report',
        'source_url': d['source_url'],
    }
    for year, d in sorted(BUDGET_DATA.items())
])
print(df.to_string(index=False))

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.togo_revenue_budget")
