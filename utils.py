# Databricks notebook source
import wbgapi as wb
import pandas as pd
from databricks.sdk.runtime import spark

def wbgapi_fetch(indicators, col_names, data_source, extra_col_names_from_country_table=[]):
    long_dfs = []
    for series, col_name in zip(indicators, col_names):
        df = wb.data.DataFrame(series, skipBlanks=True).reset_index()
        long_df = df.melt(id_vars='economy', var_name='year', value_name=col_name)
        long_df = long_df.dropna(subset=col_name)
        long_df['year'] = long_df['year'].str.replace('YR', '')
        long_df = long_df.astype({'year': 'int'})
        long_dfs.append(long_df)

    merged_df = long_dfs[0]
    for df in long_dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['economy', 'year'])

    merged_df['data_source'] = data_source

    country_df = spark.table('indicator.country').select('country_name', 'country_code', 'region', *extra_col_names_from_country_table).toPandas()
    country_df
    df = pd.merge(merged_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']]

    return df
