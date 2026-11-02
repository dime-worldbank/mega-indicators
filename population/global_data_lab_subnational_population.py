# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Regional population from Global Data Lab (dataset demographics, indicator regpopm, in
# millions) for every country, read by the Congo DR and Liberia notebooks. One request
# gives all years (gdl_download in utils). Plain pandas on both sides.

token = get_secret('DIMEBOOSTKEYVAULT', 'GDL_API_TOKEN')

# COMMAND ----------

# by default linear extrapolation for 3 years
# disabling extrapolation doesn't seem to work
spop_merged = gdl_download(token, 'demographics', ['regpopm'])
write_table(spop_merged, 'global_data_lab_subnational_population_bronze')

print(list(spop_merged.columns))
print(f'nrow: {len(spop_merged)}')

# COMMAND ----------

df_population_bronze = spop_merged.rename(columns={'Year': 'year', 'regpopm': 'population_millions'})
# Country names as the rest of the pipeline spells them
df_population_bronze['Country'] = df_population_bronze['Country'].replace(GDL_COUNTRY_RENAMES)

# COMMAND ----------

# Drop extrapolated years (first & last 3 years given Country, Region)
df = (df_population_bronze.dropna(subset=['population_millions'])
      .sort_values(['Country', 'Region', 'year'], ascending=[True, True, False]))
position = df.groupby(['Country', 'Region']).cumcount() + 1
count = df.groupby(['Country', 'Region'])['year'].transform('size')
df_no_extrapolation = df[(position > 3) & (position <= count - 3)].reset_index(drop=True)

# COMMAND ----------

write_table(df_no_extrapolation, 'global_data_lab_subnational_population')
print(f'global_data_lab_subnational_population nrow: {len(df_no_extrapolation)}')
