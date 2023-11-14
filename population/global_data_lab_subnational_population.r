# Databricks notebook source
install.packages("gdldata")

# COMMAND ----------

library(gdldata)
library(magrittr)

# COMMAND ----------

# general purpose alternative
# api_token <- Sys.getenv("GDL_API_TOKEN")

# Databricks specific
dbutils.widgets.text("GDL_API_TOKEN", "", "GDL API Token")
api_token <- dbutils.widgets.get("GDL_API_TOKEN")

sess <- gdl_session(api_token)

# COMMAND ----------

sess <- sess %>%
    set_dataset('areadata') %>%
    set_countries_all() %>%
    set_indicators(c('regpopm'))
    # by default linear extrapolation for 3 years
    # disabling extrapolation doesn't seem to work
    # set_extrapolation_years_linear(0) %>%
    # set_extrapolation_years_nearest(0) %>%
    # set_interpolation(TRUE)
spop_merged <- gdl_request(sess)
print(colnames(spop_merged))
print(paste('nrow:', nrow(spop_merged)))

# COMMAND ----------

library(tidyr)
library(readr)
library(SparkR)
hive_config <- list("spark.sql.catalogImplementation" = "hive")
sparkR.session(appName = "global_data_lab", config = hive_config)

# COMMAND ----------

library(dplyr)

key <- 'X'

df <- gather(spop_merged, key = key, value = 'population_millions', -Country, -Continent, -ISO_Code, -Level, -GDLCODE, -Region) %>%
  mutate(year = as.integer(parse_number(key))) %>%
  select(-key)

df_no_extrapolation <- df %>%
  filter(!is.na(population_millions)) %>%  # Drop rows where population_millions is null
  group_by(Country, Region) %>%
  arrange(Country, Region, desc(year)) %>%  
  filter(row_number() > 3) %>%  # Drop extrapolated years (last 3 years given Country, Region)
  ungroup()

sdf <- createDataFrame(df_no_extrapolation)
table_name <- paste0("indicator.global_data_lab_subnational_population")
saveAsTable(sdf, tableName = table_name, mode = "overwrite")

print(paste(table_name, 'nrow:', nrow(df_no_extrapolation)))
