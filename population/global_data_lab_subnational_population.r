# Databricks notebook source
install.packages("gdldata")

library(gdldata)
library(magrittr)
library(tidyr)
library(readr)
library(SparkR)
library(dplyr)

# COMMAND ----------

CATALOG <- 'prd_mega'
INTERMEDIATE_SCHEMA  <- 'indicator_intermediate'
SCHEMA <- 'indicator'
BRONZE_TABLE <- paste0(CATALOG, '.', INTERMEDIATE_SCHEMA, '.global_data_lab_subnational_population_bronze')
GOLD_TABLE <- paste0(CATALOG, '.', SCHEMA, '.global_data_lab_subnational_population')

# COMMAND ----------

# general purpose alternative
# api_token <- Sys.getenv("GDL_API_TOKEN")

# Databricks specific
dbutils.widgets.text("GDL_API_TOKEN", "", "GDL API Token")
api_token <- dbutils.widgets.get("GDL_API_TOKEN")

sess <- gdl_session(api_token)

# COMMAND ----------

sess <- sess %>%
    set_dataset('demographics') %>%
    set_countries(character(0)) %>% #Workaround for  https://github.com/GlobalDataLab/R-data-api/issues/5. Replace this line with set_countries_all when the issue is resolved
    set_indicators(c('regpopm'))
    # by default linear extrapolation for 3 years
    # disabling extrapolation doesn't seem to work
    # set_extrapolation_years_linear(0) %>%
spop_merged <- gdl_request(sess)
sdf <- createDataFrame(spop_merged)
saveAsTable(sdf, tableName = BRONZE_TABLE, mode = "overwrite")

print(colnames(spop_merged))
print(paste('nrow:', nrow(spop_merged)))

# COMMAND ----------

query = paste0("SELECT * FROM", " ", BRONZE_TABLE)
sdf <- SparkR::sql(query)
df_population_bronze <-SparkR:: collect(sdf)

# Rename columns in the R data.frame
df_population_bronze <- df_population_bronze %>%
  dplyr::rename(
    year = Year,
    population_millions = regpopm
  )

# Define the mapping vector for country name 
renames <- c(
  "Congo Democratic Republic" = "Congo, Dem. Rep.",
  "Chili"                     = "Chile"
)

# Apply the replacements to the Country column
df_population_bronze <- df_population_bronze %>%
  dplyr::mutate(Country = dplyr::recode(Country, !!!renames))

# COMMAND ----------

df_no_extrapolation <- df_population_bronze %>%
  filter(!is.na(population_millions)) %>%  # Drop rows where population_millions is null
  group_by(Country, Region) %>%
  arrange(Country, Region, desc(year)) %>%  
  filter(row_number() > 3, row_number() <= n() - 3) %>%  # Drop extrapolated years (first & last 3 years given Country, Region)
  ungroup()

# COMMAND ----------

sdf <- createDataFrame(df_no_extrapolation)
saveAsTable(sdf, tableName = GOLD_TABLE, mode = "overwrite")
print(paste(GOLD_TABLE, 'nrow:', nrow(df_no_extrapolation)))
