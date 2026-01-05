# Databricks notebook source
install.packages("gdldata")
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

# TODO: Remove this as the issue https://github.com/GlobalDataLab/R-data-api/issues/5 is resolved.
# This line here is overwriting gdl data API function
set_countries_all <- function(sess) {
  if (!is(sess, GDLSession)) {
    stop("Primary argument must be a GDL Session Object")
  }

  sess@countries <- character(0)
  return(sess)
}

# COMMAND ----------

library(tidyr)
library(readr)
library(SparkR)
sparkR.session(appName = "global_data_lab")

# COMMAND ----------

sess <- sess %>%
    set_dataset('demographics') %>%
    set_countries_all() %>%
    set_indicators(c('regpopm')) %>%
    # by default linear extrapolation for 3 years
    # disabling extrapolation doesn't seem to work
    # set_extrapolation_years_linear(0) %>%
    set_interpolation(TRUE)
spop_merged <- gdl_request(sess)
sdf <- createDataFrame(spop_merged)
table_name <- paste0("prd_mega.indicator_intermediate.global_data_lab_subnational_population_bronze")
saveAsTable(sdf, tableName = table_name, mode = "overwrite")

print(colnames(spop_merged))
print(paste('nrow:', nrow(spop_merged)))

# COMMAND ----------

sdf <- SparkR::sql("SELECT * FROM prd_mega.indicator_intermediate.global_data_lab_subnational_population_bronze")
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

library(dplyr)

df_no_extrapolation <- df_population_bronze %>%
  filter(!is.na(population_millions)) %>%  # Drop rows where population_millions is null
  group_by(Country, Region) %>%
  arrange(Country, Region, desc(year)) %>%  
  filter(row_number() > 3, row_number() <= n() - 3) %>%  # Drop extrapolated years (first & last 3 years given Country, Region)
  ungroup()

# COMMAND ----------

library(dplyr)
sdf <- createDataFrame(df_no_extrapolation)
table_name <- paste0("prd_mega.indicator.global_data_lab_subnational_population")
saveAsTable(sdf, tableName = table_name, mode = "overwrite")

print(paste(table_name, 'nrow:', nrow(df_no_extrapolation)))
