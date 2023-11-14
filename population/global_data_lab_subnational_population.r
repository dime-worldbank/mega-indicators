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

key <- 'X'

df <- gather(spop_merged, key = key, value = 'population_millions', -Country, -Continent, -ISO_Code, -Level, -GDLCODE, -Region) %>%
  dplyr::mutate(year = as.integer(parse_number(key))) %>%
  dplyr::select(-key)

sdf <- createDataFrame(df)
table_name <- paste0("indicator.global_data_lab_subnational_population")
saveAsTable(sdf, tableName = table_name, mode = "overwrite")

print(paste(table_name, 'nrow:', nrow(df)))
