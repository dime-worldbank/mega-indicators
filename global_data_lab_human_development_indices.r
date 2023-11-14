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

START_YEAR <- 1990
INDICATORS <- c('healthindex', 'edindex', 'incindex')
DATASET <- 'shdi'
END_YEAR = as.integer(format(Sys.Date(), "%Y"))

# COMMAND ----------

sess <- sess %>%
    set_dataset(DATASET) %>%
    set_countries_all() %>%
    set_year(START_YEAR) %>%
    set_indicators(INDICATORS)
shdi_merged <- gdl_request(sess)
print(paste(START_YEAR, 'nrow:', nrow(shdi_merged)))

# COMMAND ----------

for (year in (START_YEAR+1):END_YEAR) {
  sess <- sess %>%
    set_dataset(DATASET) %>%
    set_countries_all() %>%
    set_year(year) %>%
    set_indicators(INDICATORS)
  shdi <- gdl_request(sess)

  shdi_merged <- merge(shdi_merged, shdi, all = TRUE)
  print(paste(year, 'nrow:', nrow(shdi), ', merged:', nrow(shdi_merged)))
}

# COMMAND ----------

colnames(shdi_merged)

# COMMAND ----------

library(tidyr)
library(readr)
library(SparkR)
hive_config <- list("spark.sql.catalogImplementation" = "hive")
sparkR.session(appName = "global_data_lab", config = hive_config)

# COMMAND ----------

for (dimension in c('health', 'ed', 'inc')) {
  key <- paste0(dimension, 'index_')
  value <- paste0(dimension, "index")
  
  df <- gather(shdi_merged, key = key, value = value, -Country, -Continent, -ISO_Code, -Level, -GDLCODE, -Region) %>%
    dplyr::mutate(year = parse_number(key)) %>%
    dplyr::select(-key)

  sdf <- createDataFrame(df)
  table_name <- paste0("indicator.global_data_lab_", dimension, "_index")
  saveAsTable(sdf, tableName = table_name, mode = "overwrite")

  print(paste(table_name, 'nrow:', nrow(df)))
}
