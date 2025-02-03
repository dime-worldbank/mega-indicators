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
END_YEAR <- as.integer(format(Sys.Date(), "%Y"))

# COMMAND ----------

sess <- sess %>%
    set_dataset(DATASET) %>%
    set_countries_all() %>%
    set_year(START_YEAR) %>%
    set_indicators(INDICATORS)
indicator_merged <- gdl_request(sess)
print(paste(START_YEAR, 'nrow:', nrow(indicator_merged)))

# COMMAND ----------

for (year in (START_YEAR+1):END_YEAR) {
  sess <- sess %>%
    set_dataset(DATASET) %>%
    set_countries_all() %>%
    set_year(year) %>%
    set_indicators(INDICATORS)
  shdi <- gdl_request(sess)

  indicator_merged <- merge(indicator_merged, shdi, all = TRUE)
  print(paste(year, 'nrow:', nrow(shdi), ', merged:', nrow(indicator_merged)))
}

# COMMAND ----------

# Get the school attendance data from areadata

START_YEAR <- 1990
INDICATORS <- c("lprimary", "uprimary", "lsecondary", "usecondary")
DATASET <- 'areadata'
END_YEAR <- as.integer(format(Sys.Date(), "%Y"))

for (year in START_YEAR:END_YEAR) {
  sess <- sess %>%
      set_dataset(DATASET) %>%
      set_countries_all() %>%
      set_year(year) %>%
      set_indicators(INDICATORS)
      areadata <- gdl_request(sess)
      indicator_merged <- merge(indicator_merged, areadata, all = TRUE)
      print(paste(year, 'nrow:', nrow(areadata), ', merged:', nrow(indicator_merged)))
}


# COMMAND ----------

library(readr)
library(dplyr)
library(tidyr)

long_df <- indicator_merged %>%
  dplyr::select(Country, ISO_Code, Region, starts_with(c('healthindex_', 'edindex_', 'incindex_', 'lprimary_', 'uprimary_', 'lsecondary_', 'usecondary_'))) %>%
  pivot_longer(cols = starts_with(c('healthindex_', 'edindex_', 'incindex_', 'lprimary_', 'uprimary_', 'lsecondary_', 'usecondary_')), 
               names_to = "dimension", 
               values_to = "index") %>%
  dplyr::filter(!is.na(index))

combined_df <- long_df %>%
  dplyr::mutate(year = parse_number(gsub(".*_(\\d+)$", "\\1", dimension)),
         dimension = gsub("_.*", "", dimension)) %>%
  pivot_wider(names_from = dimension, values_from = index)

# COMMAND ----------

# Check if any of the specified columns are of type 'list'
columns_to_check <- c('healthindex', 'edindex', 'incindex', "lprimary", "uprimary", "lsecondary", "usecondary")

for (col in columns_to_check) {
  if (is.list(combined_df[[col]])) {
    stop(paste("Error: Column", col, "is of type list when dbl is expected. Please check if long_df has multiple records for the same country-region-dimension"))
  }
}

# COMMAND ----------

# take the weighted average for the attendance data, 
# intervals are uniform so mean works
combined_df <- combined_df %>%
  dplyr::mutate(
    attendance = rowMeans(cbind(lprimary, uprimary, lsecondary, usecondary), na.rm = TRUE)
  )

# COMMAND ----------

# Quality check: missing values
grouped_counts <- combined_df %>%
  dplyr::group_by(Country) %>%
  dplyr::summarize_all(~ mean(is.na(.)))

# COMMAND ----------

grouped_counts <- combined_df %>%
  dplyr::group_by(Country, Region, year) %>%
  dplyr::summarize(obs_count = dplyr::n())

if (!all(grouped_counts$obs_count == 1)) {
  stop(paste("Some groups do not have exactly one observation:",
             paste(grouped_counts[grouped_counts$obs_count != 1, ], collapse = ", ")))
}

# COMMAND ----------

library(SparkR)

hive_config <- list("spark.sql.catalogImplementation" = "hive")
sparkR.session(appName = "global_data_lab", config = hive_config)

sdf <- createDataFrame(combined_df)
table_name <- paste0("prd_mega.indicator_intermediate.global_data_lab_hd_index")
saveAsTable(sdf, tableName = table_name, mode = "overwrite",  overwriteSchema = "true")

print(paste(table_name, 'nrow:', nrow(df)))
