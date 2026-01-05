# Databricks notebook source
install.packages("gdldata")

# COMMAND ----------

library(gdldata)
library(magrittr)
library(SparkR)
sparkR.session(appName = "global_data_lab")


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

# Get the school attendance data from education

START_YEAR <- 1990
INDICATORS <- c("lprimary", "uprimary", "lsecondary", "usecondary")
DATASET <- 'education'
END_YEAR <- as.integer(format(Sys.Date(), "%Y"))

for (year in START_YEAR:END_YEAR) {
  sess <- sess %>%
      set_dataset(DATASET) %>%
      set_countries_all() %>%
      set_year(year) %>%
      set_indicators(INDICATORS)

  edu_data <- gdl_request(sess)
  indicator_merged <- merge(indicator_merged, edu_data, all = TRUE)
  print(paste(year, 'nrow:', nrow(edu_data), ', merged:', nrow(indicator_merged)))
}


# COMMAND ----------

sdf <- createDataFrame(indicator_merged)
table_name <- paste0("prd_mega.indicator_intermediate.global_data_lab_hd_index_bronze")
saveAsTable(sdf, tableName = table_name, mode = "overwrite")

# COMMAND ----------

# now read the bronze table 
sdf <- SparkR::sql("SELECT * FROM prd_mega.indicator_intermediate.global_data_lab_hd_index_bronze")
indicator_merged <-SparkR:: collect(sdf)

# Rename columns in the R data.frame
indicator_merged <- indicator_merged %>%
  dplyr::rename(
    year = Year,
  )
# Define the mapping vector for country name 
renames <- c(
  "Congo Democratic Republic" = "Congo, Dem. Rep.",
  "Chili"                     = "Chile"
)

# Apply the replacements to the Country column
indicator_merged <- indicator_merged %>%
  dplyr::mutate(Country = dplyr::recode(Country, !!!renames))

# COMMAND ----------

# Collapse rows by Country, Region and year, keeping the first non‑NA value
# for each of the specified indicator columns.

collapsed_df <- indicator_merged %>%
  dplyr::group_by(Country, Region, year) %>%
  dplyr::summarise(
    dplyr::across(
      c(
        starts_with('healthindex'),
        starts_with('edindex'),
        starts_with('incindex'),
        starts_with('lprimary'),
        starts_with('uprimary'),
        starts_with('lsecondary'),
        starts_with('usecondary')
      ),
      ~ {
        idx <- which(!is.na(.))
        if (length(idx) > 0) .[idx[1]] else NA_real_
      },
      .names = "{col}"
    ),
    .groups = "drop"
  )

# COMMAND ----------

# Check if any of the specified columns are of type 'list'
columns_to_check <- c('healthindex', 'edindex', 'incindex', "lprimary", "uprimary", "lsecondary", "usecondary")

for (col in columns_to_check) {
  if (is.list(collapsed_df[[col]])) {
    stop(paste("Error: Column", col, "is of type list when dbl is expected. Please check if long_df has multiple records for the same country-region-dimension"))
  }
}

# COMMAND ----------

# take the weighted average for the attendance data, 
# intervals are uniform so mean works
collapsed_df <- collapsed_df %>%
  dplyr::mutate(
    attendance = rowMeans(cbind(lprimary, uprimary, lsecondary, usecondary), na.rm = TRUE)
  )

# COMMAND ----------

# Quality check: missing values
grouped_counts <- collapsed_df %>%
  dplyr::group_by(Country) %>%
  dplyr::summarize_all(~ mean(is.na(.)))

# COMMAND ----------

grouped_counts <- collapsed_df %>%
  dplyr::group_by(Country, Region, year) %>%
  dplyr::summarize(obs_count = dplyr::n())

if (!all(grouped_counts$obs_count == 1)) {
  stop(paste("Some groups do not have exactly one observation:",
             paste(grouped_counts[grouped_counts$obs_count != 1, ], collapse = ", ")))
}

# COMMAND ----------


sdf <- createDataFrame(collapsed_df)
table_name <- paste0("prd_mega.indicator_intermediate.global_data_lab_hd_index")
saveAsTable(sdf, tableName = table_name, mode = "overwrite",  overwriteSchema = "true")

print(paste(table_name, 'nrow:', nrow(collapsed_df)))
