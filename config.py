# Databricks notebook source
# Shared schema config for `%run`. schema_suffix ("" for dev/prod, "_staging" for the
# staging target) redirects writes to a staging schema. Batch tasks get it as a job-param
# widget; DLT can't %run this and reads spark.conf instead — hence the fallback.
try:
    _suffix = dbutils.widgets.get("schema_suffix")
except Exception:
    _suffix = spark.conf.get("schema_suffix", "")

CATALOG = "prd_mega"
# Silver (intermediate) tables live here too, _silver-suffixed.
INDICATOR_SCHEMA = f"{CATALOG}.indicator{_suffix}"
