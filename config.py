# Databricks notebook source
# Where the indicator tables live. On Databricks the bundle_target job widget picks
# the Unity Catalog schema. Off Databricks (no DATABRICKS_RUNTIME_VERSION in the
# environment) BUNDLE_TARGET (default prod) picks the same schema name, and DATA_ROOT
# names a directory under which every table is a CSV at <catalog>/<schema>/<table>.csv
# (DATA_ROOT=./data gives ./data/prd_mega/indicator/gdp.csv). utils.py's read_table /
# write_table hide the difference from the notebooks.
import os

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

_SUFFIX_BY_TARGET = {"prod": "", "staging": "_staging", "dev": "_dev"}

if IS_DATABRICKS:
    _target = dbutils.widgets.get("bundle_target")
    DATA_ROOT = None
else:
    _target = os.environ.get("BUNDLE_TARGET", "prod")
    DATA_ROOT = os.environ.get("DATA_ROOT")
    if not DATA_ROOT:
        raise RuntimeError(
            "DATA_ROOT is not set. Off Databricks, point it at the directory that holds the "
            "tables as CSVs at <catalog>/<schema>/<table>.csv, e.g. DATA_ROOT=./data for "
            "./data/prd_mega/indicator/gdp.csv."
        )
    DATA_ROOT = os.path.abspath(DATA_ROOT)

if _target not in _SUFFIX_BY_TARGET:
    raise RuntimeError(f"Unknown bundle target {_target!r}; expected one of {sorted(_SUFFIX_BY_TARGET)}.")
_suffix = _SUFFIX_BY_TARGET[_target]

CATALOG = "prd_mega"
INDICATOR_SCHEMA = f"{CATALOG}.indicator{_suffix}"
# Files the notebooks keep outside tables (PDFs, GeoJSON); DATA_ROOT/raw_data locally.
VOLUME_ROOT_PATH = f"/Volumes/{CATALOG}/sboost4/vboost4{_suffix}/Workspace"
if not IS_DATABRICKS:
    VOLUME_ROOT_PATH = f"{DATA_ROOT}/raw_data"
