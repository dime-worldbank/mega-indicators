# Databricks notebook source

_SUFFIX_BY_TARGET = {"prod": "", "staging": "_staging", "dev": "_dev"}
# No vboost4_dev volume exists, so dev shares the staging volume's auxiliary files.
_VOLUME_SUFFIX_BY_TARGET = {"prod": "", "staging": "_staging", "dev": "_staging"}

_target = dbutils.widgets.get("bundle_target")
if _target not in _SUFFIX_BY_TARGET:
    raise RuntimeError(f"Unknown bundle target {_target!r}; expected one of {sorted(_SUFFIX_BY_TARGET)}.")
_suffix = _SUFFIX_BY_TARGET[_target]

CATALOG = "prd_mega"
INDICATOR_SCHEMA = f"{CATALOG}.indicator{_suffix}"
VOLUME_ROOT_PATH = f"/Volumes/{CATALOG}/sboost4/vboost4{_VOLUME_SUFFIX_BY_TARGET[_target]}/Workspace"
