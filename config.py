# Databricks notebook source

_SUFFIX_BY_TARGET = {"prod": "", "staging": "_staging"}

_target = dbutils.widgets.get("bundle_target")
if _target not in _SUFFIX_BY_TARGET:
    raise RuntimeError(f"Unknown bundle target {_target!r}; expected one of {sorted(_SUFFIX_BY_TARGET)}.")

CATALOG = "prd_mega"
INDICATOR_SCHEMA = f"{CATALOG}.indicator{_SUFFIX_BY_TARGET[_target]}"
