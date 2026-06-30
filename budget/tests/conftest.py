"""Pytest setup for the budget tests.

Note: the tests load `imf_sdmx` directly from its file via importlib (see
test_imf_sdmx.py) rather than `import imf_sdmx`, because that module carries a
`# Databricks notebook source` header and the Databricks runtime refuses to
import notebooks as modules. So no sys.path manipulation is needed here, but we
still add `budget/` to sys.path for any future test that imports a plain helper.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
