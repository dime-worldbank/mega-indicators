"""Pytest setup: put `budget/` on sys.path so tests can `from imf_sdmx import ...`
the same way the Databricks notebook does (Databricks adds the notebook's own
directory to sys.path but not the repo root, so we mirror that here).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
