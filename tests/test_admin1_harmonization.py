"""geo/admin1_harmonization.py: the tables the boundaries and population notebooks share."""
from pathlib import Path

from geo.admin1_harmonization import MOLDOVA_BOUNDARY_ALIASES, MOLDOVA_REGIONS, MOLDOVA_UNIT_TO_REGION

REPO = Path(__file__).resolve().parent.parent


def test_file_is_a_databricks_notebook():
    """Without this first line Databricks refuses to %run it."""
    assert Path(REPO / "geo" / "admin1_harmonization.py").read_text().startswith("# Databricks notebook source\n")


def test_moldova_regions_partition_the_35_units_into_4_regions():
    units = [unit for units in MOLDOVA_REGIONS.values() for unit in units]
    assert len(units) == 35  # 32 raions, Chisinau, Balti, Gagauzia; Transnistria and Bender left out
    assert len(set(units)) == 35  # each in one region
    assert sorted(MOLDOVA_REGIONS) == ["Center", "Chisinau", "North", "South"]
    assert MOLDOVA_UNIT_TO_REGION == {unit: region for region, units in MOLDOVA_REGIONS.items() for unit in units}
    assert MOLDOVA_UNIT_TO_REGION["Unitate Teritoriala Autonoma Gagauzia"] == "South"


def test_boundary_aliases_point_at_a_region_and_add_no_unit():
    assert set(MOLDOVA_BOUNDARY_ALIASES.values()) <= set(MOLDOVA_REGIONS)
    assert not set(MOLDOVA_BOUNDARY_ALIASES) & set(MOLDOVA_UNIT_TO_REGION)
