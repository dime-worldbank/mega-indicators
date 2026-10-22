# Databricks notebook source
# Admin-1 harmonization tables shared by notebooks: %run on Databricks, imported by
# local_runner.py elsewhere. Data only, so it loads both ways.
#
# TODO consolidate the rest of the admin1 harmonization here, in two sections:
#   1. spelling aliases -> admin1 labels. Source of truth: the World Bank Official Boundaries
#      names; every source, BOOST included, is aligned to them. Covers correct_admin1_names in
#      admin_boundaries_transform_load.py and the per-source fixes in population/BDI, BGD, BFA,
#      CHL, poverty/subnational_poverty and human_development (several repeat each other).
#   2. BOOST reporting units: the Albania, Ghana and Moldova groupings and the granularity
#      entries hiding in correct_admin1_names. These follow BOOST's granularity, not official
#      geography, so they should eventually come from a table the BOOST side publishes.

# Moldova: raions, Chisinau, Balti and Gagauzia grouped into the four statistical regions
# BOOST reports at (Gagauzia folded into South). Transnistria and Bender are in no region
# and not in the NBS estimates, so they are left out.
MOLDOVA_REGIONS = {
    'North': ['Balti', 'Briceni', 'Donduseni', 'Drochia', 'Edinet', 'Falesti',
              'Floresti', 'Glodeni', 'Ocnita', 'Riscani', 'Singerei', 'Soroca'],
    'Center': ['Anenii Noi', 'Calarasi', 'Criuleni', 'Dubasari', 'Hincesti', 'Ialoveni',
               'Nisporeni', 'Orhei', 'Rezina', 'Soldanesti', 'Straseni', 'Telenesti',
               'Ungheni'],
    'South': ['Basarabeasca', 'Cahul', 'Cantemir', 'Causeni', 'Cimislia', 'Leova',
              'Stefan Voda', 'Taraclia', 'Unitate Teritoriala Autonoma Gagauzia'],
    'Chisinau': ['Chisinau'],
}
MOLDOVA_UNIT_TO_REGION = {unit: region for region, units in MOLDOVA_REGIONS.items() for unit in units}
# The capital as the World Bank boundaries file may also spell it; for the boundaries notebook.
MOLDOVA_BOUNDARY_ALIASES = {'Chisinau Municipality': 'Chisinau'}
