"""Where the data lives. Everything is under DATA_ROOT (default: ./data next to this file)."""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.abspath(os.environ.get("DATA_ROOT") or os.path.join(_HERE, "data"))

# Tables are CSVs at DATA_ROOT/prd_mega/indicator/<table>.csv, mirroring the Databricks
# schema the dashboard reads, so table names mean the same thing in both places.
INDICATOR_SCHEMA = "prd_mega.indicator"

# Files kept outside tables: the downloaded boundaries GeoJSON, the source PDFs.
VOLUME_ROOT_PATH = f"{DATA_ROOT}/raw_data"

# write_table keeps only this country's rows. Set COUNTRY_NAME to an empty string to keep all.
COUNTRY_NAME = os.environ.get("COUNTRY_NAME", "Togo")
