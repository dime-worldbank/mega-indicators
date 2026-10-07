"""Where the data lives. Everything is under DATA_ROOT (default: ./data next to this file)."""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.abspath(os.environ.get("DATA_ROOT") or os.path.join(_HERE, "data"))

# Tables are CSVs at DATA_ROOT/indicator/<table>.csv, the folder the BOOST aggregate
# (mega-boost, Togo/TGO_aggregate.py) reads as INDICATOR_DIR.
INDICATOR_DIR = f"{DATA_ROOT}/indicator"

# Files kept outside tables: the downloaded boundaries GeoJSON, the source PDFs.
VOLUME_ROOT_PATH = f"{DATA_ROOT}/raw_data"

# write_table keeps only this country's rows. Set COUNTRY_NAME to an empty string to keep all.
COUNTRY_NAME = os.environ.get("COUNTRY_NAME", "Togo")
