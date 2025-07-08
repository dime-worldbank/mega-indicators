import requests
URL = 'https://datacatalogfiles.worldbank.org/ddh-published-v2/0038272/5/DR0095369/World%20Bank%20Official%20Boundaries%20(GeoJSON)/World%20Bank%20Official%20Boundaries%20-%20Admin%201.geojson'
DATA_DIR = '/dbfs/mnt/DAP/data/admin1geoboundaries'
WB_ADM1_GEO_FILENAME = f'{DATA_DIR}/World Bank Official Boundaries - Admin 1.geojson'

try:
    # Send a GET request to the URL.
    r = requests.get(URL)
    r.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

    # Open the local file in binary write mode
    with open(WB_ADM1_GEO_FILENAME, 'wb') as f:
        f.write(r.content) # Write the entire content at once

    print(f"File '{WB_ADM1_GEO_FILENAME}' downloaded successfully from '{url}'")
except requests.exceptions.RequestException as e:
    print(f"Error downloading file: {e}")
except IOError as e:
    print(f"Error saving file to disk: {e}")
