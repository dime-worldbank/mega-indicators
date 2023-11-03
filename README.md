# mega-indicators
A collection of notebooks to fetch and store indicator datasets

## Contributing

To add more indicators, please open a pull request after you've tested your code in Databricks.

- See [consumer_price_index.py](consumer_price_index.py) as a Python example of fetching data from WB API
- See [global_data_lab.r](global_data_lab.r) as an R example of fetching data using a R package from an external data source. Note running this as a job will require setting the `GDL_API_TOKEN` environment variable. Follow the instructions [here](https://docs.globaldatalab.org/gdldata/) to obtain the API token.
