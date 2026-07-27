# mega-indicators
A collection of notebooks to fetch and store indicator datasets

## Deployment

The jobs and DLT pipelines are defined as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/)
(`databricks.yml` + `resources/`) and deployed with the Databricks CLI. Use the
`adb-6102124407836814` profile (`-p adb-6102124407836814`).

```bash
# lint the bundle
databricks bundle validate -t dev -p adb-6102124407836814

# deploy your current working tree as [dev <you>] copies (schedules paused) and run one
databricks bundle deploy -t dev -p adb-6102124407836814
databricks bundle run indicators_weekly -t dev -p adb-6102124407836814
```

### Production (prod)

```bash
databricks bundle deploy -t prod -p adb-6102124407836814
databricks bundle run indicators_weekly -t prod -p adb-6102124407836814
```

Prod is bound to the existing jobs/pipelines (no duplicates) and deploys to the team's
`/Workspace/Repos/boostprocessed` folder. Everything runs as the deploying user
(`run_as: ${workspace.current_user.userName}`) until a service principal is set up. The
GDL token is read from the existing `DIMEBOOSTKEYVAULT` secret scope — no setup needed.

### TODO

- Parameterize catalog/schema for real dev write-isolation — notebooks and pipelines
  hardcode `prd_mega`, so `dev` deploys still write prod tables.

## Contributing

To add more indicators, please open a pull request after you've tested your code in Databricks.

- See [consumer_price_index.py](consumer_price_index.py) as a Python example of fetching data from WB API
- See [global_data_lab.r](global_data_lab.r) as an R example of fetching data using a R package from an external data source. Note running this as a job will require setting the `GDL_API_TOKEN` environment variable. Follow the instructions [here](https://docs.globaldatalab.org/gdldata/) to obtain the API token.
