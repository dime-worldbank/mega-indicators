# mega-indicators
A collection of notebooks to fetch and store indicator datasets

## Deployment

The jobs and DLT pipelines are defined as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/)
(`databricks.yml` + `resources/`) and deployed with the Databricks CLI. All targets
(dev, staging, prod) are deployed *and* run as the `RPF-ADBSvc-PROD` service principal,
so deploys, resource ownership, and monitoring aren't tied to any one person's account.

### One-time setup: service-principal profile

Ask a workspace admin for an OAuth secret for the service principal, then add this
profile to `~/.databrickscfg`:

```ini
[RPF-ADBSvc-PROD]
host          = <workspace url>
client_id     = <service principal application id>
client_secret = <oauth secret from the admin>
auth_type     = oauth-m2m
```

### Deploying

```bash
# lint the bundle
databricks bundle validate -t staging -p RPF-ADBSvc-PROD

# deploy your current working tree as [staging] copies (schedules paused) and run one
databricks bundle deploy -t staging -p RPF-ADBSvc-PROD
databricks bundle run indicators_weekly -t staging -p RPF-ADBSvc-PROD
```

The same commands work with `-t dev` and `-t prod`. Each target writes to its own
schema *and* its own volume, isolated from prod's:

| Target | Schema | Volume | Purpose |
|---|---|---|---|
| `dev` | `prd_mega.indicator_dev` | `vboost4_dev` | Testing work-in-progress branches |
| `staging` | `prd_mega.indicator_staging` | `vboost4_staging` | Pre-prod validation (paused schedules) |
| `prod` | `prd_mega.indicator` | `vboost4` | The real thing (live schedules + failure emails) |


Prod is bound to the existing jobs/pipelines (no duplicates) and deploys to the team's
`/Workspace/Repos/boostprocessed` folder, with `CAN_MANAGE` granted to the
`ITSDA-LKHS-DAP-PROD-boostprocessed` group. The GDL token is read from the existing
`DIMEBOOSTKEYVAULT` secret scope — no setup needed.

## Contributing

To add more indicators, please open a pull request after you've tested your code in Databricks.

- See [consumer_price_index.py](consumer_price_index.py) as a Python example of fetching data from WB API
- See [global_data_lab.r](global_data_lab.r) as an R example of fetching data using a R package from an external data source. Note running this as a job will require setting the `GDL_API_TOKEN` environment variable. Follow the instructions [here](https://docs.globaldatalab.org/gdldata/) to obtain the API token.
- If your source is a single external site (a national stats agency, etc.) rather than a
  well-established API, fetch it through `utils.py`'s `versioned_dataframe`/`fetch_raw`
  instead of calling `requests`/`pd.read_csv` directly — it caches the parsed result as a
  Delta table, so a temporarily unreachable source serves last-known-good data instead of
  failing the pipeline. See [pry_subnational_population.py](population/PRY/pry_subnational_population.py)
  for a CSV example and [alb_subnational_population.py](population/ALB/alb_subnational_population.py)
  for Excel (`parse=`).
