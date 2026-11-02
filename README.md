[![CI/CD Pipeline](https://github.com/gerardovitale/infass/actions/workflows/ci-cd.yml/badge.svg?branch=main)](https://github.com/gerardovitale/infass/actions/workflows/ci-cd.yml)
[![Trigger Ingestor](https://github.com/gerardovitale/infass/actions/workflows/trigger-ingestion.yml/badge.svg)](https://github.com/gerardovitale/infass/actions/workflows/trigger-ingestion.yml)

## Status: decommissioned (2026-11)

The project has been shut down to minimise GCP billing. Only raw data collection is still running.

**Still running** (GCP project `inflation-assistant`, region `europe-southwest1`):

- `trigger-ingestion.yml`: a GitHub Actions cron at 05:00 UTC daily that runs the Cloud Run job `infass-ingestor-merc`.
- The ingestor scrapes Mercadona and writes CSVs to `gs://infass-merc/merc/`.
- `infra/` (Terraform) manages only the ingestor job, its service account, the `infass-merc` bucket and the job-failure email alert.
- `ci-cd.yml` tests, builds and deploys only the ingestor.

**Removed:**

- transformer-v2 job
- all BigQuery datasets (dbt and SQLMesh models)
- reverse ETL jobs
- the SQLite bucket
- the API Cloud Run service
- the UI Cloud Run service
- the Carrefour ingestor job and bucket
- legacy Workflows and buckets

The code for `transformer-v2/`, `dbt/`, `retl/`, `api/`, `ui/`, `insights/` and `infra-ui/` is kept but is no longer built or deployed.

**To revive:**

1. Restore the deleted Terraform files (`infra/api.tf`, `bigquery.tf`, `dbt.tf`, `sqlmesh.tf`, `transformer.tf`) and workflows (`trigger-transformations.yml`, `trigger-reprocessing.yml`) from git history before commit `e36a213`.
2. Recreate BigQuery by reprocessing the raw CSVs in `gs://infass-merc` with transformer-v2 (`--reprocess`).
