# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

infass is a data pipeline and analytics monorepo for tracking product prices. It scrapes data from retail websites (Mercadona), transforms it through a data warehouse (BigQuery), and serves it via an API/UI.

**Data flow:** Web scraping (Ingestor) → GCS → Transformation → BigQuery → dbt models → Reverse ETL → SQLite → FastAPI → Next.js UI

## Common Commands

### Testing
```bash
make test-all                    # Run all tests
make ingestor.test               # Test ingestor module
make transformer-v2.test         # Test transformer-v2 module
make api.test                    # Test API module
make ui.test                     # Test UI module
make dbt.test                    # Test dbt models
make retl.test                   # Test reverse ETL
make sql-format.test             # Test SQL formatter
make api.local-integration-test  # Run API integration tests
```

### Running Services Locally
```bash
make api.run     # Run API on port 8080
make ui.run      # Run UI on port 3000 (requires API running)
make dbt.build   # Build dbt models
make notebook    # Start Jupyter notebook
```

### Setup
```bash
make setup       # Bootstrap all virtual environments and pre-commit hooks
```

### Linting
Pre-commit hooks handle linting automatically. Manual runs:
- Python: `black --line-length 120 .` and `flake8 --max-line-length=120`
- TypeScript/JS: `npm run lint` (in ui/)
- Terraform: `terraform fmt`
- SQL: `sqlfluff lint` (BigQuery dialect, dbt templater)

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `ingestor/` | Web scraper (Selenium + BeautifulSoup) → uploads CSV to GCS |
| `transformer-v2/` | GCS → BigQuery staging table transformation |
| `dbt/` | BigQuery transformations with 4 model layers: dev, reference, report, star |
| `retl/` | Reverse ETL: BigQuery → SQLite sync for API |
| `api/` | FastAPI service serving from SQLite |
| `ui/` | Next.js 15 + React 19 + TailwindCSS frontend |
| `infra/` | Terraform for GCP infrastructure (Cloud Run, BigQuery, IAP) |
| `infra-ui/` | Terraform for UI deployment |
| `scripts/` | Helper utilities (setup, Docker test runners) |
| `insights/` | Analytics module for generating insights |

## Tech Stack

- **Backend:** Python 3.13, FastAPI, pandas, SQLAlchemy
- **Frontend:** Next.js 15, React 19, TypeScript, TailwindCSS, Recharts
- **Data:** BigQuery, SQLite, dbt, Apache Arrow
- **Infrastructure:** GCP Cloud Run, Terraform, Docker
- **Testing:** pytest (Python), Jest (JS/TS), dbt test
- **CI/CD:** GitHub Actions with Docker-based testing

## Architecture Notes

- Each module is self-contained with its own `README.md`, `Dockerfile`, `Dockerfile.test`, and `requirements.txt`
- All tests run in Docker containers via `scripts/run-docker-test.sh <module>`
- API is protected by Google IAP (OAuth 2.0)
- Environment variables defined in `.env` (not committed)
- Terraform state stored in GCS bucket

## CI/CD Workflows

- `ci-cd.yml` - Main pipeline: test → build Docker images → push to Docker Hub → Terraform deploy
- `trigger-ingestion.yml` - Daily scheduled ingestor run (5 AM UTC)
- `run-dbt.yml` - Reusable dbt build/test workflow

## dbt Model Hierarchy

```
models/
├── dev/        # Development/experimental models
├── reference/  # ref_* dimension and fact tables
├── report/     # Business intelligence reports
└── star/       # Star schema models
```

Run dbt commands from `dbt/` directory using `venv/bin/dbt`.
