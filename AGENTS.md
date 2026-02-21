# Repository Guidelines

## Project Structure & Module Organization
This is a monorepo for a retail price data platform:
- `ingestor/`: Selenium + BeautifulSoup scrapers writing raw files to GCS.
- `transformer-v2/`: GCS-to-BigQuery transformations.
- `dbt/`: warehouse models (`dev/`, `reference/`, `report/`, `star/`).
- `retl/`: reverse ETL from BigQuery to SQLite.
- `api/`: FastAPI service over SQLite/BigQuery-backed repos.
- `ui/`: Next.js frontend and UI tests.
- `infra/` and `infra-ui/`: Terraform for Cloud Run, buckets, BigQuery, and UI infra.
- `scripts/`: shared helpers (test runner, setup, SQL formatter).
- `data/`: local test/debug artifacts.

## Build, Test, and Development Commands
Use the Makefile as the primary interface:
- `make setup`: bootstrap local virtualenvs and hooks.
- `make test-all`: run all major module tests.
- `make ingestor.test`, `make transformer-v2.test`, `make api.test`, `make ui.test`, `make retl.test`.
- `make dbt.build` / `make dbt.test`: run dbt build/tests from `dbt/`.
- `make api.run` and `make ui.run`: run API and UI locally with Docker.

## Coding Style & Naming Conventions
- Python: format with `black --line-length 120`, lint with `flake8 --max-line-length=120`.
- TypeScript/UI: use project ESLint config (`ui/eslint.config.mjs`).
- SQL: `sqlfluff` (BigQuery + dbt templater).
- Prefer clear module-scoped naming (e.g., `CarrTransformer`, `TxnRecSQLite`).
- Keep file/module names lowercase with underscores in Python; tests as `test_*.py`.

## Testing Guidelines
- Python modules use `pytest`; UI uses Jest/React Testing Library.
- Add/update tests in the same module you modify (`<module>/tests/`).
- Include integration-style tests for pipeline behavior changes (not only unit tests).
- Use deterministic fixtures from `tests/fixtures/` or `data/` when practical.

## Commit & Pull Request Guidelines
- Follow the existing commit pattern: `<scope>: <short description>` (examples: `api: implement bq count`, `infra: increase carr ingestor timeout`).
- Keep commits focused by module/scope.
- PRs should include:
1. What changed and why.
2. Affected modules and risks.
3. Test evidence (commands run and outcomes).
4. Screenshots for UI-visible changes.

## Security & Configuration Tips
- Never commit secrets; rely on `.env` and CI/GCP secret management.
- Validate required env vars before running Dockerized jobs locally.
- Prefer non-production credentials and datasets for local/integration testing.
