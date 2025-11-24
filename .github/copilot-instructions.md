# Onboarding Instructions for infass Monorepo

Purpose
- Short: help an automated coding agent (and new contributors) understand the repo quickly and work safely and efficiently.
- Scope: broad, repo-level guidance that applies across all modules (each module is independent and has its own README).

What this repo does (summary)
- A monorepo containing multiple independent modules/services for data ingestion, transformation, and downstream tooling. Each module is self-contained with its own README describing inputs, outputs, config, and usage.

Tech stack
- Languages: TypeScript, JavaScript, Python, C, C++, SQL
- Tooling: npm / Node.js, pip / Python, make, GitHub Actions, Terraform (infrastructure changes), DBT (transformations), common linters (ESLint/Prettier, flake8/black)
- OS: Primary developer environment is macOS

Project structure (typical)
- `README.md` — repo overview
- `Makefile` — common build/test commands
- `workflows/` — GitHub Actions workflows (CI/CD)
- `modules/` or top-level folders — independent modules (each with its own `README.md`)
- `requirements.txt` / `pyproject.toml` — Python deps
- `package.json` — Node packages
- `*.tf` (if present) — Terraform for infra
Note: Always read the module `README.md` before making changes.

Coding guidelines
- Small, incremental commits and clear commit messages.
- Tests required for new code: unit + integration as applicable. Use TDD when possible.
- Follow Single Responsibility Principle; keep functions/classes focused.
- Add/update documentation and READMEs when behavior or interfaces change.
- Lint and format code: run repo linters before committing (e.g., `npm run lint`, `black .`, `flake8`).
- Never commit secrets or `.env` with credentials. Use GitHub Actions secrets for CI.

Bootstrapping (macOS, minimal)
- Install runtime managers (recommended): `brew install nvm pyenv` or use system Node/Python.
- Node: `nvm install --lts && nvm use --lts`
- Install JS deps: `npm install` (run in the relevant module directory if multiple packages)
- Python: `pyenv install <version> && pyenv local <version>` then `pip install -r requirements.txt`
- Run tests: `npm test` and/or `pytest`
- Common make targets: `make test`, `make build`, `make lint` (check `Makefile`)

Avoiding common failures
- Confirm working directory for commands (module vs repo root).
- Install correct Node/Python versions matching lockfiles.
- Look for and follow module-specific setup in each `README.md`.
- If a CI job fails locally, inspect `workflows/` for environment variables and steps to replicate.

Search and inspection tips
- Find todos/hacks: `rg "TODO|FIXME|HACK" -n` or `git grep -n "TODO\|FIXME\|HACK"`
- Find workflows: list `workflows/` (CI config names: `ci-cd.yml`, `trigger-ingestor.yaml`, `trigger-transformer.yaml`, `trigger-transformations.yml`)
- List Terraform: `rg --glob '!node_modules' "provider" -n`

Infrastructure & CI notes
- Infra changes must be Terraform-based. Run `terraform plan` and follow repo conventions for state.
- CI is GitHub Actions; use repo `workflows/` files as the canonical workflow definitions.

Validation / sample feature workflow
1. Choose a module and read `README.md`.
2. Create a branch: `git checkout -b feat/<module>-<short-desc>`
3. Add tests first (TDD), implement minimal code change, run unit tests locally.
4. Run lints and formatters.
5. Push branch and open PR describing changes and test results.
6. If CI fails, reproduce the failing job locally (inspect `workflows/`), fix, rerun tests, update docs.

Resources
- `README.md` (module-level)
- `Makefile`
- `workflows/` directory for CI flows
- Look for `requirements.txt`, `package.json`, and any `*.tf` files

Conventions summary
- Tests required; small commits; update READMEs; avoid committing secrets; run lints; use module READMEs as source of truth.

If uncertain: read the module `README.md` and check the related workflow in `workflows/` before running commands that modify infrastructure or production data.
