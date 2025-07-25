# Copilot Instructions for infass Monorepo

This repository (monorepo alike) contains multiple independent modules, each with its own README describing its purpose,
input/output data, configuration, and usage.

## General Guidelines

- **Best agile and software development practices must be followed for all new features and bug fixes:**
  - Make small, incremental changes.
  - Write thorough tests for all code (unit, integration, and end-to-end where applicable).
  - Use TDD (Test-Driven Development) whenever possible.
  - Apply the Single Responsibility Principle: each function/class/module should have one clear purpose.
  - Ensure code is readable, maintainable, and well-documented.
  - Use clear commit messages and PR descriptions.
  - Update README files and documentation as needed.

- **Module-Specific Practices:**
  - Read the README in each module before making changes.
  - Respect the input/output schema and configuration described in each module’s documentation.
  - Ensure compatibility with CI/CD workflows and deployment jobs defined in [workflows](workflows/).

- **Testing:**
  - All new code must include corresponding tests.
  - Run the full test suite before merging changes.
  - Use mocks and stubs for external dependencies in tests.

- **Infrastructure:**
  - Infrastructure changes must be made via Terraform files and follow the established patterns.
  - Validate and plan Terraform changes before applying.

- **CI/CD:**
  - All modules are built and tested via GitHub Actions workflows:
    - [ci-cd.yaml](workflows/ci-cd.yaml)
    - [trigger-ingestor.yaml](workflows/trigger-ingestor.yaml)
    - [trigger-transformer.yaml](workflows/trigger-transformer.yaml)
    - [trigger-dbt.yml](workflows/trigger-dbt.yml)
  - Ensure your changes do not break existing workflows or deployments.

## References

- See each module’s README for details on architecture, data flow, configuration, and usage.
- See the Makefile for common build, test, and deployment commands.

---

**By following these instructions, you help ensure code quality, maintainability, and reliability across all modules in the infass monorepo.**
