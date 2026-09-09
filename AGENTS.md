# Repository: nifi-ige-db (Metadata Control DB Project)

## Overview
- **Repository Path**: `D:\Work\infinite\Assurent\dev\nifi-ige-db`
- **Git Remote**: `git@github.com:SSudeer-Infinite/nifi-ige-db.git`
- **Development Base**: `D:\Work\infinite\Assurent\dev`
- **Paired Application Repository**: `D:\Work\infinite\Assurent\dev\nifi-ige-app` (`git@github.com:SSudeer-Infinite/nifi-ige-app.git`)

## Scope & Artifacts
This repository houses all Control Database schemas, Flyway migrations, metadata configurations, and promotion pipelines:
- **Flyway Migrations (`db/migration/`)**:
  - Versioned forward migrations: `V<Version>__<Desc>.sql` (DDL and baseline metadata onboarding DML)
  - Undo/rollback migrations: `U<Version>__<Desc>.sql`
- **Repeatable Migrations (`db/repeatable/`)**:
  - Modular stored procedures: `R__<Desc>.sql` (e.g., `R__01_usp_resolve_batch_or_job.sql`)
- **Callbacks (`db/callbacks/`)**:
  - Pre/post-migration lifecycle hooks (`beforeMigrate.sql`, `afterMigrate.sql`)
- **Tests & Linting (`tests/`)**:
  - SQLFluff linting rules and DAG validation test scripts (`test_dag_validation.sql`)
- **CI/CD Workflows (`.github/workflows/`)**:
  - GitHub Actions Flyway migration deployment across Dev, Staging, and Prod

## Guidelines
- **Strictly Metadata Control DB**: This project is exclusively for the Metadata Control DB (`metadata_db` / `INGFW` schema).
- **No Source or Destination DB Schemas**:
  - Never place source or destination database DDL, tables, or automation into this repository.
  - Source and destination databases pre-exist in enterprise environments and are owned externally.
- **Flyway Naming Conventions**: Strictly adhere to `V<Version>__<Desc>.sql` for versioned forward migrations and `R__<Desc>.sql` for repeatable stored procedures.
- **Zero Hardcoded Credentials**: Source connection records reference Azure Key Vault secret names only.
- **Segregation**: NiFi flow definitions, Helm charts, and runtime scripts belong in `nifi-ige-app`.
