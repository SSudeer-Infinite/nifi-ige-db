# Control Metadata Database (`nifi-ige-db`)

Metadata Database repository for the Metadata-Driven Ingestion Framework. This repository houses all Control DB schema definitions (DDL), metadata onboarding configurations (DML), Flyway migrations, repeatable stored procedure migrations, and database CI/CD deployment workflows.

---

## Architecture & Governance Principles

* **Strict Flyway Discipline**: All database changes are versioned, idempotent, and deployed exclusively via Flyway.
* **Separation of Concerns**: Application code (NiFi flows, Helm, orchestration scripts) resides strictly in [nifi-ige-app](file:///D:/Work/infinite/Assurent/dev/nifi-ige-app).
* **100% Stored Procedure Encapsulation**: Pipelines interact with the Metadata DB exclusively through standardized stored procedures (`db/repeatable/`), guaranteeing abstraction, security, and atomic state updates.
* **Zero Hardcoding**: Connection properties reference Key Vault secret names; credentials are never committed in plain text.

---

## Directory Structure

```
nifi-ige-db/
├── .github/
│   └── workflows/
│       └── flyway-migration.yml            # CI/CD promotion pipeline across Dev, Staging, and Prod
├── db/
│   ├── migration/                          # Versioned Forward Migrations (V<Version>__<Desc>.sql)
│   │   ├── V1.0.0__DDL_Control_DB_Schema.sql
│   │   └── V1.1.0__CONFIG_Onboard_Eprism_Sources_And_Seed_Data.sql
│   ├── repeatable/                         # Repeatable Migrations (R__<Desc>.sql)
│   │   ├── R__01_usp_resolve_batch_or_job.sql
│   │   ├── R__02_usp_get_job_source_config.sql
│   │   ├── R__03_usp_create_batch_execution.sql
│   │   ├── R__04_usp_update_batch_execution_status.sql
│   │   ├── R__05_usp_create_job_execution.sql
│   │   ├── R__06_usp_update_job_execution_success.sql
│   │   ├── R__07_usp_update_job_execution_failure.sql
│   │   ├── R__08_usp_advance_watermark.sql
│   │   ├── R__09_usp_log_job_error.sql
│   │   ├── R__10_usp_get_batch_execution_status.sql
│   │   ├── R__11_usp_get_watermark_for_job.sql
│   │   ├── R__12_usp_get_execution_logging_summary.sql
│   │   ├── R__13_usp_check_transient_error.sql
│   │   └── R__14_usp_get_transient_errors.sql
│   └── callbacks/                          # Flyway Lifecycle Callbacks
│       ├── beforeMigrate.sql
│       └── afterMigrate.sql
├── tests/
│   ├── .sqlfluff                           # SQLFluff linting rules for T-SQL
│   └── test_dag_validation.sql             # DAG cycle detection test
├── flyway.conf                             # Baseline Flyway configuration
├── metadata_db.sql                         # Database initialization script
├── METADATA_DB_SPECIFICATION.md            # Complete architectural specification & data dictionary
└── README.md
```

---

## Flyway Migration Conventions

| Prefix | Naming Pattern | Purpose | Example |
| :--- | :--- | :--- | :--- |
| `V` | `V<Version>__<Desc>.sql` | Versioned Forward Migration (DDL, baseline metadata onboarding). Applied once. | `V1.0.0__DDL_Control_DB_Schema.sql` |
| `U` | `U<Version>__<Desc>.sql` | Undo/Rollback Migration (Reverts corresponding forward migration). | `U1.1.0__Rollback_Eprism_Sources.sql` |
| `R` | `R__<Desc>.sql` | Repeatable Migration (Stored procedures, views, functions). Re-applied whenever checksum changes. | `R__01_usp_resolve_batch_or_job.sql` |

---

## Repeatable Stored Procedures Catalog (`db/repeatable/`)

| Procedure | Purpose |
| :--- | :--- |
| `INGFW.USP_RESOLVE_BATCH_OR_JOB` | Resolves batch metadata, recovery strategy, stagger delay, and active job definitions. |
| `INGFW.USP_GET_JOB_SOURCE_CONFIG` | Resolves source table name, schema, watermark column, chunk size, and current watermark. |
| `INGFW.USP_CREATE_BATCH_EXECUTION` | Inserts a new run into `LOG_BATCH_EXECUTIONS` (`RUNNING`) and returns the generated PK. |
| `INGFW.USP_UPDATE_BATCH_EXECUTION_STATUS` | Updates final batch execution status (`SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`) and end timestamp. |
| `INGFW.USP_CREATE_JOB_EXECUTION` | Inserts a new job run into `LOG_JOB_EXECUTIONS` (`RUNNING`) with starting watermark. |
| `INGFW.USP_UPDATE_JOB_EXECUTION_SUCCESS` | Records successful job run, row counts, and ending watermark. |
| `INGFW.USP_UPDATE_JOB_EXECUTION_FAILURE` | Records failed job run and end timestamp. |
| `INGFW.USP_ADVANCE_WATERMARK` | Commits two-phase watermark update in `CONF_WATERMARKS` table. |
| `INGFW.USP_LOG_JOB_ERROR` | Inserts structured failure details and stack traces into `LOG_JOB_ERRORS`. |
| `INGFW.USP_GET_BATCH_EXECUTION_STATUS` | Returns multi-resultset summary, job details, and error logs for UC4 status payload. |
| `INGFW.USP_GET_WATERMARK_FOR_JOB` | Retrieves current watermark value for a specific job. |
| `INGFW.USP_GET_EXECUTION_LOGGING_SUMMARY` | Returns total counts of batch runs, job runs, and logged errors. |
| `INGFW.USP_CHECK_TRANSIENT_ERROR` | Evaluates an error message against `CONF_TRANSIENT_ERRORS` to determine retry eligibility. |
| `INGFW.USP_GET_TRANSIENT_ERRORS` | Returns all active predefined transient errors for pipeline caching. |

---

## Running Migrations Locally

```bash
flyway migrate
```
Or via Docker/CLI specifying `flyway.conf`.

### Testing via `mssql-metadata` Container (Docker Compose)
You can test the entire migration sequence and stored procedures using the `mssql-metadata` service running from `nifi-ige-app`:

```powershell
# 1. Ensure metadata_db database exists
docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Strong_Password_123!' -C -i metadata_db.sql

# 2. Run schema callback & versioned migrations
Get-Content db/callbacks/beforeMigrate.sql -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b
Get-Content db/migration/V1.0.0__DDL_Control_DB_Schema.sql -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b
Get-Content db/migration/V1.1.0__CONFIG_Onboard_Eprism_Sources_And_Seed_Data.sql -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b

# 3. Deploy repeatable stored procedures
Get-ChildItem db/repeatable/R__*.sql | Sort-Object Name | ForEach-Object {
    Get-Content $_.FullName -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b
}

# 4. Run test validation suites
Get-Content tests/test_dag_validation.sql -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b
Get-Content tests/test_sp_suite.sql -Raw | docker exec -i mssql-metadata /opt/mssql-tools18/bin/sqlcmd -S localhost -d metadata_db -U sa -P 'Strong_Password_123!' -C -b
```
