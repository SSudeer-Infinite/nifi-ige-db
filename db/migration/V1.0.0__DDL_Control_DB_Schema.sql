-- ==============================================================================
-- V1.0.0__DDL_Control_DB_Schema.sql: Baseline Schema DDL
-- Framework: Metadata-Driven Ingestion Engine
-- Flyway Versioned Migration: V1.0.0
-- Target Database: metadata_db (Schema: INGFW)
-- Table Prefix Standards: CONF_ (Configuration), LOG_ (Telemetry & Audit)
-- ==============================================================================
--
-- ARCHITECTURAL DESIGN RATIONALE:
-- ------------------------------------------------------------------------------
-- 1. SEPARATION OF SOURCES AND DESTINATIONS (Why two tables instead of one?):
--    - 1:N Fan-Out Architecture: A single extracted dataset from a source table often
--      needs to replicate simultaneously to multiple sinks (e.g. Bronze ADLS Gen2
--      raw Parquet, an operational Iceberg table, and an audit table). Combining
--      source and destination into a single mapping table forces complete duplication
--      of extraction queries, watermark column definitions, and throttle configs.
--    - Separation of Concerns: Source metadata strictly governs extraction (queries,
--      read concurrency, fetch chunking, watermarking, source primary keys).
--      Destination metadata strictly governs landing (target storage directory, file
--      formats, compression codecs, backpressure limits, partition formatting).
--      Separating them avoids wide, sparsely populated tables with high nullability.
--    - Independent Rollback & Watermark Lifecycle: High watermarks represent the state
--      of source extraction. On failure, cleanup operations are strictly destination-
--      specific (e.g., purging _tmp_<job_execution_id>/ folders from ADLS Gen2 or
--      truncating a target staging table). Decoupled tables mirror this physical
--      two-phase commit and rollback discipline.
--
-- 2. DEDICATED CONNECTIONS TABLE (Why isolate connection profiles?):
--    - Normalization & Reusability (DRY): Hundreds of tables are ingested from the
--      same physical database cluster or ERP instance and land in the same ADLS
--      Gen2 account. Storing hostnames, port numbers, SSL configs, and protocols
--      inside every source or destination record creates massive redundancy.
--    - Centralized Endpoint & Credential Rotation: When a database host migrates or
--      Azure Key Vault secrets rotate, updating a single row in INGFW.CONF_CONNECTIONS
--      instantly updates all associated pipelines without modifying job definitions.
--    - Zero Plain-Text Passwords: Serves as the authoritative bridge to Azure Key
--      Vault secret names for runtime Parameter Context synchronization.
--    - Connection Pool Optimization: NiFi controller services (e.g., DBCPConnectionPool)
--      are instantiated per connection profile rather than per table, allowing
--      global concurrency throttling at the database level.
--
-- 3. PREFIXING CONVENTION (CONF_ vs. LOG_):
--    - CONF_*: Applied to configuration, topology, parameters, and watermark state.
--      Low-frequency mutation via Flyway migrations or metadata onboarding.
--    - LOG_*: Applied to append-only execution metrics, telemetry, and error logs.
--      High-frequency streaming inserts requiring retention and partition management.
-- ==============================================================================

-- Ensure INGFW schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'INGFW')
BEGIN
    EXEC('CREATE SCHEMA INGFW');
END
GO

-- ==============================================================================
-- 1. TABLE: INGFW.CONF_BATCHES
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Represents an enterprise batch execution unit (typically invoked by an
--   external enterprise scheduler such as Automic UC4). A batch groups a set of
--   related ingestion jobs and governs their collective execution order,
--   concurrency stagger delays, and partial-success / fail-fast recovery policy.
--
-- RECOVERY STRATEGIES:
--   - 'IGNORE' (Partial Success): Completed jobs remain committed; failed jobs
--     have their landing purged and watermarks frozen. Batch is PARTIAL_SUCCESS.
--   - 'FAIL' (Atomic Batch): Any single job failure triggers batch-level failure.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_BATCHES', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_BATCHES (
        BATCH_ID VARCHAR(50) PRIMARY KEY,              -- Unique enterprise batch identifier (e.g. BAT_POST_STAGING_001)
        DESCRIPTION NVARCHAR(MAX) NULL,                -- Human-readable description of workload and domain
        RECOVERY_STRATEGY VARCHAR(50) NOT NULL DEFAULT 'IGNORE', -- Failure policy: IGNORE, FAIL, BATCH_RESTART, JOB_RESTART
        MAX_TRIES INT NOT NULL DEFAULT 0,              -- Maximum outer retry attempts allowed for the batch
        IS_ACTIVE BIT NOT NULL DEFAULT 1,              -- 1 = Eligible for scheduling; 0 = Temporarily suspended
        JOB_STAGGER_DELAY INT NOT NULL DEFAULT 5,      -- Delay in seconds between consecutive job dispatches to avoid spikes
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Audit creation timestamp
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP -- Audit modification timestamp
    );
END
GO

-- ==============================================================================
-- 2. TABLE: INGFW.CONF_BATCH_JOBS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Defines individual table extraction pipelines belonging to a batch.
--   Configures pipeline execution variant (fail-fast vs in-flight retry),
--   directed acyclic graph (DAG) dependencies via PREV_JOB_ID, transient
--   retry ceilings, and automatic staging cleanup behavior.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_BATCH_JOBS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_BATCH_JOBS (
        ID INT IDENTITY(1,1) PRIMARY KEY,             -- Auto-increment primary key
        SOLUTION_ID VARCHAR(50) NOT NULL,              -- Solution identifier
        PROJECT_ID VARCHAR(50) NOT NULL,               -- Project identifier
        JOB_ID VARCHAR(50) NOT NULL,                   -- Business job identifier
        BATCH_ID VARCHAR(50) NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCHES(BATCH_ID) ON DELETE SET NULL, -- Parent batch relationship (nullable for standalone jobs)
        JOB_DESCRIPTION VARCHAR(255) NULL,             -- Pipeline description (renamed from JOB_NAME)
        JOB_TYPE VARCHAR(50) NOT NULL,                 -- Pipeline variant: RDBMS_TO_PARQUET_NO_RETRY or WITH_RETRY
        PIPELINE_ID VARCHAR(255) NULL,                 -- Optional override NiFi pipeline/process group identifier
        PREV_JOB_ID INT NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCH_JOBS(ID), -- Prerequisite job dependency in execution DAG
        TRANSIENT_ERROR_RETRY_COUNT INT NOT NULL DEFAULT 0, -- Permitted in-flight retries for transient errors
        CLEANUP_ON_ERROR TINYINT NOT NULL DEFAULT 1,   -- 1 = Recursively purge _tmp_<id> staging folder on failure
        MAX_RETRY_ATTEMPTS INT NOT NULL DEFAULT 0,     -- Maximum outer recovery restart attempts
        IS_ACTIVE BIT NOT NULL DEFAULT 1,              -- 1 = Active for processing; 0 = Decommissioned / Disabled
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT UQ_CONF_BATCH_JOBS UNIQUE (SOLUTION_ID, PROJECT_ID, JOB_ID)
    );
END
GO

-- ==============================================================================
-- 3. TABLE: INGFW.CONF_CONNECTIONS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Centralized infrastructure endpoint catalog. Decouples physical hostnames,
--   ports, protocols, and Azure Key Vault secret names from individual extraction
--   and landing configurations. Enables zero-hardcoding and single-point
--   credential and endpoint rotation across all pipelines.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_CONNECTIONS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_CONNECTIONS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        CONNECTION_NAME VARCHAR(255) NOT NULL UNIQUE,  -- Symbolic name (e.g. CONN_MSSQL_SOURCE, CONN_PARQUET_DEST)
        CONNECTION_TYPE VARCHAR(50) NOT NULL,          -- Protocol/technology: MSSQL, ORACLE, POSTGRESQL, PARQUET_LANDING
        CONNECTION_PROPERTIES NVARCHAR(MAX) NOT NULL,  -- JSON containing host, port, db name, Key Vault secret reference
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 4. TABLE: INGFW.CONF_SOURCES
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Extraction-side metadata for a job. Specifies source schema and table name
--   (or custom SELECT query), chunking micro-batch size, source-side throttling,
--   and the watermark tracking column used for incremental change detection.
--   pipelines/jobs that do not need source config will not have any rows here
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_SOURCES', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_SOURCES (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        JOB_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCH_JOBS(ID) ON DELETE CASCADE, -- 1:1 or 1:N job linkage
        CONNECTION_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_CONNECTIONS(ID),           -- Physical source connection
        SOURCE_PROPERTIES NVARCHAR(MAX) NULL,          -- JSON payload: schema, table_name, custom SQL predicate
--        WATERMARK_FIELD VARCHAR(255) NULL,             -- Column evaluated for incremental ingestion (e.g. id, modified_date)
--        WATERMARK_TYPE VARCHAR(50) NULL,               -- Data type: BIGINT, TIMESTAMP, STRING
        CHUNK_SIZE INT NULL DEFAULT 1000,              -- Micro-batch row size extracted per query
        THROTTLE_RATE_TYPE INT NULL DEFAULT 0,         -- 0 = requests per second; 1 = absolute count
        THROTTLE_RATE INT NULL DEFAULT 5,              -- Throttle limit to prevent overwhelming source database
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 5. TABLE: INGFW.CONF_DESTINATIONS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Landing-side metadata for a job. Defines destination connection, target
--   storage path (e.g. ADLS Gen2 Bronze directory), file format (Parquet,
--   Iceberg, Delta), compression codec (Snappy, Zstandard), and NiFi queue
--   backpressure thresholds.
--   pipelines/jobs that do not need destination config will not have any rows here
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_DESTINATIONS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_DESTINATIONS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        JOB_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCH_JOBS(ID) ON DELETE CASCADE, -- Associated job pipeline
        CONNECTION_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_CONNECTIONS(ID),           -- Target storage connection
        DESTINATION_PROPERTIES NVARCHAR(MAX) NULL,     -- JSON: target_dir, format, compression, partition scheme
        CHUNK_SIZE INT NULL DEFAULT 1000,              -- Target file row limit per output file
        BACK_PRESSURE_LIMIT INT NULL DEFAULT 10000,    -- NiFi FlowFile queue ceiling before pausing upstream extraction
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 6. TABLE: INGFW.CONF_SOURCE_KEYS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Catalog of primary, composite, or clustering key columns for each source.
--   Used by the ingestion framework to perform partition chunking, guarantee
--   idempotent ingestion, and drive target upsert/merge logic.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_SOURCE_KEYS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_SOURCE_KEYS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        SOURCE_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_SOURCES(ID) ON DELETE CASCADE, -- Associated source definition
        KEY_FIELD VARCHAR(255) NOT NULL,               -- Column name participating in table primary/clustering key
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- Example:
--     source_id: 101
--     Source Keys are (account_name, transaction_ref, txn_date)

-- ==============================================================================
-- 7. TABLE: INGFW.CONF_WATERMARKS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Persistent state store for incremental extraction. Maintains the last
--   committed watermark (high-water mark) for each source. Employs a strict
--   Two-Phase Commit discipline: watermarks are only advanced after 100% of
--   target FlowFiles have landed successfully and passed validation.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_WATERMARKS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_WATERMARKS (
        SOURCE_ID INT PRIMARY KEY FOREIGN KEY REFERENCES INGFW.CONF_SOURCES(ID) ON DELETE CASCADE, -- 1:1 with source
        -- LAST_WATERMARK_VAL BIGINT NULL,                -- Highest numeric/identity watermark extracted and committed
        -- LAST_WATERMARK_TIMESTAMP DATETIME2 NULL,       -- Highest temporal/datetime watermark extracted and committed
        -- LAST_WATERMARK_STR VARCHAR(255) NULL,          -- String/alphanumeric watermark value
        WATERMARK_STATE NVARCHAR(MAX) NULL,            -- Auxiliary JSON state payload for composite watermarks
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- Example:
-- RUN 1:
--     source_id: 101
--     WATERMARK_STATE: {
--         "account_name": "SAVINGS", 
--         "transaction_ref": "REF00001000", 
--         "txn_date": "2024-06-06"
--     },
--     BI_CREATED_DATE: "2024-06-06"
--     BI_MODIFIED_DATE: "2024-06-06"
-- 
-- RUN 2:
--     source_id: 101
--     WATERMARK_STATE: {
--         "account_name": "CURRENT", 
--         "transaction_ref": "REF00006000", 
--         "txn_date": "2024-06-06"
--     },
--     BI_CREATED_DATE: "2024-06-07"
--     BI_MODIFIED_DATE: "2024-06-07"

-- ==============================================================================
-- 8. TABLE: INGFW.CONF_TRANSIENT_ERRORS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Authoritative catalog of predefined transient error patterns and exception
--   classes. Evaluated by USP_CHECK_TRANSIENT_ERROR during pipeline execution:
--   - Errors matching an active row permit in-flight retries with backoff.
--   - Errors NOT in this table fail fast immediately (0 retries), halting the
--     job and triggering atomic staging cleanup.
-- ==============================================================================
IF OBJECT_ID('INGFW.CONF_TRANSIENT_ERRORS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.CONF_TRANSIENT_ERRORS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ERROR_CODE VARCHAR(100) NOT NULL UNIQUE,       -- Standardized error code (e.g. SOCKET_TIMEOUT, DEADLOCK)
        ERROR_CATEGORY VARCHAR(100) NOT NULL,          -- Category: NETWORK_TIMEOUT, DATABASE_CONCURRENCY, RATE_LIMIT
        EXCEPTION_CLASS VARCHAR(255) NOT NULL,         -- Exception class name (e.g. SocketTimeoutException)
        ERROR_PATTERN VARCHAR(255) NOT NULL,           -- SQL LIKE pattern matched against raw error (%...%)
        DESCRIPTION NVARCHAR(MAX) NULL,                -- Detailed operational explanation
        IS_ACTIVE BIT NOT NULL DEFAULT 1,              -- 1 = Retry eligible; 0 = Deactivated (treat as permanent)
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 9. TABLE: INGFW.LOG_BATCH_EXECUTIONS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Audit logging and lifecycle tracking table for batch executions. Each run
--   triggered by the enterprise scheduler receives an INVOCATION_ID and records
--   overall execution status, start/end timestamps, and summary error messages.
-- ==============================================================================
IF OBJECT_ID('INGFW.LOG_BATCH_EXECUTIONS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.LOG_BATCH_EXECUTIONS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        BATCH_ID VARCHAR(50) NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCHES(BATCH_ID) ON DELETE SET NULL, -- Link to configured batch (nullable for standalone jobs)
        INVOCATION_ID VARCHAR(255) NOT NULL UNIQUE,    -- Unique execution token (e.g. b-exec-20260904-120000-abcd12)
        STATUS VARCHAR(50) NOT NULL,                   -- Lifecycle state: RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED
        START_TIME DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Execution trigger timestamp
        END_TIME DATETIME2 NULL,                       -- Batch completion or failure timestamp
        ERROR_MESSAGE NVARCHAR(MAX) NULL,              -- Root failure summary if batch terminated with errors
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 10. TABLE: INGFW.LOG_JOB_EXECUTIONS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Granular per-job execution telemetry within a batch run. Records starting
--   watermark snapshots, ending watermark values, row counts landed in the
--   destination, elapsed runtime, and atomic rollback actions taken on failure.
-- ==============================================================================
IF OBJECT_ID('INGFW.LOG_JOB_EXECUTIONS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.LOG_JOB_EXECUTIONS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        BATCH_EXECUTION_ID INT NULL FOREIGN KEY REFERENCES INGFW.LOG_BATCH_EXECUTIONS(ID) ON DELETE SET NULL, -- Nullable for standalone job runs
        JOB_ID INT NOT NULL FOREIGN KEY REFERENCES INGFW.CONF_BATCH_JOBS(ID), -- Associated batch job definition
        STATUS VARCHAR(50) NOT NULL,                   -- Job status: PENDING, RUNNING, SUCCESS, FAILED, SKIPPED
        RECORDS_PROCESSED BIGINT NOT NULL DEFAULT 0,   -- Total verified rows persisted to destination storage
        WATERMARK_START VARCHAR(255) NULL,             -- Watermark snapshot captured before extraction began
        WATERMARK_END VARCHAR(255) NULL,               -- Watermark snapshot captured after successful commit
        START_TIME DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        END_TIME DATETIME2 NULL,
        RECOVERY_ACTION_TAKEN VARCHAR(255) NULL,       -- Action taken on failure (e.g. DELETED_TMP_PARQUET_DIRECTORY)
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO

-- ==============================================================================
-- 11. TABLE: INGFW.LOG_JOB_ERRORS
-- ------------------------------------------------------------------------------
-- PURPOSE:
--   Centralized diagnostic error logging repository. Stores classified error
--   codes, localized error messages, and full exception stack traces linked
--   directly to specific batch and job invocations for fast root-cause analysis.
-- ==============================================================================
IF OBJECT_ID('INGFW.LOG_JOB_ERRORS', 'U') IS NULL
BEGIN
    CREATE TABLE INGFW.LOG_JOB_ERRORS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        BATCH_INVOCATION_ID INT NULL,                  -- References INGFW.LOG_BATCH_EXECUTIONS(ID) (nullable for standalone runs)
        JOB_INVOCATION_ID INT NULL,                    -- References INGFW.LOG_JOB_EXECUTIONS(ID)
        ERROR_CODE VARCHAR(50) NOT NULL,               -- Standardized code (e.g. SOCKET_TIMEOUT, DEADLOCK, PERMANENT_AUTH)
        ERROR_MESSAGE NVARCHAR(MAX) NOT NULL,          -- Sanitized error message
        STACK_TRACE NVARCHAR(MAX) NULL,                -- Full exception trace and diagnostic context
        BI_CREATED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
END
GO
