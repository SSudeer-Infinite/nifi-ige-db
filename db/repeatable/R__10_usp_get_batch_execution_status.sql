-- ==============================================================================
-- Repeatable Migration: R__10_usp_get_batch_execution_status.sql
-- Description: Status pipeline helper for UC4 polling and standalone job tracking
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_GET_BATCH_EXECUTION_STATUS
    @InvocationId VARCHAR(255) = NULL,
    @JobExecutionId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchExecId INT = NULL;
    DECLARE @ResolvedJobExecId INT = @JobExecutionId;

    -- If InvocationId is provided, check if it matches a Batch Execution or a Job Execution
    IF @InvocationId IS NOT NULL
    BEGIN
        SELECT @BatchExecId = ID 
        FROM INGFW.LOG_BATCH_EXECUTIONS 
        WHERE INVOCATION_ID = @InvocationId;

        -- If not found in LOG_BATCH_EXECUTIONS and InvocationId is numeric, check LOG_JOB_EXECUTIONS
        IF @BatchExecId IS NULL AND @ResolvedJobExecId IS NULL AND ISNUMERIC(@InvocationId) = 1
        BEGIN
            SELECT @ResolvedJobExecId = ID
            FROM INGFW.LOG_JOB_EXECUTIONS
            WHERE ID = CAST(@InvocationId AS INT);
        END;
    END;

    -- Branch A: Batch Execution context (or BatchExecId resolved)
    IF @BatchExecId IS NOT NULL
    BEGIN
        -- Result Set 1: Batch Details & Counts
        SELECT 
            ISNULL(B.BATCH_ID, BE.BATCH_ID) AS BATCH_ID,
            BE.ID AS BATCH_EXECUTION_ID,
            BE.INVOCATION_ID,
            BE.STATUS AS BATCH_STATUS,
            ISNULL(B.RECOVERY_STRATEGY, 'IGNORE') AS RECOVERY_STRATEGY,
            BE.START_TIME,
            BE.END_TIME,
            COUNT(JE.ID) AS TOTAL_JOBS,
            SUM(CASE WHEN JE.STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_JOBS,
            SUM(CASE WHEN JE.STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_JOBS
        FROM INGFW.LOG_BATCH_EXECUTIONS BE
        LEFT JOIN INGFW.CONF_BATCHES B ON BE.BATCH_ID = B.BATCH_ID
        LEFT JOIN INGFW.LOG_JOB_EXECUTIONS JE ON BE.ID = JE.BATCH_EXECUTION_ID
        WHERE BE.ID = @BatchExecId
        GROUP BY B.BATCH_ID, BE.BATCH_ID, BE.ID, BE.INVOCATION_ID, BE.STATUS, B.RECOVERY_STRATEGY, BE.START_TIME, BE.END_TIME;

        -- Result Set 2: Job Details
        SELECT 
            BJ.ID,
            BJ.SOLUTION_ID,
            BJ.PROJECT_ID,
            BJ.JOB_ID,
            BJ.BATCH_ID,
            BJ.JOB_DESCRIPTION,
            JE.ID AS JOB_EXECUTION_ID,
            JE.BATCH_EXECUTION_ID,
            JE.STATUS,
            JE.RECORDS_PROCESSED,
            JE.WATERMARK_START,
            JE.WATERMARK_END
        FROM INGFW.LOG_JOB_EXECUTIONS JE
        JOIN INGFW.CONF_BATCH_JOBS BJ ON JE.JOB_ID = BJ.ID
        WHERE JE.BATCH_EXECUTION_ID = @BatchExecId
        ORDER BY JE.ID ASC;

        -- Result Set 3: Job Errors (if any)
        SELECT 
            ERR.JOB_INVOCATION_ID,
            ERR.ERROR_CODE,
            ERR.ERROR_MESSAGE,
            ERR.STACK_TRACE
        FROM INGFW.LOG_JOB_ERRORS ERR
        WHERE ERR.BATCH_INVOCATION_ID = @BatchExecId
           OR ERR.JOB_INVOCATION_ID IN (SELECT ID FROM INGFW.LOG_JOB_EXECUTIONS WHERE BATCH_EXECUTION_ID = @BatchExecId);
    END
    -- Branch B: Standalone Job Execution context (no Batch Execution)
    ELSE IF @ResolvedJobExecId IS NOT NULL
    BEGIN
        -- Result Set 1: Job Execution as Single-Unit Summary
        SELECT 
            BJ.BATCH_ID,
            JE.BATCH_EXECUTION_ID,
            ISNULL(BE.INVOCATION_ID, CAST(JE.ID AS VARCHAR(255))) AS INVOCATION_ID,
            JE.STATUS AS BATCH_STATUS,
            ISNULL(B.RECOVERY_STRATEGY, 'IGNORE') AS RECOVERY_STRATEGY,
            JE.START_TIME,
            JE.END_TIME,
            1 AS TOTAL_JOBS,
            CASE WHEN JE.STATUS = 'SUCCESS' THEN 1 ELSE 0 END AS SUCCESSFUL_JOBS,
            CASE WHEN JE.STATUS = 'FAILED' THEN 1 ELSE 0 END AS FAILED_JOBS
        FROM INGFW.LOG_JOB_EXECUTIONS JE
        JOIN INGFW.CONF_BATCH_JOBS BJ ON JE.JOB_ID = BJ.ID
        LEFT JOIN INGFW.LOG_BATCH_EXECUTIONS BE ON JE.BATCH_EXECUTION_ID = BE.ID
        LEFT JOIN INGFW.CONF_BATCHES B ON ISNULL(BE.BATCH_ID, BJ.BATCH_ID) = B.BATCH_ID
        WHERE JE.ID = @ResolvedJobExecId;

        -- Result Set 2: Job Details
        SELECT 
            BJ.ID,
            BJ.SOLUTION_ID,
            BJ.PROJECT_ID,
            BJ.JOB_ID,
            BJ.BATCH_ID,
            BJ.JOB_DESCRIPTION,
            JE.ID AS JOB_EXECUTION_ID,
            JE.BATCH_EXECUTION_ID,
            JE.STATUS,
            JE.RECORDS_PROCESSED,
            JE.WATERMARK_START,
            JE.WATERMARK_END
        FROM INGFW.LOG_JOB_EXECUTIONS JE
        JOIN INGFW.CONF_BATCH_JOBS BJ ON JE.JOB_ID = BJ.ID
        WHERE JE.ID = @ResolvedJobExecId;

        -- Result Set 3: Job Errors (if any)
        SELECT 
            ERR.JOB_INVOCATION_ID,
            ERR.ERROR_CODE,
            ERR.ERROR_MESSAGE,
            ERR.STACK_TRACE
        FROM INGFW.LOG_JOB_ERRORS ERR
        WHERE ERR.JOB_INVOCATION_ID = @ResolvedJobExecId;
    END
    ELSE
    BEGIN
        -- If neither was found, return empty schemas matching the 3 result sets
        SELECT 
            CAST(NULL AS VARCHAR(50)) AS BATCH_ID,
            CAST(NULL AS INT) AS BATCH_EXECUTION_ID,
            @InvocationId AS INVOCATION_ID,
            CAST('NOT_FOUND' AS VARCHAR(50)) AS BATCH_STATUS,
            CAST(NULL AS VARCHAR(50)) AS RECOVERY_STRATEGY,
            CAST(NULL AS DATETIME2) AS START_TIME,
            CAST(NULL AS DATETIME2) AS END_TIME,
            0 AS TOTAL_JOBS,
            0 AS SUCCESSFUL_JOBS,
            0 AS FAILED_JOBS
        WHERE 1 = 0;

        SELECT 
            CAST(NULL AS INT) AS ID,
            CAST(NULL AS VARCHAR(50)) AS SOLUTION_ID,
            CAST(NULL AS VARCHAR(50)) AS PROJECT_ID,
            CAST(NULL AS VARCHAR(50)) AS JOB_ID,
            CAST(NULL AS VARCHAR(50)) AS BATCH_ID,
            CAST(NULL AS VARCHAR(255)) AS JOB_DESCRIPTION,
            CAST(NULL AS INT) AS JOB_EXECUTION_ID,
            CAST(NULL AS INT) AS BATCH_EXECUTION_ID,
            CAST(NULL AS VARCHAR(50)) AS STATUS,
            CAST(0 AS BIGINT) AS RECORDS_PROCESSED,
            CAST(NULL AS VARCHAR(255)) AS WATERMARK_START,
            CAST(NULL AS VARCHAR(255)) AS WATERMARK_END
        WHERE 1 = 0;

        SELECT 
            CAST(NULL AS INT) AS JOB_INVOCATION_ID,
            CAST(NULL AS VARCHAR(50)) AS ERROR_CODE,
            CAST(NULL AS NVARCHAR(MAX)) AS ERROR_MESSAGE,
            CAST(NULL AS NVARCHAR(MAX)) AS STACK_TRACE
        WHERE 1 = 0;
    END;
END;
GO
