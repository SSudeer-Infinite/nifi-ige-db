-- ==============================================================================
-- Repeatable Migration: R__05_usp_create_job_execution.sql
-- Description: Records start of job pipeline execution
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_CREATE_JOB_EXECUTION
    @BatchExecutionId INT,
    @JobId UNIQUEIDENTIFIER,
    @WatermarkStart VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO INGFW.LOG_JOB_EXECUTIONS (BATCH_EXECUTION_ID, JOB_ID, STATUS, WATERMARK_START, START_TIME, BI_CREATED_DATE, BI_MODIFIED_DATE)
    VALUES (@BatchExecutionId, @JobId, 'RUNNING', @WatermarkStart, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

    SELECT CAST(SCOPE_IDENTITY() AS INT) AS JOB_EXECUTION_ID;
END;
GO
