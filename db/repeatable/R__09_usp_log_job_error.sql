-- ==============================================================================
-- Repeatable Migration: R__09_usp_log_job_error.sql
-- Description: Centralized error logging
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_LOG_JOB_ERROR
    @BatchExecutionId INT = NULL,
    @JobExecutionId INT = NULL,
    @ErrorCode VARCHAR(50),
    @ErrorMessage NVARCHAR(MAX),
    @StackTrace NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO INGFW.LOG_JOB_ERRORS (BATCH_INVOCATION_ID, JOB_INVOCATION_ID, ERROR_CODE, ERROR_MESSAGE, STACK_TRACE, BI_CREATED_DATE, BI_MODIFIED_DATE)
    VALUES (@BatchExecutionId, @JobExecutionId, @ErrorCode, @ErrorMessage, @StackTrace, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
END;
GO
