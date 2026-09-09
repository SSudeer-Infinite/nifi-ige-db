-- ==============================================================================
-- Repeatable Migration: R__12_usp_get_execution_logging_summary.sql
-- Description: Helper to audit execution logs
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_GET_EXECUTION_LOGGING_SUMMARY
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        (SELECT COUNT(*) FROM INGFW.LOG_BATCH_EXECUTIONS) AS TOTAL_BATCH_EXECUTIONS,
        (SELECT COUNT(*) FROM INGFW.LOG_JOB_EXECUTIONS) AS TOTAL_JOB_EXECUTIONS,
        (SELECT COUNT(*) FROM INGFW.LOG_JOB_ERRORS) AS TOTAL_JOB_ERRORS;
END;
GO
