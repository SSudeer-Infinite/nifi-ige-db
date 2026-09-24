-- ==============================================================================
-- Repeatable Migration: R__07_usp_update_job_execution_failure.sql
-- Description: Records failed job execution
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_UPDATE_JOB_EXECUTION_FAILURE
    @JobExecutionId INT,
    @WatermarkEnd VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE INGFW.LOG_JOB_EXECUTIONS
    SET 
        STATUS = 'FAILED',
        RECORDS_PROCESSED = 0,
        WATERMARK_END = @WatermarkEnd,
        END_TIME = CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE ID = @JobExecutionId;
END;
GO
