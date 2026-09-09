-- ==============================================================================
-- Repeatable Migration: R__06_usp_update_job_execution_success.sql
-- Description: Records successful completion of job pipeline
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_UPDATE_JOB_EXECUTION_SUCCESS
    @JobExecutionId INT,
    @RecordsProcessed BIGINT,
    @WatermarkEnd VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE INGFW.LOG_JOB_EXECUTIONS
    SET 
        STATUS = 'SUCCESS',
        RECORDS_PROCESSED = @RecordsProcessed,
        WATERMARK_END = @WatermarkEnd,
        END_TIME = CURRENT_TIMESTAMP,
        BI_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE ID = @JobExecutionId;
END;
GO
