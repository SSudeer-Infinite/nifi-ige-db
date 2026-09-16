-- ==============================================================================
-- Repeatable Migration: R__11_usp_get_watermark_for_job.sql
-- Description: Helper for testing & monitoring
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_GET_WATERMARK_FOR_JOB
    @JobId UNIQUEIDENTIFIER = NULL,
    @JobName VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        BJ.JOB_ID,
        BJ.JOB_NAME,
        S.ID AS SOURCE_ID,
        W.LAST_WATERMARK_VAL,
        W.BI_MODIFIED_DATE
    FROM INGFW.CONF_WATERMARKS W
    JOIN INGFW.CONF_SOURCES S ON W.SOURCE_ID = S.ID
    JOIN INGFW.CONF_BATCH_JOBS BJ ON S.JOB_ID = BJ.JOB_ID
    WHERE (@JobId IS NOT NULL AND BJ.JOB_ID = @JobId)
       OR (@JobName IS NOT NULL AND BJ.JOB_NAME = @JobName);
END;
GO
