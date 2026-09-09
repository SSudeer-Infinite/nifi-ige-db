-- ==============================================================================
-- Repeatable Migration: R__02_usp_get_job_source_config.sql
-- Description: Retrieves source connection, table, watermark config
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_GET_JOB_SOURCE_CONFIG
    @JobId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        S.ID AS SOURCE_ID,
        S.SOURCE_PROPERTIES,
        S.WATERMARK_FIELD,
        S.WATERMARK_TYPE,
        S.CHUNK_SIZE,
        S.THROTTLE_RATE,
        ISNULL(W.LAST_WATERMARK_VAL, 0) AS LAST_WATERMARK_VAL,
        W.LAST_WATERMARK_TIMESTAMP,
        W.LAST_WATERMARK_STR,
        S.BI_CREATED_DATE,
        S.BI_MODIFIED_DATE
    FROM INGFW.CONF_SOURCES S
    LEFT JOIN INGFW.CONF_WATERMARKS W ON S.ID = W.SOURCE_ID
    WHERE S.JOB_ID = @JobId;
END;
GO
