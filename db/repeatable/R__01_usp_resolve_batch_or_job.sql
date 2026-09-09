-- ==============================================================================
-- Repeatable Migration: R__01_usp_resolve_batch_or_job.sql
-- Description: Resolves batch metadata and active jobs from CONF_ tables
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_RESOLVE_BATCH_OR_JOB
    @BatchName VARCHAR(255) = NULL,
    @JobName VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @BatchName IS NOT NULL
    BEGIN
        SELECT 
            B.ID AS BATCH_ID,
            B.BATCH_NAME,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.ID AS JOB_ID,
            BJ.JOB_NAME,
            BJ.JOB_TYPE,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCHES B
        JOIN INGFW.CONF_BATCH_JOBS BJ ON B.ID = BJ.BATCH_ID
        WHERE B.BATCH_NAME = @BatchName AND B.IS_ACTIVE = 1 AND BJ.IS_ACTIVE = 1
        ORDER BY BJ.ID ASC;
    END
    ELSE IF @JobName IS NOT NULL
    BEGIN
        SELECT 
            B.ID AS BATCH_ID,
            B.BATCH_NAME,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.ID AS JOB_ID,
            BJ.JOB_NAME,
            BJ.JOB_TYPE,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCHES B
        JOIN INGFW.CONF_BATCH_JOBS BJ ON B.ID = BJ.BATCH_ID
        WHERE BJ.JOB_NAME = @JobName AND B.IS_ACTIVE = 1 AND BJ.IS_ACTIVE = 1;
    END
END;
GO
