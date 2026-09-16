-- ==============================================================================
-- Repeatable Migration: R__01_usp_resolve_batch_or_job.sql
-- Description: Resolves batch metadata and active jobs from CONF_ tables
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_RESOLVE_BATCH_OR_JOB
    @BatchId INT = NULL,
    @JobId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- If JobId is provided, return that specific job (supports standalone jobs where BATCH_ID is NULL)
    IF @JobId IS NOT NULL
    BEGIN
        SELECT 
            B.ID AS BATCH_ID,
            B.BATCH_NAME,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.JOB_ID,
            BJ.JOB_NAME,
            BJ.JOB_TYPE,
            BJ.PARENT_JOB_ID,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCH_JOBS BJ
        LEFT JOIN INGFW.CONF_BATCHES B ON BJ.BATCH_ID = B.ID
        WHERE BJ.JOB_ID = @JobId 
          AND BJ.IS_ACTIVE = 1 
          AND (B.IS_ACTIVE = 1 OR BJ.BATCH_ID IS NULL);
    END
    -- If BatchId is provided and no JobId, return all active jobs belonging to that batch
    ELSE IF @BatchId IS NOT NULL
    BEGIN
        SELECT 
            B.ID AS BATCH_ID,
            B.BATCH_NAME,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.JOB_ID,
            BJ.JOB_NAME,
            BJ.JOB_TYPE,
            BJ.PARENT_JOB_ID,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCHES B
        JOIN INGFW.CONF_BATCH_JOBS BJ ON B.ID = BJ.BATCH_ID
        WHERE B.ID = @BatchId 
          AND B.IS_ACTIVE = 1 
          AND BJ.IS_ACTIVE = 1
        ORDER BY BJ.JOB_NAME ASC;
    END
END;
GO
