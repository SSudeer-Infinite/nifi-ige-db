-- ==============================================================================
-- Repeatable Migration: R__01_usp_resolve_batch_or_job.sql
-- Description: Resolves batch metadata and active jobs from CONF_ tables
-- Parameters:
--   @SolutionId: Solution identifier (required)
--   @ProjectId:  Project identifier (required)
--   @BatchId:    Batch identifier (optional if @JobId is provided)
--   @JobId:      Job identifier (optional if @BatchId is provided)
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_RESOLVE_BATCH_OR_JOB
    @SolutionId VARCHAR(50),
    @ProjectId VARCHAR(50),
    @BatchId VARCHAR(50) = NULL,
    @JobId VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Validate that at least one of @BatchId or @JobId is supplied
    IF @JobId IS NULL AND @BatchId IS NULL
    BEGIN
        RAISERROR('Either @BatchId or @JobId must be provided.', 16, 1);
        RETURN;
    END;

    -- If JobId is provided, return that specific job within the solution and project (supports standalone jobs where BATCH_ID is NULL)
    IF @JobId IS NOT NULL
    BEGIN
        SELECT 
            B.BATCH_ID,
            B.DESCRIPTION AS BATCH_DESCRIPTION,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.ID,
            BJ.SOLUTION_ID,
            BJ.PROJECT_ID,
            BJ.JOB_ID,
            BJ.JOB_DESCRIPTION,
            BJ.JOB_TYPE,
            BJ.PIPELINE_ID,
            BJ.PREV_JOB_ID,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCH_JOBS BJ
        LEFT JOIN INGFW.CONF_BATCHES B ON BJ.BATCH_ID = B.BATCH_ID
        WHERE BJ.SOLUTION_ID = @SolutionId
          AND BJ.PROJECT_ID = @ProjectId
          AND BJ.JOB_ID = @JobId
          AND (@BatchId IS NULL OR BJ.BATCH_ID = @BatchId)
          AND BJ.IS_ACTIVE = 1 
          AND (B.IS_ACTIVE = 1 OR BJ.BATCH_ID IS NULL);
    END
    -- If BatchId is provided and no JobId, return all active jobs belonging to that batch within the solution and project
    ELSE IF @BatchId IS NOT NULL
    BEGIN
        SELECT 
            B.BATCH_ID,
            B.DESCRIPTION AS BATCH_DESCRIPTION,
            B.RECOVERY_STRATEGY,
            B.JOB_STAGGER_DELAY,
            B.MAX_TRIES,
            BJ.ID,
            BJ.SOLUTION_ID,
            BJ.PROJECT_ID,
            BJ.JOB_ID,
            BJ.JOB_DESCRIPTION,
            BJ.JOB_TYPE,
            BJ.PIPELINE_ID,
            BJ.PREV_JOB_ID,
            BJ.TRANSIENT_ERROR_RETRY_COUNT,
            BJ.CLEANUP_ON_ERROR,
            BJ.MAX_RETRY_ATTEMPTS,
            BJ.BI_CREATED_DATE,
            BJ.BI_MODIFIED_DATE
        FROM INGFW.CONF_BATCHES B
        JOIN INGFW.CONF_BATCH_JOBS BJ ON B.BATCH_ID = BJ.BATCH_ID
        WHERE BJ.SOLUTION_ID = @SolutionId
          AND BJ.PROJECT_ID = @ProjectId
          AND B.BATCH_ID = @BatchId 
          AND B.IS_ACTIVE = 1 
          AND BJ.IS_ACTIVE = 1
        ORDER BY BJ.JOB_ID ASC;
    END
END;
GO
