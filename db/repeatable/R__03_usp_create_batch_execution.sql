-- ==============================================================================
-- Repeatable Migration: R__03_usp_create_batch_execution.sql
-- Description: Initiates batch run in BATCH_EXECUTIONS table
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_CREATE_BATCH_EXECUTION
    @BatchId INT,
    @InvocationId VARCHAR(255),
    @Status VARCHAR(50) = 'RUNNING'
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO INGFW.LOG_BATCH_EXECUTIONS (BATCH_ID, INVOCATION_ID, STATUS, START_TIME, BI_CREATED_DATE, BI_MODIFIED_DATE)
    VALUES (@BatchId, @InvocationId, @Status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

    SELECT CAST(SCOPE_IDENTITY() AS INT) AS BATCH_EXECUTION_ID;
END;
GO
