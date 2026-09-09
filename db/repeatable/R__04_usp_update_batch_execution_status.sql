-- ==============================================================================
-- Repeatable Migration: R__04_usp_update_batch_execution_status.sql
-- Description: Finalizes batch execution status and metrics
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_UPDATE_BATCH_EXECUTION_STATUS
    @BatchExecutionId INT,
    @Status VARCHAR(50),
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE INGFW.LOG_BATCH_EXECUTIONS
    SET 
        STATUS = @Status,
        END_TIME = CURRENT_TIMESTAMP,
        ERROR_MESSAGE = @ErrorMessage,
        BI_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE ID = @BatchExecutionId;
END;
GO
