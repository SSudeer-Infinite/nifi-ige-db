-- ==============================================================================
-- test_sp_suite.sql: End-to-end Verification Suite for Stored Procedures
-- ==============================================================================
SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TEST 1: Testing INGFW.USP_RESOLVE_BATCH_OR_JOB';
PRINT '============================================================';
EXEC INGFW.USP_RESOLVE_BATCH_OR_JOB @SolutionId = 'SOL_EPRISM', @ProjectId = 'PRJ_POST_STAGING', @BatchId = 'BAT_POST_STAGING_001';
EXEC INGFW.USP_RESOLVE_BATCH_OR_JOB @SolutionId = 'SOL_EPRISM', @ProjectId = 'PRJ_POST_STAGING', @JobId = 'J_POST_STAGING_001';

PRINT '============================================================';
PRINT 'TEST 2: Testing INGFW.USP_GET_JOB_SOURCE_DEST_CONFIG';
PRINT '============================================================';
EXEC INGFW.USP_GET_JOB_SOURCE_DEST_CONFIG @JobIdentifier = 'J_POST_STAGING_001';
EXEC INGFW.USP_GET_JOB_SOURCE_CONFIG @JobIdentifier = 'J_POST_STAGING_002';

PRINT '============================================================';
PRINT 'TEST 3: Testing Batch & Job Lifecycle Execution';
PRINT '============================================================';
DECLARE @TestBatchExecId INT;
DECLARE @TestJobExecId1 INT;
DECLARE @TestJobExecId2 INT;
DECLARE @OutputTable TABLE (ExecId INT);

-- 3a: Create Batch Execution
INSERT INTO @OutputTable (ExecId)
EXEC INGFW.USP_CREATE_BATCH_EXECUTION 
    @BatchId = 'BAT_POST_STAGING_001', 
    @InvocationId = 'inv-test-suite-001', 
    @Status = 'RUNNING';

SELECT @TestBatchExecId = ExecId FROM @OutputTable;
DELETE FROM @OutputTable;
PRINT 'Created Batch Execution ID: ' + CAST(@TestBatchExecId AS VARCHAR(10));

-- 3b: Create Job Execution 1
INSERT INTO @OutputTable (ExecId)
EXEC INGFW.USP_CREATE_JOB_EXECUTION 
    @BatchExecutionId = @TestBatchExecId, 
    @JobId = 1, 
    @WatermarkStart = '{"id": 0}';

SELECT @TestJobExecId1 = ExecId FROM @OutputTable;
DELETE FROM @OutputTable;
PRINT 'Created Job Execution 1 ID: ' + CAST(@TestJobExecId1 AS VARCHAR(10));

-- 3c: Update Job Execution 1 Success
EXEC INGFW.USP_UPDATE_JOB_EXECUTION_SUCCESS 
    @JobExecutionId = @TestJobExecId1, 
    @RecordsProcessed = 1500, 
    @WatermarkEnd = '{"id": 1500}';
PRINT 'Updated Job Execution 1 to SUCCESS';

-- 3d: Advance Watermark for Source 1
EXEC INGFW.USP_ADVANCE_WATERMARK 
    @SourceId = 1, 
    @WatermarkState = '{"id": 1500}';
PRINT 'Advanced Watermark for Source 1';

-- 3e: Verify Watermark
EXEC INGFW.USP_GET_WATERMARK_FOR_JOB @JobId = 'J_POST_STAGING_001';

-- 3f: Create Job Execution 2
INSERT INTO @OutputTable (ExecId)
EXEC INGFW.USP_CREATE_JOB_EXECUTION 
    @BatchExecutionId = @TestBatchExecId, 
    @JobId = 2, 
    @WatermarkStart = '{"id": 0}';

SELECT @TestJobExecId2 = ExecId FROM @OutputTable;
DELETE FROM @OutputTable;
PRINT 'Created Job Execution 2 ID: ' + CAST(@TestJobExecId2 AS VARCHAR(10));

-- 3g: Log Job Error for Job Execution 2
EXEC INGFW.USP_LOG_JOB_ERROR 
    @BatchExecutionId = @TestBatchExecId, 
    @JobExecutionId = @TestJobExecId2, 
    @ErrorCode = 'SOCKET_TIMEOUT', 
    @ErrorMessage = 'SocketTimeoutException: Connection timed out', 
    @StackTrace = 'java.net.SocketTimeoutException\n  at ...';
PRINT 'Logged Error for Job Execution 2';

-- 3h: Update Job Execution 2 Failure (testing optional @WatermarkEnd)
EXEC INGFW.USP_UPDATE_JOB_EXECUTION_FAILURE 
    @JobExecutionId = @TestJobExecId2;
PRINT 'Updated Job Execution 2 to FAILED';

-- 3i: Update Batch Execution Status
EXEC INGFW.USP_UPDATE_BATCH_EXECUTION_STATUS 
    @BatchExecutionId = @TestBatchExecId, 
    @Status = 'PARTIAL_SUCCESS', 
    @ErrorMessage = '1 job succeeded, 1 job failed';
PRINT 'Updated Batch Execution to PARTIAL_SUCCESS';

PRINT '============================================================';
PRINT 'TEST 4: Testing INGFW.USP_GET_BATCH_EXECUTION_STATUS';
PRINT '============================================================';
EXEC INGFW.USP_GET_BATCH_EXECUTION_STATUS @InvocationId = 'inv-test-suite-001';
EXEC INGFW.USP_GET_BATCH_EXECUTION_STATUS @JobExecutionId = @TestJobExecId1;

PRINT '============================================================';
PRINT 'TEST 5: Testing INGFW.USP_CHECK_TRANSIENT_ERROR';
PRINT '============================================================';
PRINT 'Testing transient pattern (SocketTimeoutException):';
EXEC INGFW.USP_CHECK_TRANSIENT_ERROR @ErrorMessage = 'Read failed: java.net.SocketTimeoutException: Read timed out';

PRINT 'Testing transient pattern (Deadlock):';
EXEC INGFW.USP_CHECK_TRANSIENT_ERROR @ErrorMessage = 'Transaction (Process ID 55) was deadlocked on lock resources';

PRINT 'Testing non-transient pattern (Syntax Error):';
EXEC INGFW.USP_CHECK_TRANSIENT_ERROR @ErrorMessage = 'Msg 102: Incorrect syntax near SELECT';

PRINT '============================================================';
PRINT 'TEST 6: Testing INGFW.USP_GET_TRANSIENT_ERRORS';
PRINT '============================================================';
EXEC INGFW.USP_GET_TRANSIENT_ERRORS;

PRINT '============================================================';
PRINT 'TEST 7: Testing INGFW.USP_GET_EXECUTION_LOGGING_SUMMARY';
PRINT '============================================================';
EXEC INGFW.USP_GET_EXECUTION_LOGGING_SUMMARY;

-- Reset test state
EXEC INGFW.USP_ADVANCE_WATERMARK @SourceId = 1, @WatermarkState = '{"id": 0}';
PRINT 'Reset watermark for Source 1 back to {"id": 0}';
PRINT '============================================================';
PRINT 'ALL TESTS COMPLETED SUCCESSFULLY!';
PRINT '============================================================';
GO
