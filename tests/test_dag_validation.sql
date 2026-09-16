-- ==============================================================================
-- test_dag_validation.sql: Cycle detection test for BATCH_JOBS parent-child DAG
-- ==============================================================================

WITH JobHierarchy AS (
    -- Anchor: root jobs (no parent)
    SELECT 
        JOB_ID,
        JOB_NAME,
        PARENT_JOB_ID,
        0 AS Depth,
        CAST(JOB_ID AS VARCHAR(MAX)) AS Path
    FROM INGFW.CONF_BATCH_JOBS
    WHERE PARENT_JOB_ID IS NULL

    UNION ALL

    -- Recursive member
    SELECT 
        child.JOB_ID,
        child.JOB_NAME,
        child.PARENT_JOB_ID,
        parent.Depth + 1,
        parent.Path + '->' + CAST(child.JOB_ID AS VARCHAR(MAX))
    FROM INGFW.CONF_BATCH_JOBS child
    JOIN JobHierarchy parent ON child.PARENT_JOB_ID = parent.JOB_ID
    WHERE parent.Path NOT LIKE '%' + CAST(child.JOB_ID AS VARCHAR(MAX)) + '%'
      AND parent.Depth < 50
)
SELECT 
    JOB_ID, 
    JOB_NAME, 
    PARENT_JOB_ID, 
    Depth, 
    Path
FROM JobHierarchy;
GO
