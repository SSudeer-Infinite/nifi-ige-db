-- ==============================================================================
-- Repeatable Migration: R__14_usp_get_transient_errors.sql
-- Description: Lists all active transient errors for pipeline caching
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_GET_TRANSIENT_ERRORS
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        ID,
        ERROR_CODE,
        ERROR_CATEGORY,
        EXCEPTION_CLASS,
        ERROR_PATTERN,
        DESCRIPTION,
        IS_ACTIVE,
        BI_CREATED_DATE,
        BI_MODIFIED_DATE
    FROM INGFW.CONF_TRANSIENT_ERRORS
    WHERE IS_ACTIVE = 1
    ORDER BY ID ASC;
END;
GO
