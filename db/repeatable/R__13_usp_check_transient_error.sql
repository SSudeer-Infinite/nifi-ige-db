-- ==============================================================================
-- Repeatable Migration: R__13_usp_check_transient_error.sql
-- Description: Evaluates whether an error is transient for automatic retry
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_CHECK_TRANSIENT_ERROR
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TOP 1
        CAST(1 AS BIT) AS IS_TRANSIENT,
        TE.ERROR_CODE,
        TE.ERROR_CATEGORY,
        TE.EXCEPTION_CLASS,
        TE.DESCRIPTION
    FROM INGFW.CONF_TRANSIENT_ERRORS TE
    WHERE TE.IS_ACTIVE = 1
      AND (
          @ErrorMessage LIKE TE.ERROR_PATTERN
          OR @ErrorMessage LIKE '%' + TE.EXCEPTION_CLASS + '%'
          OR @ErrorMessage LIKE '%' + TE.ERROR_CODE + '%'
      );

    -- If no row was matched above, return a non-transient record
    IF @@ROWCOUNT = 0
    BEGIN
        SELECT 
            CAST(0 AS BIT) AS IS_TRANSIENT,
            CAST(NULL AS VARCHAR(100)) AS ERROR_CODE,
            CAST(NULL AS VARCHAR(100)) AS ERROR_CATEGORY,
            CAST(NULL AS VARCHAR(255)) AS EXCEPTION_CLASS,
            CAST(NULL AS NVARCHAR(MAX)) AS DESCRIPTION;
    END
END;
GO
