-- ==============================================================================
-- Repeatable Migration: R__08_usp_advance_watermark.sql
-- Description: Commits watermark update (Two-Phase Commit)
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_ADVANCE_WATERMARK
    @SourceId INT,
    @WatermarkState NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE INGFW.CONF_WATERMARKS
    SET 
        WATERMARK_STATE = @WatermarkState,
        BI_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE SOURCE_ID = @SourceId;

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO INGFW.CONF_WATERMARKS (SOURCE_ID, WATERMARK_STATE, BI_CREATED_DATE, BI_MODIFIED_DATE)
        VALUES (@SourceId, @WatermarkState, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    END;
END;
GO
