-- ==============================================================================
-- Repeatable Migration: R__08_usp_advance_watermark.sql
-- Description: Commits watermark update (Two-Phase Commit)
-- ==============================================================================

CREATE OR ALTER PROCEDURE INGFW.USP_ADVANCE_WATERMARK
    @SourceId INT,
    @NewWatermarkVal BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE INGFW.CONF_WATERMARKS
    SET 
        LAST_WATERMARK_VAL = @NewWatermarkVal,
        BI_MODIFIED_DATE = CURRENT_TIMESTAMP
    WHERE SOURCE_ID = @SourceId;
END;
GO
