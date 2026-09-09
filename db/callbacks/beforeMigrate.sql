-- ==============================================================================
-- Flyway Callback: beforeMigrate.sql
-- Runs before migration batch begins
-- ==============================================================================

PRINT 'Starting Flyway migration execution for metadata_db...';

-- Ensure INGFW schema exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'INGFW')
BEGIN
    EXEC('CREATE SCHEMA INGFW');
    PRINT 'Created schema INGFW.';
END
GO
