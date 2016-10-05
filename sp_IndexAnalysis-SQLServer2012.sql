USE [master];
GO

IF OBJECT_ID('dbo.sp_IndexAnalysis') IS NOT NULL
    DROP PROCEDURE [dbo].[sp_IndexAnalysis];
GO

/********************************************************************************************* 
Index Analysis Script - SQL Server 2008
(C) 2013, Jason Strate

Feedback:
    mailto:jasonstrate@gmail.com 
    http://www.jasonstrate.com

License: 
   This query is free to download and use for personal, educational, and internal 
   corporate purposes, provided that this header is preserved. Redistribution or sale 
   of this query, in whole or in part, is prohibited without the author's express 
   written consent. 

More details:
	https://github.com/StrateSQL/sqlserver_indexanalysis

*********************************************************************************************/ 
CREATE PROCEDURE [dbo].[sp_IndexAnalysis]
(
 @TableName NVARCHAR(256) = NULL ,
 @IncludeMemoryDetails BIT = 1 ,
 @IncludeMissingIndexes BIT = 1 ,
 @IncludeMissingFKIndexes BIT = 1 ,
 @ConsolidatePartitionStats BIT = 1 ,
 @Output VARCHAR(20) = 'DUMP' ,
 @PageCompressionThreshold INT = 1000 ,
 @RowCompressionThreshold DECIMAL(4, 2) = 1 ,
 @CheckCompression BIT = 1 ,
 @ReadOnlyDatabase BIT = 0 ,
 @MaxMissingIndexCount TINYINT = 5 ,
 @MinLookupThreshold INT = 1000 ,
 @MinScanThreshold INT = 100 ,
 @Scan2SeekRatio INT = 1000 ,
 @ProcessingMessages BIT = 0
 )
    WITH RECOMPILE
AS
BEGIN
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET NOCOUNT ON;
 
    DECLARE @ERROR_MESSAGE NVARCHAR(2048) ,
        @ERROR_SEVERITY INT ,
        @ERROR_STATE INT ,
        @PROCESSING_START DATETIME;
 
    DECLARE @SQL NVARCHAR(MAX) ,
        @DB_ID INT ,
        @ObjectID INT ,
        @DatabaseName NVARCHAR(MAX);
 
    BEGIN TRY
--================================================================================================
-- Remove temporary tables	
--================================================================================================
        IF OBJECT_ID('tempdb..#MemoryBuffer') IS NOT NULL
            DROP TABLE [#MemoryBuffer];

        IF OBJECT_ID('tempdb..#TableMeta') IS NOT NULL
            DROP TABLE [#TableMeta];

        IF OBJECT_ID('tempdb..#IndexMeta') IS NOT NULL
            DROP TABLE [#IndexMeta];

        IF OBJECT_ID('tempdb..#IndexStatistics') IS NOT NULL
            DROP TABLE [#IndexStatistics];

        IF OBJECT_ID('tempdb..#ForeignKeys') IS NOT NULL
            DROP TABLE [#ForeignKeys];
				
        IF @Output NOT IN ('DUMP', 'DETAILED', 'DUPLICATE', 'OVERLAPPING', 'REALIGN')
            RAISERROR('The value "%s" provided for the @Output parameter is not valid',16,1,@Output);
 
        SELECT  @DB_ID = DB_ID() ,
                @ObjectID = OBJECT_ID(QUOTENAME(DB_NAME(DB_ID())) + '.' + COALESCE(QUOTENAME(PARSENAME(@TableName, 2)), '') + '.'
                                      + QUOTENAME(PARSENAME(@TableName, 1))) ,
                @DatabaseName = QUOTENAME(DB_NAME(DB_ID()));

        IF @TableName IS NOT NULL
            AND @ObjectID IS NULL
            RAISERROR('The object "%s" could not be found.  Execution cancelled.',16,1,@TableName);

-- Obtain memory buffer information on database objects
        CREATE TABLE [#MemoryBuffer]
        (
         [database_id] INT ,
         [object_id] INT ,
         [index_id] INT ,
         [partition_number] INT ,
         [buffered_page_count] INT ,
         [buffered_mb] DECIMAL(12, 2)
        );

        IF @IncludeMemoryDetails = 1
        BEGIN
            SET @PROCESSING_START = GETDATE();
            SET @SQL = 'WITH AllocationUnits
		AS (
			SELECT p.object_id
				,p.index_id
				,CASE WHEN @ConsolidatePartitionStats = 0 THEN p.partition_number ELSE -1 END AS partition_number
				,au.allocation_unit_id
			FROM ' + @DatabaseName + '.sys.allocation_units AS au
				INNER JOIN ' + @DatabaseName + '.sys.partitions AS p ON au.container_id = p.hobt_id AND (au.type = 1 OR au.type = 3)
			UNION ALL
			SELECT p.object_id
				,p.index_id
				,CASE WHEN @ConsolidatePartitionStats = 0 THEN p.partition_number ELSE -1 END AS partition_number 
				,au.allocation_unit_id
			FROM ' + @DatabaseName + '.sys.allocation_units AS au
				INNER JOIN ' + @DatabaseName + '.sys.partitions AS p ON au.container_id = p.partition_id AND au.type = 2
		)
		SELECT DB_ID()
			,au.object_id
			,au.index_id
			,au.partition_number
			,COUNT(*) AS buffered_page_count
			,CONVERT(DECIMAL(12,2), CAST(COUNT(*) as bigint)*CAST(8 as float)/1024) as buffer_mb
		FROM ' + @DatabaseName + '.sys.dm_os_buffer_descriptors AS bd 
			INNER JOIN AllocationUnits au ON bd.allocation_unit_id = au.allocation_unit_id
		WHERE bd.database_id = db_id()
		GROUP BY au.object_id, au.index_id, au.partition_number		
		';
		
            BEGIN TRY
                IF @ConsolidatePartitionStats = 1
                BEGIN
                    RAISERROR('Strate''s Warning: Buffered memory totals are an aggregate for all partitions on the table. This behaviour is controlled by the @ConsolidatePartitionStats parameter',10,1) WITH NOWAIT;
                    PRINT '';
                END;
                
                INSERT  INTO [#MemoryBuffer]
                        EXEC [sys].[sp_executesql] @SQL, N'@ConsolidatePartitionStats BIT', @ConsolidatePartitionStats = @ConsolidatePartitionStats;

                IF @ProcessingMessages = 1
                    PRINT 'Processing #MemoryBuffer... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
            END TRY
            BEGIN CATCH
                SELECT  @ERROR_MESSAGE = 'Populate #MemoryBuffer (Line ' + CAST(ERROR_LINE() AS NVARCHAR(25)) + '): ' + ERROR_MESSAGE() ,
                        @ERROR_SEVERITY = ERROR_SEVERITY() ,
                        @ERROR_STATE = ERROR_STATE();
     
                RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
            END CATCH;
        END;

-- Obtain index meta data information
        BEGIN
            SET @PROCESSING_START = GETDATE();

            CREATE TABLE [#TableMeta]
            (
             [database_id] SMALLINT ,
             [schema_id] INT ,
             [schema_name] NVARCHAR(128) ,
             [object_id] INT ,
             [table_name] NVARCHAR(128) ,
             [object_name] NVARCHAR(260) ,
             [table_column_count] SMALLINT ,
             [table_row_count] BIGINT ,
             [has_unique] BIT
            );

            SET @SQL = N'SELECT
			DB_ID()
			, s.schema_id
			, s.name
			, t.object_id
			, t.name
			, QUOTENAME(s.name)+''.''+QUOTENAME(t.name)
			, c2.table_column_count
			, ps2.row_count
			, CASE WHEN i2.is_unique > 0 THEN 1 ELSE 0 END
		FROM ' + @DatabaseName + '.sys.tables t
			INNER JOIN ' + @DatabaseName + '.sys.schemas s ON t.schema_id = s.schema_id
			CROSS APPLY (SELECT SUM(row_count) AS row_count FROM ' + @DatabaseName
                + '.sys.dm_db_partition_stats ps WHERE t.object_id = ps.object_id AND ps.index_id IN (0,1)) ps2
			CROSS APPLY (SELECT COUNT(*) AS table_column_count FROM ' + @DatabaseName + '.sys.columns c1 WHERE t.object_id = c1.object_id) c2
			CROSS APPLY (SELECT COUNT(*) AS is_unique FROM ' + @DatabaseName + '.sys.indexes i1 WHERE t.object_id = i1.object_id AND is_unique = 1) i2';
 
            IF @ObjectID IS NOT NULL
                SET @SQL = @SQL + CHAR(13) + 'WHERE t.object_id = @ObjectID ';

            BEGIN TRY
                INSERT  INTO [#TableMeta]
                        EXEC [sys].[sp_executesql] @SQL, N'@DB_ID INT, @ObjectID INT', @DB_ID = @DB_ID, @ObjectID = @ObjectID;

                IF @ProcessingMessages = 1
                    PRINT 'Processing #TableMeta... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
            END TRY
            BEGIN CATCH
                SELECT  @ERROR_MESSAGE = 'Populate #TableMeta (Line ' + CAST(ERROR_LINE() AS NVARCHAR(25)) + '): ' + ERROR_MESSAGE() ,
                        @ERROR_SEVERITY = ERROR_SEVERITY() ,
                        @ERROR_STATE = ERROR_STATE();
     
                RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
            END CATCH; 
        END;

        BEGIN
            SET @PROCESSING_START = GETDATE();

            CREATE TABLE [#IndexMeta]
            (
             [database_id] SMALLINT ,
             [object_id] INT ,
             [filegroup_name] NVARCHAR(128) ,
             [compression_type] NVARCHAR(128) ,
             [index_id] INT ,
             [index_name] NVARCHAR(128) ,
             [partition_count] SMALLINT ,
             [partition_number] INT ,
             [is_primary_key] BIT ,
             [is_unique] BIT ,
             [is_disabled] BIT ,
             [type_desc] NVARCHAR(128) ,
             [fill_factor] TINYINT ,
             [is_padded] BIT ,
             [reserved_page_count] BIGINT ,
             [used_page_count] BIGINT ,
             [size_in_mb] DECIMAL(12, 2) ,
             [index_row_count] BIGINT ,
             [filter_definition] NVARCHAR(MAX) ,
             [indexed_columns] NVARCHAR(MAX) ,
             [indexed_column_count] SMALLINT ,
             [included_columns] NVARCHAR(MAX) ,
             [included_column_count] SMALLINT ,
             [key_columns] NVARCHAR(MAX) ,
             [data_columns] NVARCHAR(MAX) ,
             [indexed_columns_ids] NVARCHAR(1024) ,
             [included_column_ids] NVARCHAR(1024) ,
             [distinct_indexed_columns_ids] NVARCHAR(1024)
            );

 
            SET @SQL = N'SELECT
			database_id = DB_ID()
			, object_id = t.object_id
			, filegroup = ds.name
			, x.data_compression_desc
			, i.index_id
			, index_name = COALESCE(i.name, ''N/A'')
            , x.partition_count
			, x.partition_number
			, i.is_primary_key
			, i.is_unique
			, i.is_disabled
			, type_desc = CASE WHEN i.is_unique = 1 THEN ''UNIQUE '' ELSE '''' END + i.type_desc
			, i.fill_factor
			, i.is_padded
			, x.reserved_page_count
            , x.used_page_count
			, size_in_mb = CAST(reserved_page_count * CAST(8 as float) / 1024 as DECIMAL(12,2)) 
			, row_count
			, i.filter_definition
			, indexed_columns = STUFF((
					SELECT '', '' + QUOTENAME(c.name)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 0
					ORDER BY key_ordinal ASC
					FOR XML PATH('''')), 1, 2, '''')
			, indexed_column_count = (
					SELECT COUNT(*)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 0)
			, included_columns = STUFF((
					SELECT '', '' + QUOTENAME(c.name)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 1
					ORDER BY key_ordinal ASC
					FOR XML PATH('''')), 1, 2, '''') 
			, included_column_count = (
					SELECT COUNT(*)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 1)
			, key_columns = STUFF((
					SELECT '', '' + QUOTENAME(c.name)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 0
					ORDER BY key_ordinal ASC
					FOR XML PATH(''''))
					+ COALESCE((SELECT '', '' + QUOTENAME(c.name)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
						LEFT OUTER JOIN ' + @DatabaseName + '.sys.index_columns ic_key ON c.object_id = ic_key.object_id 
							AND c.column_id = ic_key.column_id 
							AND i.index_id = ic_key.index_id
							AND ic_key.is_included_column = 0
					WHERE i.object_id = ic.object_id
					AND ic.index_id = 1
					AND ic.is_included_column = 0
					AND ic_key.index_id IS NULL
					ORDER BY ic.key_ordinal ASC
					FOR XML PATH('''')),''''), 1, 2, '''')
			, data_columns = CASE WHEN i.index_id IN (0,1) THEN ''ALL-COLUMNS'' ELSE STUFF((
					SELECT '', '' + QUOTENAME(c.name)
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
						LEFT OUTER JOIN ' + @DatabaseName
                + '.sys.index_columns ic_key ON c.object_id = ic_key.object_id AND c.column_id = ic_key.column_id AND ic_key.index_id = 1
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND ic.is_included_column = 1
					AND ic_key.index_id IS NULL
					ORDER BY ic.key_ordinal ASC
					FOR XML PATH('''')), 1, 2, '''') END
			, indexed_column_ids = (SELECT QUOTENAME(CAST(ic.column_id AS VARCHAR(10)) 
						+ CASE WHEN ic.is_descending_key = 0 THEN ''+'' ELSE ''-'' END,''('')
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND is_included_column = 0
					ORDER BY key_ordinal ASC
					FOR XML PATH(''''))
					+ ''|'' + COALESCE((SELECT QUOTENAME(CAST(ic.column_id AS VARCHAR(10)) 
						+ CASE WHEN ic.is_descending_key = 0 THEN ''+'' ELSE ''-'' END, ''('')
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
						LEFT OUTER JOIN ' + @DatabaseName + '.sys.index_columns ic_key ON c.object_id = ic_key.object_id 
							AND c.column_id = ic_key.column_id 
							AND i.index_id = ic_key.index_id
							AND ic_key.is_included_column = 0
					WHERE i.object_id = ic.object_id
					AND ic.index_id = 1
					AND ic.is_included_column = 0
					AND ic_key.index_id IS NULL
					ORDER BY ic.key_ordinal ASC
					FOR XML PATH('''')),'''')
				+ CASE WHEN i.is_unique = 1 THEN ''U'' ELSE '''' END
			, included_column_ids = CASE WHEN i.index_id IN (0,1) THEN ''ALL-COLUMNS'' ELSE
					COALESCE((SELECT QUOTENAME(ic.column_id,''('')
					FROM ' + @DatabaseName + '.sys.index_columns ic
						INNER JOIN ' + @DatabaseName + '.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
						LEFT OUTER JOIN ' + @DatabaseName
                + '.sys.index_columns ic_key ON c.object_id = ic_key.object_id AND c.column_id = ic_key.column_id AND ic_key.index_id = 1
					WHERE i.object_id = ic.object_id
					AND i.index_id = ic.index_id
					AND ic.is_included_column = 1
					AND ic_key.index_id IS NULL
					ORDER BY ic.key_ordinal ASC
					FOR XML PATH('''')), SPACE(0)) END
			, distinct_indexed_columns_ids = (SELECT QUOTENAME(ic.column_id)
                    FROM ' + @DatabaseName + '.sys.index_columns ic
                    INNER JOIN ' + @DatabaseName + '.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE ic.object_id = i.object_id 
                    AND (ic.index_id = i.index_id OR ic.index_id = 1)
                    AND is_included_column = 0
                    GROUP BY ic.column_id
                    ORDER BY ic.column_id
                    FOR XML PATH(''''))
		FROM ' + @DatabaseName + '.sys.tables t
			INNER JOIN ' + @DatabaseName + '.sys.schemas s ON t.schema_id = s.schema_id
			INNER JOIN ' + @DatabaseName + '.sys.indexes i ON t.object_id = i.object_id
			INNER JOIN ' + @DatabaseName + '.sys.data_spaces ds ON i.data_space_id = ds.data_space_id
            CROSS APPLY (SELECT p.object_id ,
                    p.index_id ,
                    CASE WHEN @ConsolidatePartitionStats = 0 THEN p.partition_number ELSE -1 END AS partition_number,
                    COUNT(*) AS partition_count,
                    SUM(ps.row_count) AS row_count,       
                    CASE 1.*SUM(p.data_compression)/NULLIF(COUNT(*),0)
                        WHEN 0 THEN ''NONE''
                        WHEN 1 THEN ''ROW''
                        WHEN 2 THEN ''PAGE''
                        ELSE ''MIXED'' END AS data_compression_desc,   
                    SUM(ps.in_row_data_page_count) AS in_row_data_page_count,
                    SUM(ps.in_row_used_page_count) AS in_row_used_page_count ,
                    SUM(ps.in_row_reserved_page_count) AS in_row_reserved_page_count,
                    SUM(ps.lob_used_page_count) AS lob_used_page_count,
                    SUM(ps.lob_reserved_page_count) AS lob_reserved_page_count,
                    SUM(ps.row_overflow_used_page_count) AS row_overflow_used_page_count,
                    SUM(ps.row_overflow_reserved_page_count) AS row_overflow_reserved_page_count,
                    SUM(ps.used_page_count) AS used_page_count ,
                    SUM(ps.reserved_page_count) AS reserved_page_count
                FROM ' + @DatabaseName + '.sys.partitions p 
                    INNER JOIN ' + @DatabaseName
                + '.sys.dm_db_partition_stats ps ON ps.object_id = p.object_id AND ps.index_id = p.index_id AND ps.partition_id = p.partition_id
                WHERE i.object_id = p.object_id AND i.index_id = p.index_id
                GROUP BY p.object_id, p.index_id, CASE WHEN @ConsolidatePartitionStats = 0 THEN p.partition_number ELSE -1 END) x
                ';
 
            IF @ObjectID IS NOT NULL
                SET @SQL = @SQL + CHAR(13) + 'WHERE t.object_id = @ObjectID ';
 
            BEGIN TRY
                IF @ConsolidatePartitionStats = 1
                BEGIN
                    RAISERROR('Strate''s Warning: Page count totals are a summary of all partitions. This behaviour is controlled by the @ConsolidatePartitionStats parameter',10,1) WITH NOWAIT;
                    PRINT '';
                END;

                INSERT  INTO [#IndexMeta]
                        EXEC [sys].[sp_executesql] @SQL, N'@DB_ID INT, @ObjectID INT, @ConsolidatePartitionStats INT', @DB_ID = @DB_ID, @ObjectID = @ObjectID,
                            @ConsolidatePartitionStats = @ConsolidatePartitionStats;

                IF @ProcessingMessages = 1
                    PRINT 'Processing #IndexMeta... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
            END TRY
            BEGIN CATCH
                SELECT  @ERROR_MESSAGE = 'Populate #IndexMeta (Line ' + CAST(ERROR_LINE() AS NVARCHAR(25)) + '): ' + ERROR_MESSAGE() ,
                        @ERROR_SEVERITY = ERROR_SEVERITY() ,
                        @ERROR_STATE = ERROR_STATE();
     
                RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
            END CATCH;
        END;

        BEGIN
            SET @PROCESSING_START = GETDATE();

            IF @ConsolidatePartitionStats = 1
            BEGIN
                RAISERROR('Strate''s Warning: Index operational stats (locking, blocking, latching, etc.) are summarized from all partitions. This behaviour is controlled by the @ConsolidatePartitionStats parameter',10,1) WITH NOWAIT;
                PRINT '';
            END;
            ELSE
            BEGIN
                RAISERROR('Strate''s Warning: Index usage stats are summarized at the index level, per partition data is not available.',10,1) WITH NOWAIT;
                PRINT '';
            END;

            SELECT  IDENTITY( INT,1,1 ) AS [row_id] ,
                    CAST('' AS VARCHAR(10)) AS [index_action] ,
                    CAST('' AS VARCHAR(50)) AS [index_pros] ,
                    CAST('' AS VARCHAR(50)) AS [index_cons] ,
                    [tm].[database_id] ,
                    [im].[filegroup_name] ,
                    [im].[compression_type] ,
                    [tm].[schema_name] ,
                    [im].[object_id] ,
                    [tm].[table_name] ,
                    [tm].[object_name] ,
                    [im].[index_id] ,
                    [im].[index_name] ,
                    [im].[is_primary_key] ,
                    [im].[is_unique] ,
                    [im].[is_disabled] ,
                    [tm].[has_unique] ,
                    [im].[type_desc] ,
                    [im].[partition_count] ,
                    [im].[partition_number] ,
                    [im].[fill_factor] ,
                    [im].[is_padded] ,
                    [im].[reserved_page_count] ,
                    [im].[used_page_count] ,
                    [im].[index_row_count] / NULLIF([im].[used_page_count], 0) AS [average_rows_per_page] ,
                    [im].[size_in_mb] ,
                    COALESCE([mb].[buffered_page_count], 0) AS [buffered_page_count] ,
                    COALESCE([mb].[buffered_mb], 0) AS [buffered_mb] ,
                    CAST(0 AS INT) AS [table_buffered_mb] ,
                    COALESCE(CAST(100. * [mb].[buffered_page_count] / NULLIF([im].[reserved_page_count], 0) AS DECIMAL(12, 2)), 0) AS [buffered_percent] ,
                    [tm].[table_row_count] ,
                    [im].[index_row_count] ,
                    ROW_NUMBER() OVER (PARTITION BY [im].[object_id] ORDER BY [im].[is_primary_key] DESC, [ius].[user_seeks] + [ius].[user_scans]
                                       + [ius].[user_lookups] DESC) AS [index_rank] ,
                    CAST(0 AS INT) AS [full_index_rank] ,
                    [ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups] AS [user_total] ,
                    COALESCE(CAST(100 * ([ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups])
                             / (NULLIF(SUM([ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups]) OVER (PARTITION BY [im].[object_id]), 0) * 1.) AS DECIMAL(6,
                                                                                                                                                2)), 0) AS [user_total_pct] ,
                    CAST(0 AS DECIMAL(6, 2)) AS [estimated_user_total_pct] ,
                    CAST(0 AS FLOAT) AS [missing_index_impact] -- Dick Baker 201303 (INT range not big enough and is f.p. anyway)
                    ,
                    [ius].[user_seeks] ,
                    [ius].[user_scans] ,
                    [ius].[user_lookups] ,
                    [ius].[user_updates] ,
                    (1. * ([ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups])) / NULLIF([ius].[user_updates], 0) AS [read_to_update_ratio] ,
                    ([ios].[leaf_insert_count] + [ios].[leaf_delete_count] + [ios].[leaf_update_count] + [ios].[leaf_ghost_count]) / NULLIF([ius].[user_updates],
                                                                                                                                            0) AS [average_rows_per_update] ,
                    CASE WHEN [ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups] >= [ius].[user_updates]
                         THEN CEILING(1. * ([ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups]) / COALESCE(NULLIF([ius].[user_seeks], 0), 1))
                         ELSE 0
                    END AS [read_to_update] ,
                    CASE WHEN [ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups] <= [ius].[user_updates]
                         THEN CEILING(1. * ([ius].[user_updates]) / COALESCE(NULLIF([ius].[user_seeks] + [ius].[user_scans] + [ius].[user_lookups], 0), 1))
                         ELSE 0
                    END AS [update_to_read] ,
                    [ios].[row_lock_count] ,
                    [ios].[row_lock_wait_count] ,
                    [ios].[row_lock_wait_in_ms] ,
                    CAST(100.0 * [ios].[row_lock_wait_count] / NULLIF([ios].[row_lock_count], 0) AS DECIMAL(12, 2)) AS [row_block_pct] ,
                    CAST(1. * [ios].[row_lock_wait_in_ms] / NULLIF([ios].[row_lock_wait_count], 0) AS DECIMAL(12, 2)) AS [avg_row_lock_waits_ms] ,
                    [ios].[page_latch_wait_count] ,
                    CAST(1. * [ios].[page_latch_wait_in_ms] / NULLIF([ios].[page_io_latch_wait_count], 0) AS DECIMAL(12, 2)) AS [avg_page_latch_wait_ms] ,
                    [ios].[page_io_latch_wait_count] ,
                    CAST(1. * [ios].[page_io_latch_wait_in_ms] / NULLIF([ios].[page_io_latch_wait_count], 0) AS DECIMAL(12, 2)) AS [avg_page_io_latch_wait_ms] ,
                    [ios].[tree_page_latch_wait_count] AS [tree_page_latch_wait_count] ,
                    CAST(1. * [ios].[tree_page_latch_wait_in_ms] / NULLIF([ios].[tree_page_io_latch_wait_count], 0) AS DECIMAL(12, 2)) AS [avg_tree_page_latch_wait_ms] ,
                    [ios].[tree_page_io_latch_wait_count] ,
                    CAST(1. * [ios].[tree_page_io_latch_wait_in_ms] / NULLIF([ios].[tree_page_io_latch_wait_count], 0) AS DECIMAL(12, 2)) AS [avg_tree_page_io_latch_wait_ms] ,
                    [ios].[range_scan_count] + [ios].[singleton_lookup_count] AS [read_operations] ,
                    [ios].[leaf_insert_count] + [ios].[leaf_update_count] + [ios].[leaf_delete_count] + [ios].[leaf_ghost_count] AS [leaf_writes] ,
                    [ios].[leaf_allocation_count] AS [leaf_page_allocations] ,
                    [ios].[leaf_page_merge_count] AS [leaf_page_merges] ,
                    [ios].[nonleaf_insert_count] + [ios].[nonleaf_update_count] + [ios].[nonleaf_delete_count] AS [nonleaf_writes] ,
                    [ios].[nonleaf_allocation_count] AS [nonleaf_page_allocations] ,
                    [ios].[nonleaf_page_merge_count] AS [nonleaf_page_merges] ,
                    [ios].[page_compression_attempt_count] ,
                    [ios].[page_compression_success_count] ,
                    CAST(100. * [ios].[page_compression_success_count] / NULLIF([ios].[page_compression_attempt_count], 0) AS DECIMAL(6, 2)) AS [page_compression_success_rate] ,
                    [tm].[table_column_count] ,
                    [im].[indexed_columns] ,
                    [im].[indexed_column_count] ,
                    [im].[included_columns] ,
                    [im].[included_column_count] ,
                    [im].[filter_definition] ,
                    [im].[key_columns] ,
                    [im].[data_columns] ,
                    [im].[indexed_columns_ids] ,
                    [im].[included_column_ids] ,
                    [im].[distinct_indexed_columns_ids] ,
                    CAST('' AS VARCHAR(MAX)) AS [duplicate_indexes] ,
                    CAST('' AS SMALLINT) AS [first_dup_index_id] ,
                    CAST('' AS VARCHAR(MAX)) AS [overlapping_indexes] ,
                    CAST('' AS VARCHAR(MAX)) AS [sibling_indexes] ,
                    CAST('' AS VARCHAR(MAX)) AS [related_foreign_keys] ,
                    CAST('' AS XML) AS [related_foreign_keys_xml]
            INTO    [#IndexStatistics]
            FROM    [#TableMeta] [tm]
            INNER JOIN [#IndexMeta] [im] ON [im].[database_id] = [tm].[database_id]
                                            AND [im].[object_id] = [tm].[object_id]
            LEFT OUTER JOIN [sys].[dm_db_index_usage_stats] [ius] ON [im].[object_id] = [ius].[object_id]
                                                                     AND [im].[index_id] = [ius].[index_id]
                                                                     AND [im].[database_id] = [ius].[database_id]
            LEFT OUTER JOIN [#MemoryBuffer] [mb] ON [im].[object_id] = [mb].[object_id]
                                                    AND [im].[index_id] = [mb].[index_id]
                                                    AND [im].[partition_number] = [mb].[partition_number]
            LEFT OUTER JOIN (SELECT [database_id] ,
                                    [object_id] ,
                                    [index_id] ,
                                    CASE WHEN @ConsolidatePartitionStats = 0 THEN [partition_number]
                                         ELSE -1
                                    END AS [partition_number] ,
                                    SUM([leaf_insert_count]) AS [leaf_insert_count] ,
                                    SUM([leaf_delete_count]) AS [leaf_delete_count] ,
                                    SUM([leaf_update_count]) AS [leaf_update_count] ,
                                    SUM([leaf_ghost_count]) AS [leaf_ghost_count] ,
                                    SUM([nonleaf_insert_count]) AS [nonleaf_insert_count] ,
                                    SUM([nonleaf_delete_count]) AS [nonleaf_delete_count] ,
                                    SUM([nonleaf_update_count]) AS [nonleaf_update_count] ,
                                    SUM([leaf_allocation_count]) AS [leaf_allocation_count] ,
                                    SUM([nonleaf_allocation_count]) AS [nonleaf_allocation_count] ,
                                    SUM([leaf_page_merge_count]) AS [leaf_page_merge_count] ,
                                    SUM([nonleaf_page_merge_count]) AS [nonleaf_page_merge_count] ,
                                    SUM([range_scan_count]) AS [range_scan_count] ,
                                    SUM([singleton_lookup_count]) AS [singleton_lookup_count] ,
                                    SUM([forwarded_fetch_count]) AS [forwarded_fetch_count] ,
                                    SUM([lob_fetch_in_pages]) AS [lob_fetch_in_pages] ,
                                    SUM([lob_fetch_in_bytes]) AS [lob_fetch_in_bytes] ,
                                    SUM([lob_orphan_create_count]) AS [lob_orphan_create_count] ,
                                    SUM([lob_orphan_insert_count]) AS [lob_orphan_insert_count] ,
                                    SUM([row_overflow_fetch_in_pages]) AS [row_overflow_fetch_in_pages] ,
                                    SUM([row_overflow_fetch_in_bytes]) AS [row_overflow_fetch_in_bytes] ,
                                    SUM([column_value_push_off_row_count]) AS [column_value_push_off_row_count] ,
                                    SUM([column_value_pull_in_row_count]) AS [column_value_pull_in_row_count] ,
                                    SUM([row_lock_count]) AS [row_lock_count] ,
                                    SUM([row_lock_wait_count]) AS [row_lock_wait_count] ,
                                    SUM([row_lock_wait_in_ms]) AS [row_lock_wait_in_ms] ,
                                    SUM([page_lock_count]) AS [page_lock_count] ,
                                    SUM([page_lock_wait_count]) AS [page_lock_wait_count] ,
                                    SUM([page_lock_wait_in_ms]) AS [page_lock_wait_in_ms] ,
                                    SUM([index_lock_promotion_attempt_count]) AS [index_lock_promotion_attempt_count] ,
                                    SUM([index_lock_promotion_count]) AS [index_lock_promotion_count] ,
                                    SUM([page_latch_wait_count]) AS [page_latch_wait_count] ,
                                    SUM([page_latch_wait_in_ms]) AS [page_latch_wait_in_ms] ,
                                    SUM([page_io_latch_wait_count]) AS [page_io_latch_wait_count] ,
                                    SUM([page_io_latch_wait_in_ms]) AS [page_io_latch_wait_in_ms] ,
                                    SUM([tree_page_latch_wait_count]) AS [tree_page_latch_wait_count] ,
                                    SUM([tree_page_latch_wait_in_ms]) AS [tree_page_latch_wait_in_ms] ,
                                    SUM([tree_page_io_latch_wait_count]) AS [tree_page_io_latch_wait_count] ,
                                    SUM([tree_page_io_latch_wait_in_ms]) AS [tree_page_io_latch_wait_in_ms] ,
                                    SUM([page_compression_attempt_count]) AS [page_compression_attempt_count] ,
                                    SUM([page_compression_success_count]) AS [page_compression_success_count]
                             FROM   [sys].[dm_db_index_operational_stats](DB_ID(), NULL, NULL, NULL)
                             GROUP BY [database_id] ,
                                    [object_id] ,
                                    [index_id] ,
                                    CASE WHEN @ConsolidatePartitionStats = 0 THEN [partition_number]
                                         ELSE -1
                                    END
                            ) [ios] ON [im].[object_id] = [ios].[object_id]
                                       AND [im].[index_id] = [ios].[index_id]
                                       AND [im].[partition_number] = [ios].[partition_number];

            IF @ProcessingMessages = 1
                PRINT 'Processing #IndexStatistics... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
        END;

    -- Collect missing index information.
        IF @IncludeMissingIndexes = 1
        BEGIN
            SET @PROCESSING_START = GETDATE();

            INSERT  INTO [#IndexStatistics]
                    ([index_action] ,
                     [index_pros] ,
                     [index_cons] ,
                     [database_id] ,
                     [has_unique] ,
                     [is_primary_key] ,
                     [is_disabled] ,
                     [user_total_pct] ,
                     [table_column_count] ,
                     [filegroup_name] ,
                     [schema_name] ,
                     [object_id] ,
                     [table_name] ,
                     [object_name] ,
                     [index_name] ,
                     [type_desc] ,
                     [missing_index_impact] ,
                     [index_rank] ,
                     [user_total] ,
                     [user_seeks] ,
                     [user_scans] ,
                     [user_lookups] ,
                     [indexed_columns] ,
                     [included_columns] ,
                     [compression_type] ,
                     [indexed_column_count] ,
                     [included_column_count]
                    )
            SELECT  '' AS [index_action] ,
                    '' AS [index_pros] ,
                    '' AS [index_cons] ,
                    [tm].[database_id] ,
                    [tm].[has_unique] ,
                    0 [is_primary_key] ,
                    0 [is_disabled] ,
                    0 [user_total_pct] ,
                    [tm].[table_column_count] ,
                    '--TBD--' AS [filegroup_name] ,
                    [tm].[schema_name] ,
                    [mid].[object_id] ,
                    [tm].[table_name] ,
                    [tm].[object_name] ,
                    '--MISSING INDEX--' AS [index_name] ,
                    '--NONCLUSTERED--' AS [type_desc] ,
                    ([migs].[user_seeks] + [migs].[user_scans]) * [migs].[avg_user_impact] AS [impact] ,
                    0 AS [index_rank] ,
                    [migs].[user_seeks] + [migs].[user_scans] AS [user_total] ,
                    [migs].[user_seeks] ,
                    [migs].[user_scans] ,
                    0 AS [user_lookups] ,
                    COALESCE([mid].[equality_columns] + CASE WHEN [mid].[inequality_columns] IS NOT NULL THEN ', '
                                                             ELSE SPACE(0)
                                                        END, SPACE(0)) + COALESCE([mid].[inequality_columns], SPACE(0)) AS [indexed_columns] ,
                    [mid].[included_columns] ,
                    '--TBD--' ,
                    [mic].[indexed_column_count] ,
                    [mic].[included_column_count]
            FROM    [#TableMeta] [tm]
            INNER JOIN [sys].[dm_db_missing_index_details] [mid] ON [mid].[database_id] = [tm].[database_id]
                                                                    AND [mid].[object_id] = [tm].[object_id]
            INNER JOIN [sys].[dm_db_missing_index_groups] [mig] ON [mid].[index_handle] = [mig].[index_handle]
            INNER JOIN [sys].[dm_db_missing_index_group_stats] [migs] ON [mig].[index_group_handle] = [migs].[group_handle]
            CROSS APPLY (SELECT SUM(CASE WHEN [column_usage] != 'INCLUDE' THEN 1
                                         ELSE 0
                                    END) AS [indexed_column_count] ,
                                SUM(CASE WHEN [column_usage] = 'INCLUDE' THEN 1
                                         ELSE 0
                                    END) AS [included_column_count]
                         FROM   [sys].[dm_db_missing_index_columns]([mid].[index_handle])
                        ) [mic];
            IF @ProcessingMessages = 1
                PRINT 'Add missing indexes to #IndexStatistics... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
        END;
	
	-- Collect foreign key information.
        BEGIN
            SET @PROCESSING_START = GETDATE();

            CREATE TABLE [#ForeignKeys]
            (
             [foreign_key_name] NVARCHAR(256) ,
             [object_id] INT ,
             [fk_columns] NVARCHAR(MAX) ,
             [fk_columns_ids] NVARCHAR(1024) ,
             [related_object_id] INT ,
             [distinct_column_ids] NVARCHAR(1024) ,
             [indexed_column_count] INT
            );
         
            SET @SQL = N'SELECT fk.name + ''|CHILD'' AS foreign_key_name
			,fkc.parent_object_id AS object_id
			,STUFF((SELECT '', '' + QUOTENAME(c.name)
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
					INNER JOIN ' + @DatabaseName + '.sys.columns c ON ifkc.parent_object_id = c.object_id AND ifkc.parent_column_id = c.column_id
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.constraint_column_id
				FOR XML PATH('''')), 1, 2, '''') AS fk_columns
			,(SELECT QUOTENAME(CAST(ifkc.parent_column_id AS VARCHAR(10))+''+'',''('')
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.constraint_column_id
				FOR XML PATH('''')) AS fk_columns_compare
			,fkc.referenced_object_id AS related_object_id 

			,(SELECT QUOTENAME(CAST(ifkc.parent_column_id AS VARCHAR(10)))
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.parent_column_id
				FOR XML PATH('''')) AS distinct_column_ids
			,(SELECT COUNT(*)
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id) AS indexed_column_count

		FROM #TableMeta tm
			INNER JOIN ' + @DatabaseName + '.sys.foreign_keys fk ON tm.object_id = fk.parent_object_id
			INNER JOIN ' + @DatabaseName + '.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		WHERE fkc.constraint_column_id = 1
		AND (@ObjectID IS NULL OR (fk.parent_object_id = @ObjectID OR fk.referenced_object_id = @ObjectID))
		UNION ALL
		SELECT fk.name + ''|PARENT'' as foreign_key_name
			,fkc.referenced_object_id AS object_id
			,STUFF((SELECT '', '' + QUOTENAME(c.name)
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
					INNER JOIN ' + @DatabaseName + '.sys.columns c ON ifkc.referenced_object_id = c.object_id AND ifkc.referenced_column_id = c.column_id
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.constraint_column_id
				FOR XML PATH('''')), 1, 2, '''') AS fk_columns
			,(SELECT QUOTENAME(CAST(ifkc.referenced_column_id AS VARCHAR(10))+''+'',''('')
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.constraint_column_id
				FOR XML PATH('''')) AS fk_columns_compare
			,fkc.parent_object_id AS related_object_id
			,(SELECT QUOTENAME(CAST(ifkc.referenced_column_id AS VARCHAR(10)))
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id
				ORDER BY ifkc.parent_column_id
				FOR XML PATH('''')) AS fk_columns_compare
			,(SELECT COUNT(*)
				FROM ' + @DatabaseName + '.sys.foreign_key_columns ifkc
				WHERE fk.object_id = ifkc.constraint_object_id) AS indexed_column_count
		FROM #TableMeta tm
			INNER JOIN ' + @DatabaseName + '.sys.foreign_keys fk ON tm.object_id = fk.referenced_object_id
			INNER JOIN ' + @DatabaseName + '.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		WHERE fkc.constraint_column_id = 1
		AND (@ObjectID IS NULL OR (fk.parent_object_id = @ObjectID OR fk.referenced_object_id = @ObjectID))
		';

            BEGIN TRY
                INSERT  INTO [#ForeignKeys]
                        ([foreign_key_name] ,
                         [object_id] ,
                         [fk_columns] ,
                         [fk_columns_ids] ,
                         [related_object_id] ,
                         [distinct_column_ids] ,
                         [indexed_column_count]
                        )
                        EXEC [sys].[sp_executesql] @SQL, N'@DB_ID INT, @ObjectID INT', @DB_ID = @DB_ID, @ObjectID = @ObjectID;
            END TRY
            BEGIN CATCH
                SELECT  @ERROR_MESSAGE = 'Populate #ForeignKeys (Line ' + CAST(ERROR_LINE() AS NVARCHAR(25)) + '): ' + ERROR_MESSAGE() ,
                        @ERROR_SEVERITY = ERROR_SEVERITY() ,
                        @ERROR_STATE = ERROR_STATE();
     
                RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
            END CATCH; 
 		
            IF @ProcessingMessages = 1
                PRINT 'Processing #ForeignKeys... ' + CONVERT(VARCHAR, DATEDIFF(MILLISECOND, @PROCESSING_START, GETDATE())) + ' ms';
        END;

    -- Determine duplicate, overlapping, and foreign key index information
        UPDATE  [i]
        SET     [i].[duplicate_indexes] = STUFF((SELECT ', ' + [iibl].[index_name] AS [data()]
                                                 FROM   [#IndexStatistics] [iibl]
                                                 WHERE  [i].[object_id] = [iibl].[object_id]
                                                        AND [i].[is_primary_key] = [iibl].[is_primary_key]
                                                        AND [i].[is_unique] = [iibl].[is_unique]
                                                        AND ISNULL([i].[filter_definition], '') = ISNULL([iibl].[filter_definition], '')
                                                        AND [i].[index_id] <> [iibl].[index_id]
                                                        AND REPLACE([i].[indexed_columns_ids], '|', '') = REPLACE([iibl].[indexed_columns_ids], '|', '')
                                                        AND [i].[included_column_ids] = [iibl].[included_column_ids]
                                                FOR
                                                 XML PATH('')
                                                ), 1, 2, '') ,
                [i].[first_dup_index_id] = (SELECT  MIN([iibl].[index_id])
                                            FROM    [#IndexStatistics] [iibl]
                                            WHERE   [i].[object_id] = [iibl].[object_id]
                                                    AND [i].[is_primary_key] = [iibl].[is_primary_key]
                                                    AND [i].[is_unique] = [iibl].[is_unique]
                                                    AND ISNULL([i].[filter_definition], '') = ISNULL([iibl].[filter_definition], '')
                                                    AND [i].[index_id] > [iibl].[index_id]
                                                    AND REPLACE([i].[indexed_columns_ids], '|', '') = REPLACE([iibl].[indexed_columns_ids], '|', '')
                                                    AND [i].[included_column_ids] = [iibl].[included_column_ids]
                                           ) ,
                [i].[overlapping_indexes] = STUFF((SELECT   ', ' + [iibl].[index_name] AS [data()]
                                                   FROM     [#IndexStatistics] [iibl]
                                                   WHERE    [i].[object_id] = [iibl].[object_id]
                                                            AND ISNULL([i].[filter_definition], '') = ISNULL([iibl].[filter_definition], '')
                                                            AND [i].[index_id] <> [iibl].[index_id]
                                                            AND LEFT([iibl].[indexed_columns_ids], CHARINDEX('|', [iibl].[indexed_columns_ids], 1) - 1) LIKE LEFT([i].[indexed_columns_ids],
                                                                                                                                                CHARINDEX('|',
                                                                                                                                                [i].[indexed_columns_ids],
                                                                                                                                                1) - 1) + '%'
                                                  FOR
                                                   XML PATH('')
                                                  ), 1, 2, '') ,
                [i].[sibling_indexes] = STUFF((SELECT   ', ' + [iibl].[index_name] AS [data()]
                                               FROM     [#IndexStatistics] [iibl]
                                               WHERE    [i].[object_id] = [iibl].[object_id]
                                                        AND [i].[index_id] <> [iibl].[index_id]
                                                        AND [i].[distinct_indexed_columns_ids] = [iibl].[distinct_indexed_columns_ids]
                                              FOR
                                               XML PATH('')
                                              ), 1, 2, '') ,
                [i].[related_foreign_keys] = STUFF((SELECT  ', ' + [ifk].[foreign_key_name] AS [data()]
                                                    FROM    [#ForeignKeys] [ifk]
                                                    WHERE   [ifk].[object_id] = [i].[object_id]
                                                            AND [i].[indexed_columns_ids] LIKE [ifk].[fk_columns_ids] + '%'
                                                   FOR
                                                    XML PATH('')
                                                   ), 1, 2, '') ,
                [i].[related_foreign_keys_xml] = CAST((SELECT   [fk].[foreign_key_name]
                                                       FROM     [#ForeignKeys] [fk]
                                                       WHERE    [fk].[object_id] = [i].[object_id]
                                                                AND [i].[indexed_columns_ids] LIKE [fk].[fk_columns_ids] + '%'
                                                      FOR
                                                       XML AUTO
                                                      ) AS XML)
        FROM    [#IndexStatistics] [i];

        IF @IncludeMissingFKIndexes = 1
        BEGIN
            INSERT  INTO [#IndexStatistics]
                    ([database_id] ,
                     [filegroup_name] ,
                     [schema_name] ,
                     [object_id] ,
                     [table_name] ,
                     [object_name] ,
                     [index_name] ,
                     [type_desc] ,
                     [has_unique] ,
                     [is_primary_key] ,
                     [is_disabled] ,
                     [user_total_pct] ,
                     [table_column_count] ,
                     [index_rank] ,
                     [indexed_columns] ,
                     [indexed_column_count] ,
                     [indexed_columns_ids] ,
                     [distinct_indexed_columns_ids] ,
                     [included_column_count] ,
                     [key_columns] ,
                     [related_foreign_keys] ,
                     [compression_type]
                    )
            SELECT  [tm].[database_id] ,
                    '--TBD--' AS [filegroup_name] ,
                    OBJECT_SCHEMA_NAME([fk].[object_id]) AS [schema_name] ,
                    [fk].[object_id] ,
                    OBJECT_NAME([fk].[object_id]) AS [table_name] ,
                    [tm].[object_name] ,
                    '--MISSING FOREIGN KEY INDEX--' AS [index_name] ,
                    'NONCLUSTERED' AS [type_desc] ,
                    [tm].[has_unique] ,
                    0 AS [is_primary_key] ,
                    0 AS [is_disabled] ,
                    0 AS [user_total_pct] ,
                    [tm].[table_column_count] ,
                    9999 AS [index_rank] ,
                    [fk].[fk_columns] ,
                    [fk].[indexed_column_count] ,
                    [fk].[fk_columns_ids] ,
                    [fk].[distinct_column_ids] ,
                    0 AS [included_column_count] ,
                    [fk].[fk_columns] ,
                    [fk].[foreign_key_name] ,
                    '--TBD--'
            FROM    [#TableMeta] AS [tm]
            INNER JOIN [#ForeignKeys] [fk] ON [fk].[object_id] = [tm].[object_id]
            LEFT OUTER JOIN [#IndexStatistics] [i] ON [fk].[object_id] = [i].[object_id]
                                                      AND [i].[indexed_columns_ids] LIKE [fk].[fk_columns_ids] + '%'
            WHERE   [i].[index_name] IS NULL;
        END;

--================================================================================================
-- Calculate estimated user total for each index.
--================================================================================================
        WITH    [StatAggregation1]
                  AS (SELECT    [row_id] ,
                                [object_id] ,
                                [user_seeks] ,
                                [user_scans] ,
                                [user_lookups] ,
                                [user_total_pct] ,
                                CONVERT(INT, SUM(CASE WHEN [index_id] IS NULL THEN [user_seeks]
                                                 END) OVER (PARTITION BY [object_id]) * 1. * [user_scans]
                                / NULLIF(SUM(CASE WHEN [index_id] IS NOT NULL THEN [user_scans]
                                             END) OVER (PARTITION BY [object_id]), 0)) AS [weighted_scans] ,
                                SUM([buffered_mb]) OVER (PARTITION BY [schema_name], [table_name]) AS [table_buffered_mb]
                      FROM      [#IndexStatistics]
                     ),
                [StatAggregation2]
                  AS (SELECT    * ,
                                CONVERT(DECIMAL(6, 2), 100. * ([StatAggregation1].[user_seeks] + [StatAggregation1].[user_scans]
                                - [StatAggregation1].[weighted_scans] + [StatAggregation1].[user_lookups])
                                / SUM([StatAggregation1].[user_seeks] + [StatAggregation1].[user_scans] - [StatAggregation1].[weighted_scans]
                                      + [StatAggregation1].[user_lookups]) OVER (PARTITION BY [StatAggregation1].[object_id])) AS [estimated_user_total_pct]
                      FROM      [StatAggregation1]
                     ),
                [StatAggregation3]
                  AS (SELECT    * ,
                                ROW_NUMBER() OVER (PARTITION BY [StatAggregation2].[object_id] ORDER BY [StatAggregation2].[estimated_user_total_pct] DESC, [StatAggregation2].[user_total_pct] DESC) AS [full_index_rank]
                      FROM      [StatAggregation2]
                     )
            UPDATE  [ibl]
            SET     [ibl].[estimated_user_total_pct] = COALESCE([a].[estimated_user_total_pct], 0) ,
                    [ibl].[table_buffered_mb] = [a].[table_buffered_mb] ,
                    [ibl].[full_index_rank] = [a].[full_index_rank]
            FROM    [#IndexStatistics] [ibl]
            INNER JOIN [StatAggregation3] [a] ON [ibl].[row_id] = [a].[row_id];

--================================================================================================
-- Update Pro/Con statuses
--================================================================================================    
        UPDATE  [#IndexStatistics]
        SET     [index_pros] = COALESCE(STUFF(CASE WHEN [index_name] = '--MISSING INDEX--'
                                                        AND [related_foreign_keys] IS NOT NULL THEN ', MIFK'
                                                   WHEN [related_foreign_keys] IS NOT NULL THEN ', FK'
                                                   ELSE ''
                                              END + CASE WHEN [is_unique] = 1 THEN ', UQ'
                                                         ELSE ''
                                                    END + CASE WHEN [full_index_rank] <= 5
                                                                    AND [index_name] = '--MISSING INDEX--' THEN ', TM5'
                                                               ELSE ''
                                                          END + COALESCE(', ' + CASE WHEN [read_to_update] BETWEEN 1 AND 9 THEN '$'
                                                                                     WHEN [read_to_update] BETWEEN 10 AND 99 THEN '$$'
                                                                                     WHEN [read_to_update] BETWEEN 100 AND 999 THEN '$$$'
                                                                                     WHEN [read_to_update] > 999 THEN '$$$+'
                                                                                END, ''), 1, 2, ''), '') ,
                [index_cons] = COALESCE(STUFF(CASE WHEN [index_id] = 0 THEN ', HP'
                                                   ELSE ''
                                              END + CASE WHEN [user_lookups] >= @MinLookupThreshold
                                                              AND [user_lookups] > [user_seeks] + [user_scans] THEN ', LKUP'
                                                         ELSE ''
                                                    END + CASE WHEN NULLIF([user_scans], 0) >= @MinScanThreshold
                                                                    AND [user_seeks] / NULLIF([user_scans], 0) < @Scan2SeekRatio THEN ', SCN'
                                                               ELSE ''
                                                          END + CASE WHEN [duplicate_indexes] IS NOT NULL THEN ', DUP'
                                                                     ELSE ''
                                                                END + CASE WHEN [overlapping_indexes] IS NOT NULL THEN ', OVLP'
                                                                           ELSE ''
                                                                      END + CASE WHEN [sibling_indexes] IS NOT NULL THEN ', SIB'
                                                                                 ELSE ''
                                                                            END + COALESCE(', ' + CASE WHEN [update_to_read] BETWEEN 1 AND 9 THEN '$'
                                                                                                       WHEN [update_to_read] BETWEEN 10 AND 99 THEN '$$'
                                                                                                       WHEN [update_to_read] BETWEEN 100 AND 999 THEN '$$$'
                                                                                                       WHEN [update_to_read] > 999 THEN '$$$+'
                                                                                                  END, '') + COALESCE(', '
                                                                                                                      + CASE WHEN [index_id] <= 1
                                                                                                                                  OR [indexed_column_count]
                                                                                                                                  + [included_column_count] < 4
                                                                                                                             THEN NULL
                                                                                                                             WHEN 100. * ([indexed_column_count]
                                                                                                                                  + [included_column_count])
                                                                                                                                  / [table_column_count] >= 90
                                                                                                                             THEN 'C90%'
                                                                                                                             WHEN 100. * ([indexed_column_count]
                                                                                                                                  + [included_column_count])
                                                                                                                                  / [table_column_count] >= 50
                                                                                                                             THEN 'C50%'
                                                                                                                             WHEN 100. * ([indexed_column_count]
                                                                                                                                  + [included_column_count])
                                                                                                                                  / [table_column_count] >= 25
                                                                                                                             THEN 'C25%'
                                                                                                                        END, '')
                                              + CASE WHEN [index_id] IS NOT NULL
                                                          AND [user_total_pct] < 1 THEN ', U1%'
                                                     ELSE ''
                                                END + CASE WHEN @CheckCompression = 1
                                                                AND [compression_type] = 'NONE' THEN ', NOCMP'
                                                           ELSE ''
                                                      END + CASE WHEN [is_disabled] = 1 THEN ', DSB'
                                                                 ELSE ''
                                                            END, 1, 2, ''), '');

--================================================================================================
-- Update Index Action information
--================================================================================================
        WITH    [IndexAction]
                  AS (SELECT    [row_id] ,
                                CASE WHEN [user_lookups] >= @MinLookupThreshold
                                          AND [user_lookups] > [user_seeks]
                                          AND [type_desc] IN ('CLUSTERED', 'HEAP', 'UNIQUE CLUSTERED') THEN 'REALIGN'
                                     WHEN [user_total_pct] < 5.
                                          AND [type_desc] IN ('CLUSTERED', 'HEAP', 'UNIQUE CLUSTERED')
                                          AND SUM([user_seeks] + [user_scans] + [user_lookups]) OVER (PARTITION BY [object_id]) > 0 THEN 'REALIGN'
                                     WHEN [is_disabled] = 1 THEN 'ENABLE'
                                     WHEN [duplicate_indexes] IS NOT NULL
                                          AND [first_dup_index_id] IS NOT NULL
                                          AND [index_id] IS NOT NULL THEN 'NEG-DUP'
                                     WHEN [type_desc] = '--MISSING FOREIGN KEY--' THEN 'CREATE'
                                     WHEN [type_desc] = 'XML' THEN '---'
                                     WHEN [is_unique] = 1 THEN '---'
                                     WHEN [related_foreign_keys] IS NOT NULL THEN '---'
                                     WHEN [type_desc] = '--NONCLUSTERED--'
                                          AND ROW_NUMBER() OVER (PARTITION BY [table_name] ORDER BY [user_total] DESC) <= @MaxMissingIndexCount
                                          AND [estimated_user_total_pct] > 1 THEN 'CREATE'
                                     WHEN [type_desc] = '--NONCLUSTERED--'
                                          AND [estimated_user_total_pct] > .1 THEN 'BLEND'
                                     WHEN ROW_NUMBER() OVER (PARTITION BY [table_name] ORDER BY [user_total] DESC, [index_rank]) > 10
                                          AND [index_id] IS NOT NULL THEN 'NEG-COUNT'
                                     WHEN [index_id] NOT IN (0, 1)
                                          AND [duplicate_indexes] IS NULL
                                          AND [user_total] = 0
                                          AND [index_id] IS NOT NULL THEN 'NEG-USAGE'
                                     ELSE '---'
                                END AS [index_action]
                      FROM      [#IndexStatistics]
                     )
            UPDATE  [ibl]
            SET     [ibl].[index_action] = [ia].[index_action]
            FROM    [#IndexStatistics] [ibl]
            INNER JOIN [IndexAction] [ia] ON [ibl].[row_id] = [ia].[row_id];
	
--================================================================================================
-- 	Output results from query
--================================================================================================
        IF @Output = 'DUMP'
        BEGIN
            SELECT  *
            FROM    [#IndexStatistics]
            ORDER BY [table_buffered_mb] DESC ,
                    [object_id] ,
                    COALESCE([user_total], -1) DESC ,
                    COALESCE([user_updates], -1) DESC ,
                    COALESCE([index_id], 999);
        END;
        ELSE
            IF @Output = 'DETAILED'
            BEGIN
                SELECT  [index_action] ,
                        [index_pros] ,
                        [index_cons] ,
                        [object_name] ,
                        [index_name] ,
                        [type_desc] ,
                        [indexed_columns] ,
                        [included_columns] ,
                        [filter_definition] ,
                        [is_primary_key] ,
                        [is_unique] ,
                        [is_disabled] ,
                        [has_unique] ,
                        [partition_number] ,
                        [fill_factor] ,
                        [is_padded] ,
                        [size_in_mb] ,
                        [buffered_mb] ,
                        [table_buffered_mb] ,
                        [buffered_percent] ,
                        [index_row_count] ,
                        [user_total_pct] ,
                        [estimated_user_total_pct] ,
                        [missing_index_impact] ,
                        [user_total] ,
                        [user_seeks] ,
                        [user_scans] ,
                        [user_lookups] ,
                        [user_updates] ,
                        [read_to_update_ratio] ,
                        [read_to_update] ,
                        [update_to_read] ,
                        [row_lock_count] ,
                        [row_lock_wait_count] ,
                        [row_lock_wait_in_ms] ,
                        [row_block_pct] ,
                        [avg_row_lock_waits_ms] ,
                        [page_latch_wait_count] ,
                        [avg_page_latch_wait_ms] ,
                        [page_io_latch_wait_count] ,
                        [avg_page_io_latch_wait_ms] ,
                        [tree_page_latch_wait_count] ,
                        [avg_tree_page_latch_wait_ms] ,
                        [tree_page_io_latch_wait_count] ,
                        [avg_tree_page_io_latch_wait_ms] ,
                        [read_operations] ,
                        [leaf_writes] ,
                        [leaf_page_allocations] ,
                        [leaf_page_merges] ,
                        [nonleaf_writes] ,
                        [nonleaf_page_allocations] ,
                        [nonleaf_page_merges] ,
                        [duplicate_indexes] ,
                        [overlapping_indexes] ,
                        [related_foreign_keys] ,
                        [related_foreign_keys_xml] ,
                        [key_columns] ,
                        [data_columns]
                FROM    [#IndexStatistics]
                WHERE   ([estimated_user_total_pct] > 0.01
                        AND [index_id] IS NULL
                        )
                        OR [related_foreign_keys] IS NOT NULL
                        OR [index_id] IS NOT NULL
                ORDER BY [table_buffered_mb] DESC ,
                        [object_id] ,
                        COALESCE([user_total], -1) DESC ,
                        COALESCE([user_updates], -1) DESC ,
                        COALESCE([index_id], 999);
            END;
            ELSE
                IF @Output = 'REALIGN'
                BEGIN
                    SELECT  [index_action] ,
                            [object_name] ,
                            [index_name] ,
                            [partition_number] ,
                            [type_desc] ,
                            [index_pros] ,
                            [index_cons] ,
                            [index_row_count] ,
                            [user_total] ,
                            [user_seeks] ,
                            [user_scans] ,
                            [user_lookups] ,
                            [user_updates] ,
                            [user_total_pct] ,
                            [size_in_mb] ,
                            [buffered_mb] ,
                            [table_buffered_mb] ,
                            [buffered_percent] ,
                            [indexed_columns] ,
                            [included_columns] ,
                            [is_primary_key] ,
                            [is_unique] ,
                            [is_disabled] ,
                            [has_unique] ,
                            [read_to_update_ratio] ,
                            [read_to_update] ,
                            [update_to_read] ,
                            [row_lock_count] ,
                            [row_lock_wait_count] ,
                            [row_lock_wait_in_ms] ,
                            [row_block_pct] ,
                            [avg_row_lock_waits_ms] ,
                            [page_latch_wait_count] ,
                            [avg_page_latch_wait_ms] ,
                            [page_io_latch_wait_count] ,
                            [avg_page_io_latch_wait_ms] ,
                            [read_operations]
                    FROM    [#IndexStatistics]
                    WHERE   [object_id] IN (SELECT  [object_id]
                                            FROM    [#IndexStatistics]
                                            WHERE   [index_action] = 'REALIGN')
                    ORDER BY SUM([user_total]) OVER (PARTITION BY [object_id]) DESC ,
                            [object_id] ,
                            COALESCE([user_total], -1) DESC ,
                            COALESCE([user_updates], -1) DESC ,
                            COALESCE([index_id], 999);
                END;
                ELSE
                    IF @Output = 'DUPLICATE'
                    BEGIN
                        SELECT  DENSE_RANK() OVER (ORDER BY [key_columns], [data_columns]) AS [duplicate_group] ,
                                [index_action] ,
                                [index_pros] ,
                                [index_cons] ,
                                [object_name] ,
                                [index_name] ,
                                [type_desc] ,
                                [indexed_columns] ,
                                [included_columns] ,
                                [is_primary_key] ,
                                [is_unique] ,
                                [duplicate_indexes] ,
                                [size_in_mb] ,
                                [index_row_count] ,
                                [user_total_pct] ,
                                [user_total] ,
                                [user_seeks] ,
                                [user_scans] ,
                                [user_lookups] ,
                                [user_updates] ,
                                [read_operations]
                        FROM    [#IndexStatistics]
                        WHERE   [duplicate_indexes] IS NOT NULL
                        ORDER BY [table_buffered_mb] DESC ,
                                [object_id] ,
                                RANK() OVER (ORDER BY [key_columns], [data_columns]);
                    END;
                    ELSE
                        IF @Output = 'OVERLAPPING'
                        BEGIN
                            SELECT  [index_action] ,
                                    [index_pros] ,
                                    [index_cons] ,
                                    [object_name] ,
                                    [overlapping_indexes] ,
                                    [index_name] ,
                                    [type_desc] ,
                                    [indexed_columns] ,
                                    [included_columns] ,
                                    [is_primary_key] ,
                                    [is_unique] ,
                                    [size_in_mb] ,
                                    [index_row_count] ,
                                    [user_total_pct] ,
                                    [user_total] ,
                                    [user_seeks] ,
                                    [user_scans] ,
                                    [user_lookups] ,
                                    [user_updates] ,
                                    [read_operations]
                            FROM    [#IndexStatistics]
                            WHERE   [overlapping_indexes] IS NOT NULL
                            ORDER BY [table_buffered_mb] DESC ,
                                    [object_id] ,
                                    [user_total] DESC;
                        END;
    END TRY 
    BEGIN CATCH
        SELECT  @ERROR_MESSAGE = 'Procedure Error (Line ' + CAST(ERROR_LINE() AS NVARCHAR(25)) + '): ' + ERROR_MESSAGE() ,
                @ERROR_SEVERITY = ERROR_SEVERITY() ,
                @ERROR_STATE = ERROR_STATE();
     
        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
    END CATCH;
END;


