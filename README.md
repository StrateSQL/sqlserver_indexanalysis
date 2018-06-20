# sqlserver_indexanalysis

Note: 
    Recommendations in the Index Action column are not black and white recommendations.  
    They are more light grey ideas of what may be appropriate.  Always use your experience 
    with the database in place of a blanket recommendation. 

    The information in the DMVs is gathered from when the SQL Server service last started, the
    database was brought onling, or when the index metadata was updated (such as during reindex 
    operations, which ever event is more recent. 

    The index statistics accumulated in sys.dm_db_index_operational_stats and 
    sys.dm_db_index_usage_stats are reset when the index is rebuilt. 

    The index statistics for a table that are accumulated in the DMVs 
    sys.dm_db_missing_index_* are reset whenever an index is created on the table. 

    The index name provided in the name column for indexes that do not exist is not a 
    recommended name for the index.  It’s just an informative placeholder. 
   
Parameters

    @TableName(NVARCHAR(256)): Optional parameter to add
    @IncludeMissingIndexes(BIT): Identifies whether to include missing indexes in the output
    @IncludeMissingFKIndexes(BIT): Identifies whether to include missing foreign key indexes 
        in the output
    @Output(VARCHAR(20)): Determines the output results from the stored procedure.  The
        available values are
           * DETAILED: All results from the index analysis
           * DUPLICATE: Index results for deplicate indexes
           * OVERLAPPING: Index results for overlapping indexes

Columns:

    index_action: Analysis recommendation on action to take on the index 
        CREATE: Recommend adding the index to the table. 
        DROP-DUP: Recommend dropping the index since it is a duplicate 
		DROP-COUNT: Count of indexes on tables may indicate that the index may be a candidate
			for removal.
		DROP-USAGE: Usage of the index suggests it may be a candidate for removal 
        BLEND: Review the missing index details to see if the missing index details can be 
            added to an existing index. 
        REALIGN: Bookmark lookups on the index exceed the number of seeks on the table.  
            Recommend investigating whether to move the clustered index to another index or 
            add included columns to the indexes that are part of the bookmark lookups. 
    index_pros: list of reasons that indicate the benefits provided by the index 
        FK: The index schema maps to a foreign key 
        UQ: Index is a unique constraint 
        $, $$, $$$, $$$+: Indicates the ratio of read to write uses in execution plans.  The 
            higher the ratio the more dollar signs; this should correlate to greater benefit 
            provided by the index. 
    index_cons: list of reasons that indicate some negative aspects associate with the index 
        SCN: Flag indicating that the ratio of seeks to scans on the index less than 1,000. 
        DP: Index schema is a duplicate of another index. 
        OV: Index schema overlaps another index. 
        $, $$, $$$, $$$+: Indicates the ratio of write to read uses in execution plans.  The 
            higher the ratio the more dollar signs; this should correlate to more cost  
            incurred by the index.
		DSB: Index is disabled.  These should be enabled or removed.
    filegroup: file group that the index is located.
    schema_id: Schema ID 
    schema_name: Name of the schema. 
    object_id: Object ID 
    table_name: Name of the table name 
    index_id: Index ID 
    index_name: Name of the index. 
    is_unique: Flag indicating whether an index has a unique index. 
    has_unique: Flag indicating whether the table has a unique index. 
    type_desc: Type of index; either clustered or non-clustered. 
    partition_number: Partition number. 
    fill_factor: Percentage of free space left on pages the index was created or rebuilt. 
    is_padded: Boolean value indicating whether fill factor is applied to nonleaf levels 
    reserved_page_count: Total number of pages reserved for the index. 
    size_in_mb: The amount of space in MB the index utilizes on disk. 
    buffered_page_count: Total number of pages in the buffer for the index. 
    buffer_mb: The amount of space in MB in the buffer for the index. 
    pct_in_buffer: The percentage of an index that is current in the SQL Server buffer. 
    table_buffer_mb: The amount of space in MB in the SQL Server buffer that is being 
        utilized by the table. 
    row_count: Number of rows in the index. 
    missing_index_impact: Calculation of impact of a potential index.  This is based on the seeks and 
        scans that the index could have utilized multiplied by average improvement the index 
        would have provided.  This is included only for missing indexes. 
    existing_ranking: Ranking of the existing indexes ordered by user_total descending across
        the indexes for the table. 
    user_total: Total number of seek, scan, and lookup operations for the index. 
    user_total_pct: Percentage of total number of seek, scan, and lookup operations for this 
        index compared to all seek, scan, and lookup operations for existing indexes for the 
        table. 
    estimated_user_total_pct: Percentage of total number of seek, scan, and lookup operations 
        for this index compared to all seek, scan, and lookup operations for existing and 
        potential indexes for the table.  This number is naturally skewed because a seek for 
        potential Index A resulted in another operation on an existing index and both of 
        these operations would be counted. 
    user_seeks: Number of seek operations on the index. 
    user_scans: Number of scan operations on the index. 
    user_lookups: Number of lookup operations on the index. 
    user_updates: Number of update operations on the index. 
    read_to_update_ratio: Ratio of user_seeks, user_scans, and user_lookups to user_updates. 
    read_to_update: Division of user_seeks, user_scans, and user_lookups by user_updates. 
    update_to_read: Division of user_updates to user_seeks, user_scans by user_lookups. 
    row_lock_count: Cumulative number of row locks requested. 
    row_lock_wait_count: Cumulative number of times the Database Engine waited on a row lock. 
    row_lock_wait_in_ms: Total number of milliseconds the Database Engine waited on a row 
        lock. 
    row_block_pct: Percentage of row locks that encounter waits on a row lock. 
    avg_row_lock_waits_ms: Average number of milliseconds the Database Engine waited on a row 
        lock. 
    page_latch_wait_count: Cumulative number of times the page latch waits occurred 
    avg_page_latch_wait_ms: Average number of milliseconds the Database Engine waited on a 
        page latch wait. 
    page_io_latch_wait_count: Cumulative number of times the page IO latch waits occurred 
    avg_page_io_latch_wait_ms: Average number of milliseconds the Database Engine waited on a 
        page IO latch wait. 
    tree_page_latch_wait_count: Cumulative number of times the tree page latch waits occurred 
    avg_tree_page_latch_wait_ms: Average number of milliseconds the Database Engine waited on 
        a tree page latch wait. 
    tree_page_io_latch_wait_count: Cumulative number of times the tree page IO latch waits 
        occurred 
    avg_tree_page_io_latch_wait_ms: Average number of milliseconds the Database Engine waited 
        on a tree page IO latch wait. 
    read_operations: Cumulative count of range_scan_count and singleton_lookup_count 
        operations 
    leaf_writes: Cumulative count of leaf_insert_count, leaf_update_count, leaf_delete_count 
        and leaf_ghost_count operations 
    leaf_page_allocations: Cumulative count of leaf-level page allocations in the index or 
        heap.  For an index, a page allocation corresponds to a page split. 
    leaf_page_merges: Cumulative count of page merges at the leaf level. 
    nonleaf_writes: Cumulative count of leaf_insert_count, leaf_update_count and 
        leaf_delete_count operations 
    nonleaf_page_allocations: Cumulative count of page allocations caused by page splits 
        above the leaf level. 
    nonleaf_page_merges: Cumulative count of page merges above the leaf level. 
    indexed_columns: Columns that are part of the index, missing index or foreign key. 
    included_columns: Columns that are included in the index or missing index. 
    indexed_columns_ids: Column IDs that are part of the index, missing index or foreign 
        key 
    included_column_ids: Column IDs that are included in the index or missing index. 
    duplicate_indexes: List of Indexes that exist on the table that are identical to the 
        index on this row. 
    overlapping_indexes: List of Indexes that exist on the table that overlap the index on 
        this row. 
    related_foreign_keys: List of foreign keys that are related to the index either as an 
        exact match or covering index. 
    related_foreign_keys_xml: XML document listing foreign keys that are related to the index 
        either as an exact match or covering index. 
