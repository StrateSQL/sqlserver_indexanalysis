# SQL Server Index Analysis

This repository provides a stored procedure for analyzing SQL Server indexes, offering insights into potential optimizations, such as creating, modifying, or dropping indexes. The recommendations are intended as guidelines and should be adapted based on your knowledge and experience with the database.

---

## ⚠️ Important Notes
- Recommendations in the **Index Action** column are not definitive but serve as starting points for further analysis. Always apply your expertise and knowledge of the database before making changes.
- DMV (Dynamic Management Views) data is reset:
  - When the SQL Server service restarts.
  - When the database is brought online.
  - When index metadata is updated, such as during reindex operations.
- Index statistics in `sys.dm_db_index_operational_stats` and `sys.dm_db_index_usage_stats` are cleared when an index is rebuilt.
- Missing index statistics in the `sys.dm_db_missing_index_*` DMVs are reset whenever a new index is created on a table.
- Names in the **Index Name** column for non-existent indexes are placeholders, not recommended names.

---

## 📊 Parameters
| Parameter                     | Type              | Description                                                                 |
|-------------------------------|-------------------|-----------------------------------------------------------------------------|
| `@TableName`                  | `NVARCHAR(256)`   | (Optional) Specifies the table to analyze.                                  |
| `@IncludeMissingIndexes`      | `BIT`             | Include missing indexes in the output.                                      |
| `@IncludeMissingFKIndexes`    | `BIT`             | Include missing foreign key indexes in the output.                          |
| `@Output`                     | `VARCHAR(20)`     | Determines the output format: `DETAILED`, `DUPLICATE`, or `OVERLAPPING`.    |

---

## 📖 Output Columns
| Column Name                  | Description                                                                                                    |
|------------------------------|----------------------------------------------------------------------------------------------------------------|
| **index_action**             | Recommended action for the index (e.g., `CREATE`, `DROP-DUP`, `DROP-USAGE`, `REALIGN`, `BLEND`).               |
| **index_pros**               | Benefits of the index (e.g., foreign key (`FK`), unique constraint (`UQ`), read-to-write ratio).               |
| **index_cons**               | Drawbacks of the index (e.g., low seeks-to-scans ratio, duplicate, overlapping, or high write costs).          |
| **filegroup**                | Filegroup where the index resides.                                                                            |
| **table_name**               | Name of the table.                                                                                            |
| **index_name**               | Name of the index.                                                                                            |
| **is_unique**                | Indicates if the index is unique.                                                                             |
| **size_in_mb**               | Disk space (in MB) used by the index.                                                                         |
| **pct_in_buffer**            | Percentage of the index currently in the SQL Server buffer.                                                   |
| **row_count**                | Number of rows in the index.                                                                                  |
| **missing_index_impact**     | Calculated impact of a potential index (based on seeks, scans, and average improvement).                      |
| **user_total**               | Total seek, scan, and lookup operations for the index.                                                        |
| **read_to_update_ratio**     | Ratio of read operations to update operations.                                                                |
| **row_lock_count**           | Cumulative number of row locks requested.                                                                     |
| **leaf_page_allocations**    | Number of page splits at the leaf level.                                                                       |
| **indexed_columns**          | List of columns in the index, missing index, or foreign key.                                                  |
| **duplicate_indexes**        | List of duplicate indexes on the table.                                                                       |
| **related_foreign_keys**     | Related foreign keys associated with the index.                                                               |

For a detailed description of all columns, see the source code or documentation.

---

## 💡 Index Action Recommendations
- **CREATE**: Add a new index to improve performance.
- **DROP-DUP**: Remove duplicate indexes.
- **DROP-USAGE**: Remove indexes with low usage or high cost.
- **BLEND**: Merge missing index details into an existing index.
- **REALIGN**: Adjust clustered indexes or include additional columns.

---

## 📜 Example Usage
```sql
EXEC dbo.IndexAnalysis 
    @TableName = 'MyTable',
    @IncludeMissingIndexes = 1,
    @Output = 'DETAILED';
