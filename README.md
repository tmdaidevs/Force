# Force — Microsoft Fabric Scanner

> **Important:** This project is designed to run exclusively inside a **Microsoft Fabric Notebook**. It relies on Fabric-provided APIs (`notebookutils`, `sempy_labs`) and OneLake connectivity that are only available in the Fabric runtime. It cannot be run locally or in standard Jupyter environments.

A collection of Microsoft Fabric Notebooks that scan all **Lakehouses** and **Warehouses** across your tenant and evaluate them against best-practice rules covering structure, schema, performance, compression, maintenance, Delta Lake, data quality, security, and more.

## What It Does

### Lakehouse Scanner (`force_lakehouse_engine.py`)

1. Discovers all Lakehouses across your Fabric workspaces
2. Reads Delta log metadata and Parquet file statistics for each table
3. Runs **40 built-in rules** (defined in `force_lakehouse_rules.json`)
4. Writes findings to a target Lakehouse as two Delta tables:
   - `force_lakehouse_analysis` — latest results, overwritten each run
   - `force_lakehouse_analysis_history` — appended each run for trending

### Warehouse Scanner (`force_warehouse_engine.py`)

1. Discovers all Warehouses across your Fabric workspaces
2. Connects to each Warehouse and executes T-SQL analysis queries
3. Runs **51 active rules** (defined in `force_warehouse_rules.json`)
4. Writes findings to a target Lakehouse as a Delta table:
   - `force_warehouse_analysis` — latest results, overwritten each run

## Prerequisites

- Microsoft Fabric workspace with a Lakehouse for storing results
- Fabric capacity (F64 or higher recommended for large tenants)
- Workspace Admin or Fabric Admin permissions (required for `admin.list_workspaces` and `admin.scan_workspaces` APIs)
- A Fabric Notebook environment

## Setup

### Lakehouse Scanner

1. Import `force_lakehouse_engine.py` into a Fabric Notebook
2. Upload `force_lakehouse_rules.json` to the same notebook resource folder (or the Lakehouse Files section — adjust `RULES_FILE_PATH` accordingly)
3. Edit the **CONFIGURATION** section at the top of the notebook

### Warehouse Scanner

1. Import `force_warehouse_engine.py` into a Fabric Notebook
2. Upload `force_warehouse_rules.json` to the same notebook resource folder
3. Edit the **CONFIGURATION** section at the top of the notebook

## Configuration

### Lakehouse Scanner

Edit the configuration variables at the top of `force_lakehouse_engine.py` before running the notebook:

```python
# Target Lakehouse for writing analysis results.
TARGET_WORKSPACE_NAME = "YourWorkspaceName"
TARGET_LAKEHOUSE_NAME = "YourLakehouseName"

# Workspace scope: [] = scan all, or list specific IDs/names.
WORKSPACE_FILTER = []

# Rules file path (relative to notebook location).
RULES_FILE_PATH = "force_lakehouse_rules.json"

# Output table names in the target Lakehouse.
OUTPUT_TABLE_NAME = "force_lakehouse_analysis"
OUTPUT_TABLE_HISTORY_NAME = "force_lakehouse_analysis_history"
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `TARGET_WORKSPACE_NAME` | Yes | — | Workspace name where results are written |
| `TARGET_LAKEHOUSE_NAME` | Yes | — | Lakehouse name for storing results |
| `WORKSPACE_FILTER` | No | `[]` (all) | List of workspace IDs or names to scan |
| `RULES_FILE_PATH` | No | `force_lakehouse_rules.json` | Path to the rules JSON file |
| `OUTPUT_TABLE_NAME` | No | `force_lakehouse_analysis` | Delta table name for latest results |
| `OUTPUT_TABLE_HISTORY_NAME` | No | `force_lakehouse_analysis_history` | Delta table name for historical results |

### Warehouse Scanner

Edit the configuration variables at the top of `force_warehouse_engine.py` before running the notebook:

```python
# Target Lakehouse for writing analysis results.
TARGET_WORKSPACE_NAME = "YourWorkspaceName"
TARGET_LAKEHOUSE_NAME = "YourLakehouseName"

# Workspace scope: [] = scan all, or list specific IDs/names.
WORKSPACE_FILTER = []

# Rules file path (relative to notebook location).
RULES_FILE_PATH = "force_warehouse_rules.json"

# Output table name in the target Lakehouse.
OUTPUT_TABLE_NAME = "force_warehouse_analysis"
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `TARGET_WORKSPACE_NAME` | Yes | — | Workspace name where results are written |
| `TARGET_LAKEHOUSE_NAME` | Yes | — | Lakehouse name for storing results |
| `WORKSPACE_FILTER` | No | `[]` (all) | List of workspace IDs or names to scan |
| `RULES_FILE_PATH` | No | `force_warehouse_rules.json` | Path to the rules JSON file |
| `OUTPUT_TABLE_NAME` | No | `force_warehouse_analysis` | Delta table name for results |

### Examples

Scan all workspaces:
```python
WORKSPACE_FILTER = []
```

Scan specific workspaces by name:
```python
WORKSPACE_FILTER = ["Sales Analytics", "Finance Reporting"]
```

Scan specific workspaces by ID:
```python
WORKSPACE_FILTER = ["a1b2c3d4-e5f6-7890-abcd-ef1234567890", "b2c3d4e5-f6a7-8901-bcde-f12345678901"]
```

## Rules

### Lakehouse Rules (40 rules)

The Lakehouse scanner includes 40 rules across these categories:

| Category | Rules | Examples |
|----------|-------|---------|
| Structure | RL001-RL003 | Layer naming, partitioning, Z-order |
| Schema | RL004-RL005, RL008-RL009, RL013, RL028 | Naming conventions, nullability, data types, audit columns |
| Maintenance | RL006, RL015 | Retention policies, VACUUM |
| Compression | RL007, RL019 | File sizes, compression ratios |
| Performance | RL011-RL012, RL018, RL020-RL022, RL024-RL027 | Skew, encoding, row groups, bloom filters, parallelism |
| Metadata | RL010 | Column documentation |
| Delta Lake | RL014, RL016-RL017, RL030-RL031 | Checkpoints, auto-optimize, data skipping |
| Data Quality | RL025, RL029 | Cardinality, null ratios |
| Parquet File Structure | RL032 | Row-group offset continuity |

Each rule produces either **Optimized** or **Anomaly** as a finding, along with a severity level and recommendation.

### Warehouse Rules (60 rules, 51 active)

The Warehouse scanner includes 60 rules (51 active, 9 disabled for Fabric incompatibility) across these categories:

| Category | Rules | Examples |
|----------|-------|----------|
| Data Quality | RL001, RL003, RL007-RL009, RL013-RL014, RL028, RL032-RL034, RL036-RL037 | NULL values, FK integrity, data types, duplicates, audit columns |
| Performance | RL017-RL020, RL031 | Column sizing, statistics, computed columns |
| Security | RL012, RL021, RL023, RL035 | Data masking, db_owner privileges, RLS, sensitive data |
| Maintainability | RL004-RL005, RL010-RL011, RL015-RL016, RL024-RL027, RL029, RL038-RL040, RL043 | Naming, documentation, deprecated types, wide tables, cross-schema |
| Concurrency | RL115-RL116 | Snapshot isolation, read committed snapshot |
| Reliability | RL114, RL127 | Page verify, clean shutdown |
| Compatibility | RL120 | Compatibility level |
| Standards | RL121 | ANSI SQL settings |
| Availability | RL123-RL124 | Database state, user access |
| Result Caching | RL125 | Result set caching |
| Data Management | RL126, RL128-RL129 | Containment, full-text search, data retention |

Rules disabled for Fabric incompatibility are marked with `"status": "false"` and a `fabric_note` explanation.

Each rule produces a `result` column containing the finding details and an **Indicator** line (`Optimized - OPT_2001` or `Anomaly - ERR_1001`).

### Adding Custom Lakehouse Rules

You can add your own rules to `force_lakehouse_rules.json`. Each rule is a JSON object inside the `"rules"` array. There are two types of rules: **PySpark rules** (analyze Delta log metadata) and **DuckDB rules** (analyze Parquet file metadata).

#### Rule Schema

```json
{
  "id": "RL033",
  "category": "YourCategory",
  "description": "What this rule checks.",
  "pyspark_query": "...or...",
  "duckdb_query": "...pick one...",
  "recommendation": "What the user should do if the rule finds an issue.",
  "status": "true",
  "severity": 2,
  "content": "analysis",
  "level": "table"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique rule ID (e.g., `RL033`). Must not conflict with existing IDs. |
| `category` | Yes | Grouping category (e.g., `Structure`, `Schema`, `Performance`, `Maintenance`, `Compression`, `Delta Lake`, `Data Quality`, or your own). |
| `description` | Yes | Short description of what the rule checks. |
| `pyspark_query` | One of these | Python code executed against Delta log data. Use this **or** `duckdb_query`, not both. |
| `duckdb_query` | One of these | SQL query executed against Parquet metadata. Use this **or** `pyspark_query`, not both. |
| `recommendation` | Yes | Actionable guidance shown when the rule detects an issue. |
| `status` | Yes | `"true"` to enable, `"false"` to disable. |
| `severity` | Yes | `1` (critical), `2` (warning), or `3` (informational). |
| `content` | Yes | Set to `"analysis"`. |
| `level` | Yes | Scope of the rule: `"lakehouse"`, `"table"`, or `"column"`. |

#### Output Format (Required)

Every rule **must** print an indicator line as the last output. This is how the engine determines pass/fail:

```
Indicator: Optimized - OPT_2001
```
or
```
Indicator: Anomaly - ERR_1001
```

Print findings/details before the indicator, with an empty line separating them:

```python
print("Some finding or detail here")
print("")
print("Indicator: " + ("Anomaly - ERR_1001" if issue_found else "Optimized - OPT_2001"))
```

#### PySpark Rules (Delta Log Analysis)

PySpark rules are Python code executed via `exec()`. The following variables are available:

| Variable | Type | Description |
|----------|------|-------------|
| `df_all` | pandas DataFrame | Flattened Delta log data (all JSON log entries). Columns include `log_version`, `metadata_*`, `add_*`, `commitInfo_*`, etc. |
| `table_name` | str | Name of the current table. |
| `lakehouse_name` | str | Name of the current Lakehouse. |
| `full_table_name` | str | `"{lakehouse_name}.{table_name}"`. |
| `json` | module | Python `json` module. |
| `datetime` | class | Python `datetime.datetime` class. |
| `pd` | module | pandas module. |
| `re` | module | Python `re` module. |

**Example: Check if table has more than 50 columns**
```json
{
  "id": "RL033",
  "category": "Schema",
  "description": "Check for tables with excessive column count.",
  "pyspark_query": "schema_info = None\nfor _, row in df_all.iterrows():\n    for col in row.index:\n        if 'metadata_schema' in col and isinstance(row[col], str):\n            try:\n                schema_info = json.loads(row[col].replace(\"'\", '\"'))\n                break\n            except:\n                pass\n    if schema_info:\n        break\n\nissue = False\nif schema_info and 'fields' in schema_info:\n    count = len(schema_info['fields'])\n    print(f\"Table has {count} columns\")\n    if count > 50:\n        print(\"WARNING: Wide table detected. Consider splitting into multiple tables.\")\n        issue = True\nelse:\n    print(\"Could not determine column count\")\n\nprint(\"\")\nprint(\"Indicator: \" + (\"Anomaly - ERR_1001\" if issue else \"Optimized - OPT_2001\"))",
  "recommendation": "Tables with many columns can impact scan performance. Consider vertical partitioning for wide tables.",
  "status": "true",
  "severity": 3,
  "content": "analysis",
  "level": "table"
}
```

#### DuckDB Rules (Parquet Metadata Analysis)

DuckDB rules are SQL queries executed against a table called `df`, which contains the output of DuckDB's `parquet_metadata()` function. The query must return a **single string value** in a column called `output`.

**Available columns in `df`:**

| Column | Description |
|--------|-------------|
| `file_name` | Parquet file path |
| `row_group_id` | Row group index |
| `row_group_num_rows` | Number of rows in the row group |
| `row_group_bytes` | Size of the row group in bytes |
| `path_in_schema` | Column name |
| `type` | Parquet physical type (e.g., `INT32`, `BYTE_ARRAY`, `BOOLEAN`) |
| `encodings` | Encoding types used |
| `compression` | Compression codec |
| `total_compressed_size` | Compressed size in bytes |
| `total_uncompressed_size` | Uncompressed size in bytes |
| `stats_min` | Minimum value (if statistics available) |
| `stats_max` | Maximum value (if statistics available) |
| `stats_null_count` | Number of null values |
| `stats_distinct_count` | Number of distinct values (if available) |
| `file_offset` | Byte offset of the column chunk in the file |

**Example: Check if any column uses GZIP compression (slower than SNAPPY)**
```json
{
  "id": "RL034",
  "category": "Compression",
  "description": "Check for columns using GZIP compression instead of SNAPPY.",
  "duckdb_query": "WITH gzip_cols AS (\n  SELECT DISTINCT path_in_schema\n  FROM df\n  WHERE compression = 'GZIP'\n)\nSELECT\n  CONCAT(\n    CASE\n      WHEN (SELECT COUNT(*) FROM gzip_cols) = 0 THEN\n        'No GZIP compression found. All columns use efficient compression.'\n      ELSE\n        'Found ' || (SELECT COUNT(*) FROM gzip_cols) || ' columns using GZIP compression. '\n        || 'SNAPPY is recommended for better read performance.'\n    END,\n    '\n\n',\n    'Indicator: ',\n    CASE\n      WHEN (SELECT COUNT(*) FROM gzip_cols) = 0 THEN 'Optimized - OPT_2001'\n      ELSE 'Anomaly - ERR_1001'\n    END\n  ) AS output;",
  "recommendation": "Switch from GZIP to SNAPPY compression for better read performance, unless storage cost is the primary concern.",
  "status": "true",
  "severity": 2,
  "content": "analysis",
  "level": "column"
}
```

#### Tips

- Set `"status": "false"` to disable a rule without deleting it.
- Use severity `1` for issues that can cause data loss or corruption, `2` for performance concerns, and `3` for best-practice recommendations.
- PySpark rules use `print()` for output — all printed text is captured as the rule result.
- DuckDB queries must return exactly one row with one column named `output`.
- Test your rule by running it against a single table before adding it to the rules file.

### Adding Custom Warehouse Rules

You can add your own rules to `force_warehouse_rules.json`. Each rule is a JSON object inside the `"rules"` array containing a T-SQL query.

#### Warehouse Rule Schema

```json
{
  "id": "RL044",
  "category": "YourCategory",
  "description": "What this rule checks.",
  "sql_query": "SELECT ... AS table_name, ... AS column_name, ... AS result",
  "recommendation": "What the user should do if the rule finds an issue.",
  "status": "true",
  "severity": 2,
  "content": "query",
  "level": "table"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique rule ID (e.g., `RL044`). Must not conflict with existing IDs. |
| `category` | Yes | Grouping category (e.g., `Data Quality`, `Performance`, `Security`, `Maintainability`). |
| `description` | Yes | Short description of what the rule checks. |
| `sql_query` | Yes | T-SQL query returning columns `table_name`, `column_name`, and `result`. |
| `recommendation` | Yes | Actionable guidance shown when the rule detects an issue. |
| `status` | Yes | `"true"` to enable, `"false"` to disable. |
| `severity` | Yes | `1` (critical), `2` (warning), or `3` (informational). |
| `content` | Yes | Set to `"query"`. |
| `level` | Yes | Scope: `"database"`, `"table"`, or `"column"`. |

The `result` column **must** end with an indicator line:

```
Indicator: Optimized - OPT_2001
```
or
```
Indicator: Anomaly - ERR_1001
```

Use `CHAR(10)` for line breaks within the result string.

**Fabric-specific limitations:** Triggers, traditional indexes, computed columns, and many `ALTER DATABASE` settings are not available in Fabric Warehouse. Check the [T-SQL surface area documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area) before writing new rules.

## Dependencies

- `pandas` — DataFrame operations
- `polars` — Delta Lake write support
- `duckdb` — Parquet metadata analysis
- `fsspec` — OneLake file system access
- `sempy-labs` — Fabric Admin APIs
- `notebookutils` — Fabric-provided (available in notebooks automatically, not pip-installable)

## License

MIT License — see [LICENSE](LICENSE)
