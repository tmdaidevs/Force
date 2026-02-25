# =============================================================================
# Force Lakehouse Scanner
# This script must be executed inside a Microsoft Fabric Notebook.
# It depends on Fabric-only APIs (notebookutils, sempy_labs) and OneLake
# connectivity that are not available outside the Fabric runtime.
# =============================================================================

import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "semantic-link-labs", "polars", "deltalake", "duckdb", "-q"])

import sempy_labs as labs
import pandas as pd
import notebookutils
import json
import fsspec
import re
import io
from contextlib import redirect_stdout
from datetime import datetime, timezone
import duckdb
import polars as pl

# ============================================================
# CONFIGURATION - Edit this section before running
# ============================================================

# Target Lakehouse for writing analysis results.
# Replace with the workspace name where results should be stored.
TARGET_WORKSPACE_NAME = "YourWorkspaceName"

# Replace with the Lakehouse name for storing results.
TARGET_LAKEHOUSE_NAME = "YourLakehouseName"

# Workspace scope: which workspaces to scan.
# Set to [] to scan ALL workspaces (requires admin API permissions).
# Or provide a list of workspace IDs or names, e.g.:
#   WORKSPACE_FILTER = ["workspace-id-1", "workspace-id-2"]
#   WORKSPACE_FILTER = ["Sales Analytics", "Finance Reporting"]
WORKSPACE_FILTER = []

# Rules file path (relative to notebook location).
# Default expects the JSON file alongside this notebook.
RULES_FILE_PATH = "force_lakehouse_rules.json"

# Output table names in the target Lakehouse.
OUTPUT_TABLE_NAME = "force_lakehouse_analysis"
OUTPUT_TABLE_HISTORY_NAME = "force_lakehouse_analysis_history"

# ============================================================
# END CONFIGURATION
# ============================================================


def get_onelake_filesystem():
    """Create an fsspec filesystem for OneLake access."""
    return fsspec.filesystem(
        "abfss",
        account_name="onelake",
        account_host="onelake.dfs.fabric.microsoft.com",
        token=notebookutils.credentials.getToken('storage'),
        use_fabric_endpoint="true"
    )


def process_all_workspaces(labs, workspace_filter=None):
    """Get all Lakehouse items from all workspaces."""
    # Get list of all workspaces
    workspaces_df = labs.admin.list_workspaces()

    if workspace_filter:
        # Support both IDs and names in the filter list
        mask = (
            workspaces_df['Id'].isin(workspace_filter) |
            workspaces_df['Name'].isin(workspace_filter)
        )
        workspaces_df = workspaces_df[mask]
        if workspaces_df.empty:
            print(f"Warning: No workspaces matched the filter: {workspace_filter}")
            return pd.DataFrame()

    workspace_ids = workspaces_df['Id'].tolist()
    print(f"Scanning {len(workspace_ids)} workspace(s)...")
    
    all_workspace_items = []
    
    # Process workspaces in chunks of 100
    for i in range(0, len(workspace_ids), 100):
        chunk_ids = workspace_ids[i:i+100]
        workspace_data = labs.admin.scan_workspaces(workspace=chunk_ids)
        
        if not workspace_data or 'workspaces' not in workspace_data:
            continue
            
        for workspace in workspace_data['workspaces']:
            workspace_id = workspace['id']
            workspace_name = workspace['name']
            
            # Process only Lakehouse items
            if 'Lakehouse' not in workspace:
                continue
                
            for item in workspace['Lakehouse']:
                # Skip DataflowsStagingLakehouse and System workspaces
                if (item['name'] == 'DataflowsStagingLakehouse' or 
                     workspace_name == 'PersonalWorkspace System'):
                    continue
                
                connection_type = 'Lakehouse'
                    
                item_data = {
                    'workspace_id': workspace_id,
                    'workspace_name': workspace_name,
                    'item_type': 'Lakehouse',
                    'connection_type': connection_type,
                    'id': item['id'],
                    'name': item['name']
                }
                
                # Add extended properties if available
                if 'extendedProperties' in item and isinstance(item['extendedProperties'], dict):
                    for ext_key, ext_value in item['extendedProperties'].items():
                        item_data[f'ext_{ext_key}'] = ext_value
                
                all_workspace_items.append(item_data)
    
    # Create DataFrame
    return pd.DataFrame(all_workspace_items)

def get_lakehouse_schemas(lakehouse_df):
    """Get schemas from each Lakehouse."""
    all_schemas = []
    
    for _, lakehouse in lakehouse_df.iterrows():
        lakehouse_id = lakehouse['id']
        workspace_id = lakehouse['workspace_id']
        lakehouse_name = lakehouse['name']
        workspace_name = lakehouse['workspace_name']
        connection_type = 'Lakehouse'
        
        try:
            # Connect to Lakehouse
            conn = notebookutils.data.connect_to_artifact(lakehouse_id, workspace_id, connection_type)
            schemas_df = conn.query("SELECT * FROM sys.schemas;")
            
            if schemas_df is not None and len(schemas_df) > 0:
                # Add information
                schemas_df['lakehouse_id'] = lakehouse_id
                schemas_df['lakehouse_name'] = lakehouse_name
                schemas_df['workspace_id'] = workspace_id
                schemas_df['workspace_name'] = workspace_name
                schemas_df['item_type'] = 'Lakehouse'
                schemas_df['connection_type'] = connection_type
                
                all_schemas.append(schemas_df)
                
        except Exception as e:
            print(f"Warning: Could not get schemas for lakehouse '{lakehouse_name}' in '{workspace_name}': {e}")

    return pd.concat(all_schemas, ignore_index=True) if all_schemas else pd.DataFrame()

def get_tables_by_schema(lakehouse_df, schemas_df):
    """Get tables for each schema in each Lakehouse."""
    all_tables = []
    
    # Group schemas by lakehouse
    schema_groups = schemas_df.groupby(['lakehouse_id', 'workspace_id'])
    
    for (lakehouse_id, workspace_id), group in schema_groups:
        try:
            # Get metadata including extended properties
            lakehouse_info = lakehouse_df[lakehouse_df['id'] == lakehouse_id].iloc[0]
            lakehouse_name = lakehouse_info['name']
            workspace_name = lakehouse_info['workspace_name']
            connection_type = 'Lakehouse'
            
            # Connect to Lakehouse
            conn = notebookutils.data.connect_to_artifact(lakehouse_id, workspace_id, connection_type)
            
            for _, schema_row in group.iterrows():
                schema_name = schema_row['name']
                schema_id = schema_row['schema_id']
                
                try:
                    query = f"""
                    SELECT 
                        t.*, 
                        '{schema_name}' as schema_name,
                        '{lakehouse_id}' as lakehouse_id,
                        '{lakehouse_name}' as lakehouse_name,
                        '{workspace_id}' as workspace_id,
                        '{workspace_name}' as workspace_name,
                        'Lakehouse' as item_type,
                        'Lakehouse' as connection_type
                    FROM 
                        sys.tables t
                    WHERE 
                        schema_id = {schema_id}
                    """
                    
                    tables_df = conn.query(query)
                    
                    if tables_df is not None and len(tables_df) > 0:
                        # Add extended properties from lakehouse to each table
                        for column in lakehouse_info.index:
                            # Only add extended properties columns
                            if column.startswith('ext_'):
                                tables_df[column] = lakehouse_info[column]
                        
                        all_tables.append(tables_df)
                        
                except Exception as e:
                    print(f"Warning: Could not get tables for schema '{schema_name}' in '{lakehouse_name}': {e}")

        except Exception as e:
            print(f"Warning: Could not connect to lakehouse '{lakehouse_id}': {e}")
    
    if all_tables:
        return pd.concat(all_tables, ignore_index=True)
    else:
        return pd.DataFrame()

def flatten_json_columns(df, json_columns=None):
    """
    Flatten JSON-formatted columns into separate columns in the DataFrame.
    
    Parameters:
    -----------
    df: pandas DataFrame
        DataFrame containing JSON-formatted columns
    json_columns: list
        List of column names containing JSON data to flatten. If None, detects columns with 'ext_' prefix that contain JSON.
        
    Returns:
    --------
    pandas DataFrame
        DataFrame with flattened JSON columns
    """
    if df.empty:
        return df
    
    result_df = df.copy()
    
    # If no specific columns provided, try to detect JSON columns with 'ext_' prefix
    if json_columns is None:
        json_columns = []
        for col in result_df.columns:
            if col.startswith('ext_') and isinstance(result_df[col].iloc[0], str):
                # Try parsing the first non-null value as JSON
                try:
                    sample_val = result_df[col].dropna().iloc[0]
                    if isinstance(sample_val, str) and sample_val.startswith('{') and sample_val.endswith('}'):
                        json.loads(sample_val)  # Test if it's valid JSON
                        json_columns.append(col)
                except (ValueError, IndexError, json.JSONDecodeError):
                    continue
    
    # Process each JSON column
    for col in json_columns:
        try:
            # Convert JSON strings to dictionaries where possible
            parsed_series = result_df[col].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.startswith('{') and x.endswith('}') else None
            )
            
            # Only process if we have successfully parsed values
            if not parsed_series.dropna().empty:
                # Get all unique keys across all dictionaries
                all_keys = set()
                for item in parsed_series.dropna():
                    if isinstance(item, dict):
                        all_keys.update(item.keys())
                
                # Create a new column for each key
                for key in all_keys:
                    new_col_name = f"{col}_{key}"
                    
                    # Extract values for the key
                    result_df[new_col_name] = parsed_series.apply(
                        lambda x: x.get(key) if isinstance(x, dict) else None
                    )
        
        except Exception as e:
            # Skip on error and keep original column
            print(f"Warning: Could not flatten column '{col}': {e}")
            continue
    
    return result_df

def generate_table_paths(flattened_tables):
    """
    Generate the OneLake path for each table in the flattened_tables DataFrame.
    
    Parameters:
    -----------
    flattened_tables: pandas DataFrame
        DataFrame containing lakehouse and table information
        
    Returns:
    --------
    pandas DataFrame
        Original DataFrame with an additional 'table_path' column
    """
    if flattened_tables.empty:
        return flattened_tables
    
    result_df = flattened_tables.copy()
    
    # Define a function to generate the path for Lakehouse tables
    def create_path(row):
        try:
            workspace_name = row['workspace_name']
            lakehouse_name = row['lakehouse_name']
            table_name = row['name']
            
            # Generate path for Lakehouse
            return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{table_name}"
        except Exception:
            return None
    
    # Apply the function to each row
    result_df['table_path'] = result_df.apply(create_path, axis=1)
    
    return result_df

# Function to load lakehouse rules from the JSON file
def load_lakehouse_rules():
    rules_path = RULES_FILE_PATH
    try:
        with open(rules_path, "r") as f:
            rules_data = json.load(f)
            return rules_data.get("rules", [])
    except Exception as e:
        print(f"Warning: Could not load rules from '{rules_path}': {e}")
        return []

# Extract lakehouse and table name from path
def extract_lakehouse_table_info(path):
    pattern = r'abfss://([^@]+)@[^/]+/([^/]+)\.([^/]+)/Tables/([^/]+)/'
    match = re.search(pattern, path)
    if (match):
        lakehouse_container = match.group(1)
        lakehouse_name = match.group(2)
        lakehouse_type = match.group(3)
        table_name = match.group(4)
        return {
            'lakehouse_container': lakehouse_container,
            'lakehouse_name': lakehouse_name,
            'lakehouse_type': lakehouse_type,
            'table_name': table_name
        }
    return {
        'lakehouse_container': None,
        'lakehouse_name': None,
        'lakehouse_type': None,
        'table_name': None
    }

# Function to process delta log actions
def process_delta_log(actions, version, filename):
    data = {
        'log_version': version,
        'filename': filename
    }
    
    for i, action in enumerate(actions):
        action_type = list(action.keys())[0]
        action_data = action[action_type]
        
        prefix = f"{action_type}_{i}_"
        for key, value in action_data.items():
            data[f"{prefix}{key}"] = value
            
        # Extract summary information
        if action_type == 'commitInfo':
            data['summary_timestamp'] = action_data.get('timestamp')
            data['summary_operation'] = action_data.get('operation')
            
        # Extract metadata
        if action_type == 'metaData':
            for key, value in action_data.items():
                data[f"metadata_{key}"] = value
                
        # Extract format information
        if action_type == 'protocol':
            for key, value in action_data.items():
                data[f"format_{key}"] = value
    
    return data

# Function to get parquet metadata using DuckDB
def get_parquet_metadata(table_path):
    """
    Get parquet metadata for a table using DuckDB
    
    Args:
        table_path: Path to the parquet files
        
    Returns:
        DataFrame with parquet metadata
    """
    try:
        # Create a parquet_metadata query
        query = f"SELECT * FROM parquet_metadata('{table_path}/*.parquet')"
        
        # Execute query using duckdb
        metadata_df = duckdb.query(query).df()
        return metadata_df
    except Exception as e:
        print(f"Warning: Could not read parquet metadata from '{table_path}': {e}")
        return pd.DataFrame()

# Function to analyze a rule based on the pyspark_query or duckdb_query content
def analyze_rule(rule, df_all, parquet_metadata_df=None):
    """
    Executes the PySpark or DuckDB query from the rule and returns the output
    and an optional dynamic remediation script.
    
    Args:
        rule: The rule to analyze from the JSON file
        df_all: DataFrame with Delta log data
        parquet_metadata_df: DataFrame with parquet metadata (for DuckDB queries)
        
    Returns:
        Tuple of (output_text, dynamic_remediation_script_or_None)
    """
    rule_id = rule.get("id", "")
    pyspark_query = rule.get("pyspark_query", "")
    duckdb_query = rule.get("duckdb_query", "")
    
    # Check if it's a DuckDB query
    if duckdb_query and parquet_metadata_df is not None and not parquet_metadata_df.empty:
        try:
            output_buffer = io.StringIO()
            with redirect_stdout(output_buffer):
                try:
                    # Create a DuckDB connection
                    con = duckdb.connect(database=':memory:')
                    
                    # Register the DataFrame as a table
                    con.register('df', parquet_metadata_df)
                    
                    # Execute the query - get a direct string output rather than DataFrame for cleaner display
                    result = con.execute(duckdb_query).fetchone()[0]
                    
                    # Print the result directly (redirected to buffer)
                    print(result)
                        
                except Exception:
                    pass
            
            output = output_buffer.getvalue().strip()

            return output, None
            
        except Exception:
            return f"Error analyzing rule {rule_id} with DuckDB", None
    
    # If not a DuckDB query, use PySpark query
    elif pyspark_query:
        try:
            # Import required modules for the queries
            import json
            from datetime import datetime
            
            # Get table and lakehouse info
            table_name = df_all['table_name'].iloc[0] if not df_all.empty and 'table_name' in df_all.columns else "unknown"
            lakehouse_name = df_all['lakehouse_name'].iloc[0] if not df_all.empty and 'lakehouse_name' in df_all.columns else "unknown"
            full_table_name = f"{lakehouse_name}.{table_name}"
            
            # Capture the output of the query execution using StringIO
            output_buffer = io.StringIO()
            with redirect_stdout(output_buffer):
                try:
                    # Define globals and locals for execution
                    exec_globals = {
                        'json': json,
                        'datetime': datetime,
                        'pd': pd,
                        're': re,
                        'table_name': table_name,
                        'lakehouse_name': lakehouse_name,
                        'full_table_name': full_table_name
                    }
                    
                    # Local variables available to the query
                    exec_locals = {
                        "df_all": df_all,
                        "table_name": table_name,
                        "lakehouse_name": lakehouse_name,
                        "full_table_name": full_table_name,
                        "pd": pd
                    }
                    
                    # Modify the query to use the DataFrame directly
                    modified_query = pyspark_query
                    
                    # Execute the query directly
                    exec(modified_query, exec_globals, exec_locals)
                    
                    # Check if rule set a remediation_script variable
                    if 'remediation_script' in exec_locals:
                        exec_globals['_remediation_script'] = exec_locals['remediation_script']
                except Exception:
                    pass
            
            output = output_buffer.getvalue().strip()
            
            if not output:
                return "", None
            
            # Check if the rule dynamically set a remediation_script
            dynamic_remediation = exec_globals.get('_remediation_script', None)
                
            return output, dynamic_remediation
        
        except Exception:
            return f"Error analyzing rule {rule_id}", None
    else:
        return "", None

# Function to analyze rules for a specific table
def analyze_table_rules(table_path, workspace_id=None, workspace_name=None, lakehouse_id=None):
    """
    Analyzes all rules for a specific table path
    
    Args:
        table_path: The path to the table
        workspace_id: The ID of the workspace containing the table
        workspace_name: The name of the workspace containing the table
        lakehouse_id: The ID of the lakehouse containing the table
        
    Returns:
        DataFrame with rule analysis results
    """
    # Set up filesystem for OneLake
    fs = get_onelake_filesystem()
    
    delta_log_dir = f"{table_path}/_delta_log/"
    lh_info = extract_lakehouse_table_info(delta_log_dir)
    
    # Process JSON log files
    json_logs = []
    max_attempts = 10
    for i in range(max_attempts):
        file_num = str(i).zfill(20)
        log_path = f"{delta_log_dir}{file_num}.json"
        try:
            with fs.open(log_path, "r") as f:
                actions = [json.loads(line) for line in f]
                data = process_delta_log(actions, i, f"{file_num}.json")
                data['source'] = 'json'
                data.update(lh_info)
                json_logs.append(data)
        except Exception:
            pass

    df_json = pd.DataFrame(json_logs)
    df_all = df_json

    # Get parquet metadata using DuckDB
    parquet_metadata_df = get_parquet_metadata(table_path)

    # Load rules from JSON file
    rules = load_lakehouse_rules()
    
    rules_results = []
    
    # Analyze all rules
    for rule in rules:
        rule_id = rule["id"]
        category = rule["category"]
        description = rule["description"]
        recommendation = rule["recommendation"]
        status_enabled = rule.get("status", "true") == "true"
        severity = rule.get("severity", 3)
        level = rule.get("level", "table")
        remediation_template = rule.get("remediation_template", "")
        
        # Only check enabled rules
        if not status_enabled:
            continue
            
        # Execute the query and get the output - check if it's a DuckDB query
        if "duckdb_query" in rule and rule["duckdb_query"]:
            if parquet_metadata_df.empty:
                rule_result = "Parquet metadata not available for this table"
                dynamic_remediation = None
            else:
                rule_result, dynamic_remediation = analyze_rule(rule, df_all, parquet_metadata_df)
        else:
            rule_result, dynamic_remediation = analyze_rule(rule, df_all)
        
        # Build remediation script: prefer dynamic (from rule code), fallback to template
        remediation_script = None
        is_anomaly = isinstance(rule_result, str) and "Anomaly - ERR_1001" in rule_result
        
        if is_anomaly:
            if dynamic_remediation:
                remediation_script = dynamic_remediation
            elif remediation_template:
                # Replace placeholders in template
                remediation_script = remediation_template.replace(
                    "{table_name}", lh_info.get('table_name', '')
                ).replace(
                    "{lakehouse_name}", lh_info.get('lakehouse_name', '')
                ).replace(
                    "{full_table_name}", f"{lh_info.get('lakehouse_name', '')}.{lh_info.get('table_name', '')}"
                )
        
        # Add result with single result column
        result = {
            'rule_id': rule_id,
            'category': category,
            'description': description,
            'recommendation': recommendation,
            'severity': severity,
            'level': level,
            'table_name': lh_info['table_name'],
            'lakehouse_name': lh_info['lakehouse_name'],
            'lakehouse_id': lakehouse_id,
            'workspace_id': workspace_id,
            'workspace_name': workspace_name,
            'rule_result': rule_result.strip() if isinstance(rule_result, str) else rule_result,
            'remediation_script': remediation_script
        }
        rules_results.append(result)
    
    return pd.DataFrame(rules_results)

# Main execution flow
def main():
    """Main execution: scan workspaces, analyze tables, write results."""
    print("Force Lakehouse Scanner starting...")
    print(f"Target: {TARGET_WORKSPACE_NAME}/{TARGET_LAKEHOUSE_NAME}")
    if WORKSPACE_FILTER:
        print(f"Workspace filter: {WORKSPACE_FILTER}")
    else:
        print("Scope: All workspaces")

    lakehouses = process_all_workspaces(labs, workspace_filter=WORKSPACE_FILTER if WORKSPACE_FILTER else None)

    if lakehouses.empty:
        print("No lakehouses found. Check permissions and workspace filter.")
        return

    print(f"Found {len(lakehouses)} lakehouse(s). Fetching schemas...")
    lakehouse_schemas = get_lakehouse_schemas(lakehouses)

    if lakehouse_schemas.empty:
        print("No schemas found in any lakehouse.")
        return

    tables_by_schema = get_tables_by_schema(lakehouses, lakehouse_schemas)

    # Flatten any JSON-formatted columns only if we have tables
    flattened_tables = flatten_json_columns(tables_by_schema) if not tables_by_schema.empty else pd.DataFrame()

    # Generate table paths for all tables
    tables_with_paths = generate_table_paths(flattened_tables)

    # Execute rule analysis if we have tables with paths
    if tables_with_paths.empty or 'table_path' not in tables_with_paths.columns:
        print("No tables with paths found to analyze.")
        return

    all_rule_results = []
    total_tables = len(tables_with_paths)
    print(f"Analyzing {total_tables} table(s) against rules...")

    # Iterate through each table path and analyze rules
    for idx, (i, row) in enumerate(tables_with_paths.iterrows(), start=1):
        table_path = row['table_path']
        if table_path is None:
            continue

        # Get workspace and lakehouse information
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        lakehouse_id = row['lakehouse_id']
        table_name = row.get('name', 'unknown')

        print(f"  [{idx}/{total_tables}] Analyzing {workspace_name}/{table_name}...")

        try:
            # Run rule analysis for this table with workspace and lakehouse info
            rule_results = analyze_table_rules(
                table_path,
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                lakehouse_id=lakehouse_id
            )

            if not rule_results.empty:
                # Add the table path to the results
                rule_results['table_path'] = table_path
                all_rule_results.append(rule_results)
        except Exception as e:
            print(f"Warning: Could not analyze table at '{table_path}': {e}")

    # Combine all rule results into a single DataFrame if any were found
    if all_rule_results:
        combined_results = pd.concat(all_rule_results, ignore_index=True)

        # Add timestamp for when the analysis was performed - use ISO 8601 format with UTC timezone
        now_utc = datetime.now(timezone.utc)
        combined_results['analysis_timestamp'] = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # Display results for notebook view
        display(combined_results)

        # Build target Lakehouse path from configuration
        target_lakehouse = f"abfss://{TARGET_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{TARGET_LAKEHOUSE_NAME}.Lakehouse"
        target_table = OUTPUT_TABLE_NAME
        target_table_history = OUTPUT_TABLE_HISTORY_NAME

        # Convert to polars DataFrame
        findings_polars = pl.from_pandas(combined_results)

        # Convert analysis_timestamp from string to datetime in polars DataFrame
        if 'analysis_timestamp' in findings_polars.columns:
            findings_polars = findings_polars.with_columns(
                pl.col('analysis_timestamp').str.to_datetime()
            )

        try:
            # Always use overwrite mode for the main table
            findings_polars.write_delta(
                f"{target_lakehouse}/Tables/{target_table}",
                mode="overwrite"
            )
            print(f"Analysis results successfully written to {target_lakehouse}/Tables/{target_table} using overwrite mode")

            # Always use append mode for the history table
            findings_polars.write_delta(
                f"{target_lakehouse}/Tables/{target_table_history}",
                mode="append"
            )
            print(f"Analysis results successfully written to {target_lakehouse}/Tables/{target_table_history} using append mode")

        except Exception as e:
            print(f"Error writing to tables: {str(e)}")
    else:
        print("No rule results were generated.")

# Run
main()