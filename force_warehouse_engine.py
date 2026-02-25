# =============================================================================
# Force Warehouse Scanner
# This script must be executed inside a Microsoft Fabric Notebook.
# It depends on Fabric-only APIs (notebookutils, sempy_labs) and OneLake
# connectivity that are not available outside the Fabric runtime.
# =============================================================================

import pandas as pd
import json
import notebookutils
import os
import re
from datetime import datetime, timezone
import sempy_labs as labs
import polars as pl

# Use the correct path to the existing rules file
lakehouse_force = "abfss://Fabric_Optimization_FEATURE@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Tables/"
delta_path = "abfss://Fabric_Optimization_FEATURE@onelake.dfs.fabric.microsoft.com/Main.warehouse/Tables/dbo/Date"
rules_file_path = "./builtin/Force/standardized_fabric_warehouse_rules Copy CopyNEWWW.json"



def get_connection(warehouse_name, workspace_id):
    """Open a connection to a specific Fabric Warehouse artifact.
    
    Args:
        warehouse_name: Name of the warehouse
        workspace_id: ID of the workspace containing the warehouse
    
    Returns:
        Connection object to the specified warehouse
    """
    return notebookutils.data.connect_to_artifact(warehouse_name, workspace_id, "Warehouse")

def load_json_with_comments(file_path):
    """Load a JSON file that may contain comments."""
    try:
        # First, check if file exists and has content
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            raise ValueError(f"File is empty: {file_path}")
            
        # If we get here, file exists and has content
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
            # Remove single-line comments
            content = re.sub(r'//.*', '', content)
            
            # Parse JSON
            return json.loads(content)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}. Current directory: {os.getcwd()}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error parsing JSON: {e}", e.doc, e.pos)

def create_finding_data(rule, workspace_id, workspace_name, warehouse_name, table_name=None, 
                       column_name=None, result="", scan_timestamp=None, extra_data=None):
    """Helper function to create a standardized finding data dictionary."""
    finding_data = {
        "rule_id": rule.get("id"),
        "category": rule.get("category"),
        "description": rule.get("description"),
        "recommendation": rule.get("recommendation"),
        "severity": rule.get("severity"),
        "content": rule.get("content", ""),
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "warehouse_name": warehouse_name,
        "table_name": table_name,
        "column_name": column_name,
        "result": result,
        "scan_date": scan_timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "level": rule.get("level", "database")
    }
    
    # Add any extra data if provided
    if extra_data:
        finding_data.update(extra_data)
        
    return finding_data

def analyze_warehouse(rules_file_path, warehouse_name, workspace_id, workspace_name):
    """Analyze a specific warehouse using the rules defined in the JSON file."""
    # Get current timestamp in ISO 8601 format with UTC timezone (Z)
    scan_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    
    # Read the JSON file with comment handling
    data = load_json_with_comments(rules_file_path)
    rules = data.get("rules", [])
    all_findings = []
    
    try:
        # Get connection to the specific Fabric Warehouse
        print(f"Connecting to warehouse '{warehouse_name}' in workspace '{workspace_name}' ({workspace_id})...")
        connection = get_connection(warehouse_name, workspace_id)
        
        # Process each active rule
        for rule in rules:
            # Skip inactive rules quickly
            rule_status = rule.get("status")
            if rule_status not in [True, "true", 1]:
                continue
                
            rule_id = rule.get("id")
            content = rule.get("content", "")
            sql_query = rule.get("sql_query", "")
            
            # Process SQL query rules
            if content == "query" and sql_query:
                try:
                    # Execute the SQL query
                    query_results = connection.query(sql_query)
                    
                    # If DataFrame is returned and has results
                    if isinstance(query_results, pd.DataFrame):
                        # Standardize column names if needed
                        if not query_results.empty:
                            # Standardize column names efficiently
                            column_renames = {}
                            if "table_name" not in query_results.columns:
                                if "tablename" in query_results.columns:
                                    column_renames["tablename"] = "table_name"
                                elif "name" in query_results.columns:
                                    column_renames["name"] = "table_name"
                                    
                            if "column_name" not in query_results.columns and "columnname" in query_results.columns:
                                column_renames["columnname"] = "column_name"
                                
                            # Apply renames in one operation if needed
                            if column_renames:
                                query_results = query_results.rename(columns=column_renames)
                            
                            # Add result column if doesn't exist
                            if "result" not in query_results.columns:
                                query_results["result"] = f"Found issue with {rule_id}"
                            
                            # Process each row efficiently
                            for _, row in query_results.iterrows():
                                # Get basic finding data
                                extra_data = {col: row[col] for col in query_results.columns 
                                             if col not in ["table_name", "column_name", "result"]}
                                
                                # Add finding with all data
                                all_findings.append(create_finding_data(
                                    rule, 
                                    workspace_id, 
                                    workspace_name, 
                                    warehouse_name,
                                    row.get("table_name") if "table_name" in query_results.columns else None,
                                    row.get("column_name") if "column_name" in query_results.columns else None,
                                    row.get("result") if "result" in query_results.columns else "Finding detected",
                                    scan_timestamp,
                                    extra_data
                                ))
                        else:
                            # No issues found, add a "clean" entry
                            all_findings.append(create_finding_data(
                                rule, workspace_id, workspace_name, warehouse_name, 
                                result="No issues found", scan_timestamp=scan_timestamp
                            ))
                except Exception as e:
                    # Record error as finding
                    all_findings.append(create_finding_data(
                        rule, workspace_id, workspace_name, warehouse_name,
                        result=f"Error executing query: {str(e)}", scan_timestamp=scan_timestamp
                    ))
            else:
                # For general rules (non-queries), add a placeholder entry
                all_findings.append(create_finding_data(
                    rule, workspace_id, workspace_name, warehouse_name,
                    result="General guidance - no query executed", scan_timestamp=scan_timestamp
                ))
                
    except Exception as e:
        # Process connection error
        error_message = str(e)
        print(f"Error connecting to warehouse '{warehouse_name}' in workspace '{workspace_name}': {error_message}")
        
        # Create connection error finding with a custom rule
        connection_error_rule = {
            "id": "CONNECTION_ERROR",
            "category": "System",
            "description": "Failed to connect to warehouse",
            "recommendation": "Check warehouse accessibility and credentials",
            "severity": 1,
            "content": "system",
            "level": "database"
        }
        
        all_findings.append(create_finding_data(
            connection_error_rule, workspace_id, workspace_name, warehouse_name,
            result=f"Connection error: {error_message}", scan_timestamp=scan_timestamp
        ))
    
    # Create DataFrame from findings more efficiently
    columns = ["rule_id", "category", "description", "recommendation", "severity", "content", 
               "workspace_id", "workspace_name", "warehouse_name", "table_name", "column_name", 
               "result", "scan_date", "level"]
    
    findings_df = pd.DataFrame(all_findings) if all_findings else pd.DataFrame(columns=columns)
    
    return findings_df

def process_all_workspaces(labs):
    """Get all workspaces and their items."""
    # Get list of all workspaces
    workspaces_df = labs.admin.list_workspaces()
    
    # List to store all items from all workspaces
    all_workspace_items = []
    
    # Get all workspace IDs
    workspace_ids = workspaces_df['Id'].tolist()
    
    # Process workspaces in chunks of 100
    for i in range(0, len(workspace_ids), 100):
        # Get next chunk of 100 IDs
        chunk_ids = workspace_ids[i:i+100]
        
        # Scan workspaces for this chunk
        workspace_data = labs.admin.scan_workspaces(workspace=chunk_ids)
        
        if workspace_data and 'workspaces' in workspace_data:
            # Process each workspace in the chunk
            for workspace in workspace_data['workspaces']:
                workspace_id = workspace['id']
                workspace_name = workspace['name']
                
                for item_type, items in workspace.items():
                    if not isinstance(items, list):
                        continue
                        
                    for item in items:
                        item_data = {'workspace_id': workspace_id,
                                    'workspace_name': workspace_name,
                                    'item_type': item_type}
                        
                        # Add all item properties
                        for key, value in item.items():
                            if key == 'extendedProperties' and isinstance(value, dict):
                                for ext_key, ext_value in value.items():
                                    item_data[f'ext_{ext_key}'] = ext_value
                            else:
                                item_data[key] = value
                        
                        all_workspace_items.append(item_data)
    
    # Create final DataFrame
    df = pd.DataFrame(all_workspace_items)
    
    if not df.empty:
        # Add scan_date column with current date and time in ISO 8601 format with UTC timezone (Z)
        df['scan_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        # Convert all columns to string to avoid type conflicts
        for col in df.columns:
            df[col] = df[col].astype(str)
    
    return df

def analyze_all_warehouses(rules_file_path, labs):
    """Analyze all warehouses across all workspaces."""
    # Get all workspaces and their items
    print("Getting all workspaces and their items...")
    all_items_df = process_all_workspaces(labs)
    
    # Filter for warehouse items only - do this in one operation for efficiency
    warehouses_df = all_items_df[
        (all_items_df['item_type'] == 'warehouses') & 
        (all_items_df['name'] != 'DataflowsStagingWarehouse')
    ].copy()
    
    print(f"Found {len(warehouses_df)} warehouses across {warehouses_df['workspace_id'].nunique()} workspaces")
    
    # Pre-allocate list with estimated capacity
    all_findings = []
    
    # Process each warehouse
    for index, row in warehouses_df.iterrows():
        workspace_id = row['workspace_id']
        workspace_name = row['workspace_name']
        warehouse_name = row['name'] if 'name' in row else row.get('displayName', f"Warehouse_{index}")
        
        print(f"\nProcessing warehouse {warehouse_name} in workspace {workspace_name} ({workspace_id})...")
        
        try:
            # Analyze this warehouse
            findings_df = analyze_warehouse(rules_file_path, warehouse_name, workspace_id, workspace_name)
            
            # Add to findings if not empty (avoid unnecessary operations)
            if not findings_df.empty:
                all_findings.append(findings_df)
                print(f"Completed analysis for {warehouse_name}. Found: {len(findings_df)} findings.")
            else:
                print(f"Completed analysis for {warehouse_name}. No findings.")
            
        except Exception as e:
            print(f"Error analyzing warehouse {warehouse_name}: {str(e)}")
    
    # Concatenate all findings efficiently
    all_findings_df = pd.concat(all_findings, ignore_index=True) if all_findings else pd.DataFrame()
    
    # Optimize column order if data exists
    if not all_findings_df.empty:
        # Get existing columns and compute priority columns in one pass
        cols = set(all_findings_df.columns)
        priority_cols = ['rule_id', 'level', 'workspace_name', 'workspace_id', 
                         'warehouse_name', 'table_name', 'column_name']
        
        # Use list comprehension for efficiency
        existing_priority_cols = [col for col in priority_cols if col in cols]
        remaining_cols = [col for col in all_findings_df.columns if col not in priority_cols]
        
        # Reorder columns
        all_findings_df = all_findings_df[existing_priority_cols + remaining_cols]
    
    print("\nAnalysis complete for all warehouses")
    return all_findings_df

# Main execution block 
if __name__ == "__main__":
    try:
        print("Using sempy_labs to analyze all warehouses...")
        
        # Analyze all warehouses - now returns a single DataFrame
        all_findings_df = analyze_all_warehouses(rules_file_path, labs)
            
        # Save results using Polars
        try:
            print("\nWriting findings to Delta table...")
            
            # Only convert to Polars if we have data (avoids empty DataFrame conversion)
            if not all_findings_df.empty:
                # Convert pandas DataFrame to Polars DataFrame
                findings_polars = pl.from_pandas(all_findings_df)
            
                # Convert scan_date from string to timestamp in one operation
                if 'scan_date' in findings_polars.columns:
                    findings_polars = findings_polars.with_columns(
                        pl.col('scan_date').str.to_datetime()
                    )
                
                # Write all findings to a single Delta table
                print("Writing all findings to Delta table...")
                findings_polars.write_delta(
                    f"{lakehouse_force}force_all_findings", 
                    mode="overwrite"
                )
                
                print(f"Successfully wrote {findings_polars.height} findings to Delta table")
            else:
                print("No findings to write to Delta table")
                
        except Exception as e:
            print(f"Error writing to Delta table: {str(e)}")
            print(f"Error details: {type(e).__name__}, {str(e)}")
            
    except Exception as e:
        print(f"Error analyzing warehouses: {str(e)}")
        print("Make sure sempy_labs is properly installed and configured.")
        print("For installation: pip install sempy-labs")
        
display(all_findings_df)