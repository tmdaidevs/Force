# =============================================================================
# Force Remediation Executor - Fabric Notebook
# =============================================================================
#
# This notebook is triggered by the Force Remediation User Data Function
# to execute Spark SQL remediation commands (VACUUM, OPTIMIZE, ALTER TABLE).
#
# SETUP:
#   1. Import this notebook into your Fabric workspace
#   2. Attach it to a Lakehouse (the one containing the tables to remediate)
#   3. Mark the cell below as a "Parameters" cell
#   4. Note the workspace ID and notebook ID for the User Data Function config
#
# SECURITY:
#   The script is validated against an allowlist before execution.
#   Only VACUUM, OPTIMIZE, ALTER TABLE, and ANALYZE TABLE are permitted.
# =============================================================================

# --- Parameters cell (mark this cell as Parameters in Fabric) ---
remediation_script = ""

# --- Validation and execution ---
ALLOWED_PREFIXES = [
    "VACUUM",
    "OPTIMIZE",
    "ALTER TABLE",
    "ANALYZE TABLE",
]

if not remediation_script or not remediation_script.strip():
    print("ERROR: No remediation script provided.")
    raise ValueError("No remediation script provided. Pass 'remediation_script' as a notebook parameter.")

normalized = remediation_script.strip().upper()
is_allowed = any(normalized.startswith(prefix) for prefix in ALLOWED_PREFIXES)

if not is_allowed:
    print(f"ERROR: Script rejected by allowlist: {remediation_script[:100]}")
    raise ValueError(
        f"Script rejected: only {', '.join(ALLOWED_PREFIXES)} commands are allowed. "
        f"Received: {remediation_script[:100]}"
    )

print(f"Executing remediation script: {remediation_script}")
print("-" * 60)

# Execute the Spark SQL command
try:
    result = spark.sql(remediation_script)
    result.show()
    print("-" * 60)
    print("Remediation executed successfully.")
except Exception as e:
    print(f"ERROR: Remediation failed: {str(e)}")
    raise
