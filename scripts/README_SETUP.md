# Databricks Job Setup Guide

This guide explains how to set up the Databricks job with proper SQL queries and permissions.

## Problem

The error "The SQL entity could not be found" occurs because:
1. The workflow references queries that don't exist in the workspace
2. The run-as user doesn't have permissions on the warehouse or queries
3. Inline queries in the workflow JSON aren't supported - you need to create Query objects first

## Solution

We need to:
1. Create Query objects in Databricks from the SQL files
2. Grant permissions on the warehouse and queries
3. Update the workflow JSON to reference the query IDs

## Quick Start

### 1. Verify Warehouse ID

First, get your warehouse ID:

```bash
databricks --profile lapd-crime warehouses list
```

Update the warehouse ID in the scripts if needed (default: `ba90f266beeaacb2`)

### 2. Run Setup Script

```bash
./scripts/setup_databricks_job.sh [profile] [warehouse_id]
```

This will:
- Grant CAN_USE permission on the warehouse
- Create all SQL queries from the SQL files
- Grant CAN_RUN permission on each query
- Update the workflow JSON with the query IDs

### 3. Create/Update the Job

```bash
# Create new job
databricks --profile lapd-crime jobs create --json @databricks/workflows/lapd_crime_pipeline.json

# Or update existing job
databricks --profile lapd-crime jobs update <job_id> --json @databricks/workflows/lapd_crime_pipeline.json
```

### 4. Run the Job

```bash
databricks --profile lapd-crime jobs run-now <job_id>
```

## Manual Steps (if scripts fail)

### Step 1: Grant Warehouse Permissions

```bash
WAREHOUSE_ID="ba90f266beeaacb2"
databricks --profile lapd-crime warehouses update-permissions "$WAREHOUSE_ID" \
  --json @permissions/warehouse_can_use.json
```

### Step 2: Create Queries

```bash
./scripts/create_queries.sh lapd-crime ba90f266beeaacb2
```

This creates queries from:
- `databricks/notebooks/00_uc_bootstrap_with_grants.sql`
- `databricks/notebooks/01_external_location_with_grants.sql`
- `databricks/notebooks/02_bronze.sql`
- `databricks/notebooks/03_silver_fact.sql`
- `databricks/notebooks/04_silver_dim.sql`
- `databricks/notebooks/05_gold_agg_1.sql`
- `databricks/notebooks/06_gold_agg_2.sql`

### Step 3: Update Workflow JSON

```bash
./scripts/update_workflow_with_query_ids.sh
```

This updates `databricks/workflows/lapd_crime_pipeline.json` to use `query_id` instead of inline queries.

## File Structure

```
.
├── permissions/
│   ├── warehouse_can_use.json      # Warehouse permissions
│   └── query_can_run.json           # Query permissions
├── build/
│   ├── query_payloads/              # Query creation payloads
│   └── query_ids/                   # Created query IDs
├── databricks/
│   ├── notebooks/
│   │   ├── 00_uc_bootstrap_with_grants.sql
│   │   ├── 01_external_location_with_grants.sql
│   │   └── ... (other SQL files)
│   └── workflows/
│       └── lapd_crime_pipeline.json  # Updated with query_ids
└── scripts/
    ├── create_queries.sh
    ├── update_workflow_with_query_ids.sh
    └── setup_databricks_job.sh
```

## Troubleshooting

### "SQL entity could not be found"
- Verify warehouse ID is correct: `databricks --profile lapd-crime warehouses list`
- Check that queries were created: Query IDs should be in `build/query_ids/`
- Verify permissions were granted

### "Permission denied"
- Ensure warehouse has CAN_USE permission
- Ensure queries have CAN_RUN permission
- Check that `run_as.user_name` in workflow matches the user in permission files

### "Query ID not found"
- Re-run `create_queries.sh` to recreate queries
- Check that `update_workflow_with_query_ids.sh` ran successfully
- Verify query IDs in `build/query_ids/` match the workflow JSON

## Notes

- The SQL files with `_with_grants` suffix include all necessary GRANT statements
- Query IDs are saved in `build/query_ids/` for reference
- The workflow JSON is automatically updated to use `query_id` instead of inline `query.query`

