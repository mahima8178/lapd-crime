# Databricks Job Setup - Complete Solution

## Problem Summary

The error `"The SQL entity could not be found"` occurs because:
- Databricks Jobs API requires **Query objects** to exist in the workspace
- Inline SQL queries in workflow JSON (`query.query`) are not supported
- The run-as user needs `CAN_USE` on warehouse and `CAN_RUN` on queries

## Solution Overview

We've created a complete setup that:
1. ✅ Creates Query objects from your SQL files
2. ✅ Grants all necessary permissions
3. ✅ Updates workflow JSON to use `query_id` instead of inline queries

## Quick Start (3 Steps)

### Step 1: Verify Warehouse ID

```bash
databricks --profile lapd-crime warehouses list
```

Note the warehouse ID (or use default: `ba90f266beeaacb2`)

### Step 2: Run Setup Script

```bash
cd /Users/mahimathakur/lapd-crime/lapd-crime
./scripts/setup_databricks_job.sh lapd-crime ba90f266beeaacb2
```

This script will:
- Grant `CAN_USE` permission on the warehouse
- Create 7 Query objects from your SQL files
- Grant `CAN_RUN` permission on each query
- Update `databricks/workflows/lapd_crime_pipeline.json` with query IDs

### Step 3: Create/Update Job

```bash
# Create new job
databricks --profile lapd-crime jobs create --json @databricks/workflows/lapd_crime_pipeline.json

# Or update existing job (replace JOB_ID)
databricks --profile lapd-crime jobs update JOB_ID --json @databricks/workflows/lapd_crime_pipeline.json
```

### Step 4: Run the Job

```bash
databricks --profile lapd-crime jobs run-now JOB_ID
```

## What Was Created

### 1. Permission Files
- `permissions/warehouse_can_use.json` - Warehouse permissions
- `permissions/query_can_run.json` - Query permissions

### 2. SQL Files with Grants
- `databricks/notebooks/00_uc_bootstrap_with_grants.sql` - Catalog + schemas + all grants
- `databricks/notebooks/01_external_location_with_grants.sql` - Storage credential + external location + grants

### 3. Scripts
- `scripts/setup_databricks_job.sh` - Master setup script (does everything)
- `scripts/create_queries.sh` - Creates queries from SQL files
- `scripts/update_workflow_with_query_ids.sh` - Updates workflow JSON

### 4. Build Artifacts (created by scripts)
- `build/query_payloads/` - Query creation payloads
- `build/query_ids/` - Created query IDs (one per task)

## Workflow JSON Structure

After running the setup, your workflow JSON will have:

```json
{
  "name": "lapd-crime-sql-pipeline",
  "run_as": { "user_name": "mahima.thakur@sigmoidanalytics.com" },
  "tasks": [
    {
      "task_key": "00_uc_bootstrap",
      "sql_task": {
        "warehouse_id": "ba90f266beeaacb2",
        "query": {
          "query_id": "abc123..."  // Real query ID from Databricks
        }
      }
    },
    // ... other tasks
  ]
}
```

## Troubleshooting

### Error: "jq: command not found"
```bash
brew install jq
```

### Error: "SQL entity could not be found"
1. Verify warehouse ID: `databricks --profile lapd-crime warehouses list`
2. Check queries exist: Query IDs should be in `build/query_ids/`
3. Re-run setup: `./scripts/setup_databricks_job.sh`

### Error: "Permission denied"
1. Check warehouse permissions were granted
2. Verify `run_as.user_name` matches the user in permission files
3. Re-grant permissions manually if needed

### Query IDs are empty
- Re-run `./scripts/create_queries.sh`
- Check that SQL files exist in `databricks/notebooks/`
- Verify warehouse ID is correct

## Manual Steps (if scripts fail)

### 1. Grant Warehouse Permission
```bash
databricks --profile lapd-crime warehouses update-permissions ba90f266beeaacb2 \
  --json @permissions/warehouse_can_use.json
```

### 2. Create One Query Manually (example)
```bash
jq -n \
  --arg dn "00_uc_bootstrap" \
  --arg wid "ba90f266beeaacb2" \
  --rawfile qt "databricks/notebooks/00_uc_bootstrap_with_grants.sql" \
'{
  query: {
    display_name: $dn,
    warehouse_id: $wid,
    query_text: $qt
  }
}' > /tmp/query.json

QUERY_ID=$(databricks --profile lapd-crime queries create --json @/tmp/query.json | jq -r '.id')
echo $QUERY_ID
```

### 3. Grant Query Permission
```bash
databricks --profile lapd-crime permissions update queries "$QUERY_ID" \
  --json @permissions/query_can_run.json
```

## Next Steps

1. ✅ Run setup script
2. ✅ Create/update job
3. ✅ Run job
4. ✅ Monitor job runs in Databricks UI

## Key Files Reference

| File | Purpose |
|------|---------|
| `scripts/setup_databricks_job.sh` | Master setup script |
| `databricks/workflows/lapd_crime_pipeline.json` | Job workflow (updated with query_ids) |
| `permissions/*.json` | Permission templates |
| `build/query_ids/*.id` | Created query IDs (after running setup) |

## Support

If you encounter issues:
1. Check `scripts/README_SETUP.md` for detailed troubleshooting
2. Verify all prerequisites (jq, databricks CLI, correct profile)
3. Check Databricks UI to verify queries were created
4. Verify warehouse is accessible and running

