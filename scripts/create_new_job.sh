#!/bin/bash
# Script to create new queries and create a new Databricks job

set -e

PROFILE="${1:-lapd-crime}"
WAREHOUSE_ID="${2:-ba90f266beeaacb2}"

echo "=========================================="
echo "Create New Job with New Queries"
echo "=========================================="
echo "Profile: $PROFILE"
echo "Warehouse ID: $WAREHOUSE_ID"
echo ""

# Step 1: Create all queries from SQL files
echo "Step 1: Creating queries from SQL files..."
./scripts/create_queries.sh "$PROFILE" "$WAREHOUSE_ID"

# Step 2: Update workflow JSON with new query IDs
echo ""
echo "Step 2: Updating workflow JSON with new query IDs..."
./scripts/update_workflow_with_query_ids.sh

# Step 3: Create the new job
echo ""
echo "Step 3: Creating new Databricks job..."
JOB_OUTPUT=$(databricks --profile "$PROFILE" jobs create \
  --json @databricks/workflows/lapd_crime_pipeline.json --output json)

JOB_ID=$(echo "$JOB_OUTPUT" | jq -r '.job_id')

if [ -z "$JOB_ID" ] || [ "$JOB_ID" == "null" ]; then
    echo "ERROR: Failed to create job"
    echo "$JOB_OUTPUT"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ New job created successfully!"
echo "=========================================="
echo ""
echo "Job ID: $JOB_ID"
echo ""
echo "Query IDs created:"
for file in build/query_ids/*.id; do
    if [ -f "$file" ]; then
        task=$(basename "$file" .id)
        query_id=$(cat "$file")
        echo "  $task: $query_id"
    fi
done
echo ""
echo "Next steps:"
echo "  1. Run the job:"
echo "     databricks --profile $PROFILE jobs run-now $JOB_ID"
echo ""
echo "  2. Check job status:"
echo "     databricks --profile $PROFILE jobs get $JOB_ID"
echo ""
echo "  3. List job runs:"
echo "     databricks --profile $PROFILE jobs list-runs --job-id $JOB_ID"
echo ""

