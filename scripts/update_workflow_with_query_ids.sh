#!/bin/bash
# Script to update workflow JSON with query IDs from build/query_ids/

set -e

WORKFLOW_FILE="databricks/workflows/lapd_crime_pipeline.json"
QUERY_IDS_DIR="build/query_ids"

if [ ! -d "$QUERY_IDS_DIR" ]; then
    echo "ERROR: Query IDs directory not found. Run create_queries.sh first."
    exit 1
fi

# Read query IDs
Q00_ID=$(cat "$QUERY_IDS_DIR/00_uc_bootstrap.id" 2>/dev/null || echo "")
Q01_ID=$(cat "$QUERY_IDS_DIR/01_external_location.id" 2>/dev/null || echo "")
Q02_ID=$(cat "$QUERY_IDS_DIR/02_bronze.id" 2>/dev/null || echo "")
Q03_ID=$(cat "$QUERY_IDS_DIR/03_silver_fact.id" 2>/dev/null || echo "")
Q04_ID=$(cat "$QUERY_IDS_DIR/04_silver_dim.id" 2>/dev/null || echo "")
Q05_ID=$(cat "$QUERY_IDS_DIR/05_gold_agg_1.id" 2>/dev/null || echo "")
Q06_ID=$(cat "$QUERY_IDS_DIR/06_gold_agg_2.id" 2>/dev/null || echo "")

if [ -z "$Q00_ID" ] || [ -z "$Q01_ID" ] || [ -z "$Q02_ID" ] || [ -z "$Q03_ID" ] || [ -z "$Q04_ID" ] || [ -z "$Q05_ID" ] || [ -z "$Q06_ID" ]; then
    echo "ERROR: Some query IDs are missing. Run create_queries.sh first."
    exit 1
fi

echo "Updating workflow JSON with query IDs..."
echo "Q00_ID: $Q00_ID"
echo "Q01_ID: $Q01_ID"
echo "Q02_ID: $Q02_ID"
echo "Q03_ID: $Q03_ID"
echo "Q04_ID: $Q04_ID"
echo "Q05_ID: $Q05_ID"
echo "Q06_ID: $Q06_ID"
echo ""

# Create a Python script to update the JSON
python3 << EOF
import json
import sys

workflow_file = "$WORKFLOW_FILE"
query_ids = {
    "00_uc_bootstrap": "$Q00_ID",
    "01_external_location": "$Q01_ID",
    "02_bronze": "$Q02_ID",
    "03_silver_fact": "$Q03_ID",
    "04_silver_dim": "$Q04_ID",
    "05_gold_agg_1": "$Q05_ID",
    "06_gold_agg_2": "$Q06_ID"
}

with open(workflow_file, 'r') as f:
    workflow = json.load(f)

# Update each task to use query_id instead of inline query
for task in workflow['tasks']:
    task_key = task['task_key']
    if task_key in query_ids:
        # Replace query.query with query.query_id
        if 'sql_task' in task and 'query' in task['sql_task']:
            # Remove the nested query object and replace with query_id
            task['sql_task']['query'] = {
                "query_id": query_ids[task_key]
            }
            print(f"Updated {task_key} with query_id: {query_ids[task_key]}")

# Write back
with open(workflow_file, 'w') as f:
    json.dump(workflow, f, indent=2)

print(f"\nWorkflow updated successfully: {workflow_file}")
EOF

echo ""
echo "Workflow JSON updated! You can now create/update the job:"
echo "  databricks --profile lapd-crime jobs create --json @$WORKFLOW_FILE"

