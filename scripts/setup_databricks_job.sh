#!/bin/bash
# Master script to set up Databricks job with queries and permissions
# Usage: ./scripts/setup_databricks_job.sh [profile] [warehouse_id]

set -e

PROFILE="${1:-lapd-crime}"
WAREHOUSE_ID="${2:-ba90f266beeaacb2}"
USER_EMAIL="mahima.thakur@sigmoidanalytics.com"

echo "=========================================="
echo "Databricks Job Setup Script"
echo "=========================================="
echo "Profile: $PROFILE"
echo "Warehouse ID: $WAREHOUSE_ID"
echo "User: $USER_EMAIL"
echo ""

# Step 1: Grant warehouse permissions
echo "Step 1: Granting CAN_USE permission on warehouse..."
databricks --profile "$PROFILE" warehouses update-permissions "$WAREHOUSE_ID" \
    --json @permissions/warehouse_can_use.json || {
    echo "WARNING: Failed to grant warehouse permissions. Continuing..."
}

# Step 2: Create queries
echo ""
echo "Step 2: Creating SQL queries from files..."
./scripts/create_queries.sh "$PROFILE" "$WAREHOUSE_ID"

# Step 3: Update workflow with query IDs
echo ""
echo "Step 3: Query IDs saved in build/query_ids/"
echo "Note: Update databricks/workflows/lapd_crime_pipeline.json manually with query IDs from build/query_ids/"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review the updated workflow: databricks/workflows/lapd_crime_pipeline.json"
echo "2. Create or update the job:"
echo "   databricks --profile $PROFILE jobs create --json @databricks/workflows/lapd_crime_pipeline.json"
echo ""
echo "   Or update existing job:"
echo "   databricks --profile $PROFILE jobs update <job_id> --json @databricks/workflows/lapd_crime_pipeline.json"
echo ""
echo "3. Run the job:"
echo "   databricks --profile $PROFILE jobs run-now <job_id>"
echo ""

