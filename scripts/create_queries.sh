#!/bin/bash
# Script to create Databricks SQL queries from SQL files and grant permissions

set -e

PROFILE="${1:-lapd-crime}"
WAREHOUSE_ID="${2:-ba90f266beeaacb2}"
USER_EMAIL="mahima.thakur@sigmoidanalytics.com"

echo "Creating queries with warehouse_id: $WAREHOUSE_ID"
echo "Profile: $PROFILE"
echo "User: $USER_EMAIL"
echo ""

# Create directories
mkdir -p build/query_payloads build/query_ids

# Function to create a query
create_query() {
    local task_key=$1
    local display_name=$2
    local sql_file=$3
    
    echo "Creating query: $display_name from $sql_file"
    
    # Create query payload
    jq -n \
        --arg dn "$display_name" \
        --arg wid "$WAREHOUSE_ID" \
        --rawfile qt "$sql_file" \
    '{
      query: {
        display_name: $dn,
        warehouse_id: $wid,
        query_text: $qt
      }
    }' > "build/query_payloads/${task_key}.json"
    
    # Create the query and get the ID
    local query_id=$(databricks --profile "$PROFILE" queries create \
        --json "@build/query_payloads/${task_key}.json" | jq -r '.id')
    
    if [ -z "$query_id" ] || [ "$query_id" == "null" ]; then
        echo "ERROR: Failed to create query $display_name"
        exit 1
    fi
    
    echo "$query_id" > "build/query_ids/${task_key}.id"
    echo "Created query $display_name with ID: $query_id"
    
    # Grant permissions
    echo "Granting CAN_RUN permission on query $query_id..."
    databricks --profile "$PROFILE" permissions update queries "$query_id" \
        --json "@permissions/query_can_run.json"
    
    echo ""
}

# Create all queries
create_query "00_uc_bootstrap" "00_uc_bootstrap" "databricks/notebooks/00_uc_bootstrap_with_grants.sql"
create_query "01_external_location" "01_external_location" "databricks/notebooks/01_external_location_with_grants.sql"
create_query "02_bronze" "02_bronze" "databricks/notebooks/02_bronze.sql"
create_query "03_silver_fact" "03_silver_fact" "databricks/notebooks/03_silver_fact.sql"
create_query "04_silver_dim" "04_silver_dim" "databricks/notebooks/04_silver_dim.sql"
create_query "05_gold_agg_1" "05_gold_agg_1" "databricks/notebooks/05_gold_agg_1.sql"
create_query "06_gold_agg_2" "06_gold_agg_2" "databricks/notebooks/06_gold_agg_2.sql"

echo "All queries created successfully!"
echo ""
echo "Query IDs saved in build/query_ids/"
echo ""
echo "Next steps:"
echo "1. Grant CAN_USE on warehouse:"
echo "   databricks --profile $PROFILE warehouses update-permissions $WAREHOUSE_ID --json @permissions/warehouse_can_use.json"
echo ""
echo "2. Update workflow JSON with query IDs from build/query_ids/"
echo "3. Create/update the job with the workflow JSON"

