#!/bin/bash
# Script to debug job failures by getting detailed run output

set -e

JOB_ID="${1}"
RUN_ID="${2}"

if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 <job_id> [run_id]"
    echo "If run_id not provided, will get the latest run"
    exit 1
fi

PROFILE="${3:-lapd-crime}"

if [ -z "$RUN_ID" ]; then
    echo "Getting latest run for job $JOB_ID..."
    RUN_ID=$(databricks --profile "$PROFILE" jobs list-runs --job-id "$JOB_ID" --limit 1 --output json 2>/dev/null | \
        jq -r '.[0].run_id // .runs[0].run_id // empty')
    
    if [ -z "$RUN_ID" ] || [ "$RUN_ID" == "null" ]; then
        echo "ERROR: Could not get run ID"
        exit 1
    fi
    echo "Run ID: $RUN_ID"
fi

echo ""
echo "=========================================="
echo "Job Run Details"
echo "=========================================="
echo "Job ID: $JOB_ID"
echo "Run ID: $RUN_ID"
echo ""

# Get run details
echo "Run State:"
databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json 2>/dev/null | \
    jq -r '.state | {life_cycle_state, result_state, state_message}'

echo ""
echo "Task Details:"
databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json 2>/dev/null | \
    jq -r '.tasks[] | "\(.task_key): \(.state.life_cycle_state) - \(.state.result_state // "N/A")"'

echo ""
echo "Failed Task Details:"
databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json 2>/dev/null | \
    jq -r '.tasks[] | select(.state.result_state == "FAILED" or .state.life_cycle_state == "INTERNAL_ERROR") | 
    "Task: \(.task_key)\nState: \(.state.life_cycle_state)\nResult: \(.state.result_state // "N/A")\nMessage: \(.state.state_message // "N/A")\n"'

echo ""
echo "View in UI:"
echo "https://dbc-09c1dd7c-05dd.cloud.databricks.com/?o=4208328546714555#job/$JOB_ID/run/$RUN_ID"
echo ""

