#!/bin/bash
# Script to check job run details and get error messages

set -e

PROFILE="${1:-lapd-crime}"
JOB_ID="${2}"

if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 [profile] <job_id>"
    echo "Example: $0 lapd-crime 717410263184866"
    exit 1
fi

echo "Getting latest run for job $JOB_ID..."

# Get the latest run ID
RUN_ID=$(databricks --profile "$PROFILE" jobs runs list --job-id "$JOB_ID" --output json 2>/dev/null | \
    jq -r '.runs[0].run_id // empty')

if [ -z "$RUN_ID" ] || [ "$RUN_ID" == "null" ]; then
    echo "No runs found for job $JOB_ID"
    exit 1
fi

echo "Run ID: $RUN_ID"
echo ""

# Get run details
echo "=== Run Details ==="
databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json | jq '{
    run_id: .run_id,
    state: .state,
    tasks: [.tasks[] | {
        task_key: .task_key,
        state: .state,
        run_id: .run_id
    }]
}'

echo ""
echo "=== Task Details ==="
# Get details for each task
for TASK_KEY in $(databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json | \
    jq -r '.tasks[].task_key'); do
    echo ""
    echo "Task: $TASK_KEY"
    TASK_RUN_ID=$(databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json | \
        jq -r ".tasks[] | select(.task_key == \"$TASK_KEY\") | .run_id")
    
    if [ -n "$TASK_RUN_ID" ] && [ "$TASK_RUN_ID" != "null" ]; then
        echo "Task Run ID: $TASK_RUN_ID"
        echo "State:"
        databricks --profile "$PROFILE" jobs runs get "$RUN_ID" --output json | \
            jq -r ".tasks[] | select(.task_key == \"$TASK_KEY\") | .state | {life_cycle_state, result_state, state_message}"
        
        # Try to get SQL output if it's a SQL task
        echo "Output (if available):"
        databricks --profile "$PROFILE" jobs runs get-output "$TASK_RUN_ID" --output json 2>/dev/null | \
            jq -r '.notebook_output.result // .error // "No output available"' || echo "Could not retrieve output"
    fi
done

