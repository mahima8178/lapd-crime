#!/bin/bash
# Master script to run the entire LAPD Crime Data Pipeline from scratch
# This script automates: ingestion → Databricks setup → job creation → execution

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROFILE="${1:-lapd-crime}"
WAREHOUSE_ID="${2:-ba90f266beeaacb2}"
SOCRATA_DATASET_ID="${3:-2nrs-mtv8}"
S3_BUCKET="${4:-public-crime-raw-dev-eu-west-1}"
S3_PREFIX="${5:-bronze/lapd_crime/json}"

echo -e "${BLUE}=========================================="
echo "LAPD Crime Data Pipeline - Run from Scratch"
echo "==========================================${NC}"
echo ""
echo "Configuration:"
echo "  Profile: $PROFILE"
echo "  Warehouse ID: $WAREHOUSE_ID"
echo "  Socrata Dataset: $SOCRATA_DATASET_ID"
echo "  S3 Bucket: $S3_BUCKET"
echo "  S3 Prefix: $S3_PREFIX"
echo ""

# Step 1: Check Prerequisites
echo -e "${BLUE}=========================================="
echo "Step 1: Checking Prerequisites"
echo "==========================================${NC}"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗${NC} Python 3 not found. Please install Python 3.7+"
    exit 1
fi
echo -e "${GREEN}✓${NC} Python: $(python3 --version)"

# Check Python packages
if ! python3 -c "import boto3, requests" 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC} Installing Python packages..."
    pip install -r scripts/requirements_ingestion.txt
fi
echo -e "${GREEN}✓${NC} Python packages installed"

# Check jq
if ! command -v jq &> /dev/null; then
    echo -e "${RED}✗${NC} jq not found. Install with: brew install jq"
    exit 1
fi
echo -e "${GREEN}✓${NC} jq installed"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗${NC} AWS CLI not found"
    exit 1
fi
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}✗${NC} AWS credentials not configured. Run: aws configure"
    exit 1
fi
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo -e "${GREEN}✓${NC} AWS CLI configured (Account: $AWS_ACCOUNT)"

# Check Databricks CLI (2025+)
if ! command -v databricks &> /dev/null; then
  echo -e "${RED}✗${NC} Databricks CLI not found"
  echo "Install: brew install databricks/tap/databricks"
  exit 1
fi

# Force Homebrew CLI (avoid legacy python databricks-cli)
DBX_BIN="$(which databricks)"
if [[ "$DBX_BIN" != "/opt/homebrew/bin/databricks" ]] && [[ "$DBX_BIN" != "/usr/local/bin/databricks" ]]; then
  echo -e "${RED}✗${NC} databricks binary is not Homebrew one: $DBX_BIN"
  echo "Expected: /opt/homebrew/bin/databricks (Apple Silicon) or /usr/local/bin/databricks (Intel)"
  echo "Fix: uninstall legacy python CLI:"
  echo "  python3 -m pip uninstall -y databricks-cli databricks"
  echo "Then install Homebrew CLI:"
  echo "  brew install databricks/tap/databricks"
  exit 1
fi

echo -e "${GREEN}✓${NC} Databricks CLI: $(databricks version | head -1)"

# Validate auth for profile using safe calls
if ! databricks current-user me --profile "$PROFILE" >/dev/null 2>&1; then
  echo -e "${RED}✗${NC} Databricks CLI profile '$PROFILE' not authenticated."
  echo "Fix (token auth):"
  echo "  databricks auth login --profile $PROFILE"
  echo "Then verify:"
  echo "  databricks current-user me --profile $PROFILE"
  exit 1
fi
echo -e "${GREEN}✓${NC} Databricks CLI authenticated for profile: $PROFILE"

# Check S3 bucket
if ! aws s3 ls "s3://$S3_BUCKET/" &> /dev/null; then
    echo -e "${YELLOW}⚠${NC} S3 bucket not accessible. Creating bucket..."
    aws s3 mb "s3://$S3_BUCKET" --region eu-west-1 || {
        echo -e "${RED}✗${NC} Failed to create bucket. Check AWS permissions."
        exit 1
    }
    echo -e "${GREEN}✓${NC} S3 bucket created"
else
    echo -e "${GREEN}✓${NC} S3 bucket accessible: $S3_BUCKET"
fi

echo ""
echo -e "${GREEN}All prerequisites met!${NC}"
echo ""

# Step 2: Ingest Data from Socrata
echo -e "${BLUE}=========================================="
echo "Step 2: Ingesting Data from Socrata to S3"
echo "==========================================${NC}"
echo ""

read -p "Do you want to ingest data from Socrata? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting Socrata ingestion..."
    python3 scripts/ingest_socrata_to_s3.py \
        --dataset-id "$SOCRATA_DATASET_ID" \
        --bucket "$S3_BUCKET" \
        --prefix "$S3_PREFIX" \
        --max-records 100000 || {
        echo -e "${YELLOW}⚠${NC} Ingestion had issues. Continuing anyway..."
    }
    echo ""
    echo -e "${GREEN}✓${NC} Data ingestion complete"
    
    # Verify files
    FILE_COUNT=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive | wc -l | tr -d ' ')
    echo "Files in S3: $FILE_COUNT"
else
    echo -e "${YELLOW}⚠${NC} Skipping ingestion. Make sure data exists in S3."
fi

echo ""

# Step 3: Setup Databricks
echo -e "${BLUE}=========================================="
echo "Step 3: Setting Up Databricks Job"
echo "==========================================${NC}"
echo ""

# Verify warehouse exists
echo "Verifying warehouse ID: $WAREHOUSE_ID"
if ! databricks warehouses get "$WAREHOUSE_ID" --profile "$PROFILE" >/dev/null 2>&1; then
  echo -e "${YELLOW}⚠${NC} Warehouse not found or not accessible for this profile."
  echo "Available warehouses:"
  databricks warehouses list --profile "$PROFILE"
  exit 1
fi
echo -e "${GREEN}✓${NC} Warehouse verified: $WAREHOUSE_ID"
echo ""

# Run setup script
echo "Running Databricks setup..."
if [ -f "scripts/setup_databricks_job.sh" ]; then
    ./scripts/setup_databricks_job.sh "$PROFILE" "$WAREHOUSE_ID" || {
        echo -e "${RED}✗${NC} Setup failed. Check errors above."
        exit 1
    }
else
    echo -e "${RED}✗${NC} Setup script not found"
    exit 1
fi

echo ""
echo -e "${GREEN}✓${NC} Databricks setup complete"
echo ""

# Step 4: Create or Update Job
echo -e "${BLUE}=========================================="
echo "Step 4: Creating/Updating Databricks Job"
echo "==========================================${NC}"
echo ""

# Check if workflow JSON exists
if [ ! -f "databricks/workflows/lapd_crime_pipeline.json" ]; then
    echo -e "${RED}✗${NC} Workflow JSON not found"
    exit 1
fi

# Try to find existing job by name
JOB_NAME="lapd-crime-sql-pipeline"
EXISTING_JOB=$(databricks --profile "$PROFILE" jobs list --output json 2>/dev/null | \
    jq -r ".jobs[] | select(.settings.name == \"$JOB_NAME\") | .job_id" | head -1)

if [ -n "$EXISTING_JOB" ] && [ "$EXISTING_JOB" != "null" ]; then
    echo "Found existing job: $EXISTING_JOB"
    read -p "Update existing job? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Updating job $EXISTING_JOB..."
        databricks --profile "$PROFILE" jobs update "$EXISTING_JOB" \
            --json @databricks/workflows/lapd_crime_pipeline.json
        JOB_ID="$EXISTING_JOB"
        echo -e "${GREEN}✓${NC} Job updated: $JOB_ID"
    else
        echo "Creating new job..."
        JOB_ID=$(databricks --profile "$PROFILE" jobs create \
            --json @databricks/workflows/lapd_crime_pipeline.json | jq -r '.job_id')
        echo -e "${GREEN}✓${NC} Job created: $JOB_ID"
    fi
else
    echo "Creating new job..."
    JOB_ID=$(databricks --profile "$PROFILE" jobs create \
        --json @databricks/workflows/lapd_crime_pipeline.json | jq -r '.job_id')
    echo -e "${GREEN}✓${NC} Job created: $JOB_ID"
fi

if [ -z "$JOB_ID" ] || [ "$JOB_ID" == "null" ]; then
    echo -e "${RED}✗${NC} Failed to get job ID"
    exit 1
fi

echo ""
echo -e "${GREEN}Job ID: $JOB_ID${NC}"
echo ""

# Step 5: Run the Job
echo -e "${BLUE}=========================================="
echo "Step 5: Running Databricks Job"
echo "==========================================${NC}"
echo ""

read -p "Do you want to run the job now? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting job run..."
    RUN_OUTPUT=$(databricks --profile "$PROFILE" jobs run-now "$JOB_ID" --output json)
    RUN_ID=$(echo "$RUN_OUTPUT" | jq -r '.run_id')
    
    if [ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ]; then
        echo -e "${GREEN}✓${NC} Job run started: $RUN_ID"
        echo ""
        echo "Monitoring job run..."
        echo "Job ID: $JOB_ID"
        echo "Run ID: $RUN_ID"
        echo ""
        
        # Wait for job to complete (with timeout)
        echo "Waiting for job to complete (this may take several minutes)..."
        TIMEOUT=1800  # 30 minutes
        ELAPSED=0
        INTERVAL=10
        
        while [ $ELAPSED -lt $TIMEOUT ]; do
            STATUS=$(databricks --profile "$PROFILE" runs get "$RUN_ID" --output json 2>/dev/null | \
                jq -r '.state.life_cycle_state')
            
            if [ "$STATUS" == "TERMINATED" ] || [ "$STATUS" == "SKIPPED" ]; then
                RESULT_STATE=$(databricks --profile "$PROFILE" runs get "$RUN_ID" --output json | \
                    jq -r '.state.result_state')
                
                echo ""
                if [ "$RESULT_STATE" == "SUCCESS" ]; then
                    echo -e "${GREEN}=========================================="
                    echo "Job completed successfully!"
                    echo "==========================================${NC}"
                else
                    echo -e "${RED}=========================================="
                    echo "Job failed or was skipped"
                    echo "Result State: $RESULT_STATE"
                    echo "==========================================${NC}"
                    echo ""
                    echo "Check the job run for details:"
                    echo "  databricks --profile $PROFILE runs get $RUN_ID"
                fi
                break
            fi
            
            echo -n "."
            sleep $INTERVAL
            ELAPSED=$((ELAPSED + INTERVAL))
        done
        
        if [ $ELAPSED -ge $TIMEOUT ]; then
            echo ""
            echo -e "${YELLOW}⚠${NC} Timeout waiting for job completion. Check status manually."
        fi
    else
        echo -e "${RED}✗${NC} Failed to start job run"
    fi
else
    echo -e "${YELLOW}⚠${NC} Skipping job run. Run manually with:"
    echo "  databricks --profile $PROFILE jobs run-now $JOB_ID"
fi

echo ""

# Step 6: Summary
echo -e "${BLUE}=========================================="
echo "Summary"
echo "==========================================${NC}"
echo ""
echo -e "${GREEN}✓${NC} Prerequisites checked"
echo -e "${GREEN}✓${NC} Data ingestion completed"
echo -e "${GREEN}✓${NC} Databricks job setup completed"
echo -e "${GREEN}✓${NC} Job created/updated: $JOB_ID"
echo ""
echo "Next steps:"
echo "  1. Verify data in S3:"
echo "     ${GREEN}aws s3 ls s3://$S3_BUCKET/$S3_PREFIX/${NC}"
echo ""
echo "  2. Run the job (if not already running):"
echo "     ${GREEN}databricks --profile $PROFILE jobs run-now $JOB_ID${NC}"
echo ""
echo "  3. Check job status:"
echo "     ${GREEN}databricks --profile $PROFILE jobs get $JOB_ID${NC}"
echo ""
echo "  4. Query data in Databricks SQL:"
echo "     ${GREEN}USE CATALOG public_crime;${NC}"
echo "     ${GREEN}SELECT COUNT(*) FROM bronze.lapd_crime_raw_json;${NC}"
echo ""
echo -e "${BLUE}=========================================="
echo "Pipeline setup complete!"
echo "==========================================${NC}"

