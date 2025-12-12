#!/bin/bash
# Quick Start Script - Checks prerequisites and guides you through setup

set -e

echo "=========================================="
echo "LAPD Crime Data Pipeline - Quick Start"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    echo -e "${GREEN}✓${NC} Python: $PYTHON_VERSION"
else
    echo -e "${RED}✗${NC} Python 3 not found. Please install Python 3.7+"
    exit 1
fi

# Check pip packages
echo "Checking Python packages..."
if python3 -c "import boto3, requests" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Required Python packages installed"
else
    echo -e "${YELLOW}⚠${NC} Installing required packages..."
    pip install -r scripts/requirements_ingestion.txt
fi

# Check jq
if command -v jq &> /dev/null; then
    echo -e "${GREEN}✓${NC} jq installed"
else
    echo -e "${YELLOW}⚠${NC} jq not found. Install with: brew install jq"
fi

# Check AWS CLI
if command -v aws &> /dev/null; then
    if aws sts get-caller-identity &> /dev/null; then
        AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
        echo -e "${GREEN}✓${NC} AWS CLI configured (Account: $AWS_ACCOUNT)"
    else
        echo -e "${RED}✗${NC} AWS credentials not configured. Run: aws configure"
        exit 1
    fi
else
    echo -e "${RED}✗${NC} AWS CLI not found. Please install AWS CLI"
    exit 1
fi

# Check Databricks CLI
if command -v databricks &> /dev/null; then
    if databricks --profile lapd-crime workspaces list &> /dev/null; then
        echo -e "${GREEN}✓${NC} Databricks CLI configured"
    else
        echo -e "${YELLOW}⚠${NC} Databricks CLI not configured. Run: databricks configure --profile lapd-crime"
    fi
else
    echo -e "${RED}✗${NC} Databricks CLI not found. Please install Databricks CLI"
    exit 1
fi

# Check S3 bucket
echo ""
echo "Checking S3 bucket access..."
if aws s3 ls s3://public-crime-raw-dev-eu-west-1/ &> /dev/null; then
    echo -e "${GREEN}✓${NC} S3 bucket accessible: public-crime-raw-dev-eu-west-1"
else
    echo -e "${YELLOW}⚠${NC} S3 bucket not accessible. Creating bucket..."
    aws s3 mb s3://public-crime-raw-dev-eu-west-1 --region eu-west-1 || {
        echo -e "${RED}✗${NC} Failed to create bucket. Check AWS permissions."
        exit 1
    }
    echo -e "${GREEN}✓${NC} S3 bucket created"
fi

echo ""
echo "=========================================="
echo "Prerequisites Check Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Ingest data from Socrata to S3:"
echo "   ${GREEN}python scripts/ingest_socrata_to_s3.py${NC}"
echo ""
echo "2. Get your Databricks warehouse ID:"
echo "   ${GREEN}databricks --profile lapd-crime warehouses list${NC}"
echo ""
echo "3. Setup Databricks job (replace <warehouse_id>):"
echo "   ${GREEN}./scripts/setup_databricks_job.sh lapd-crime <warehouse_id>${NC}"
echo ""
echo "4. Create the job:"
echo "   ${GREEN}databricks --profile lapd-crime jobs create --json @databricks/workflows/lapd_crime_pipeline.json${NC}"
echo ""
echo "5. Run the job (replace <job_id>):"
echo "   ${GREEN}databricks --profile lapd-crime jobs run-now <job_id>${NC}"
echo ""
echo "For detailed instructions, see: ${GREEN}GETTING_STARTED.md${NC}"
echo ""

