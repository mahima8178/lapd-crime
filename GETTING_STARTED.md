# Getting Started with LAPD Crime Data Pipeline

This guide will walk you through setting up and running the complete LAPD crime data pipeline from Socrata API to Databricks.

## Prerequisites

### 1. Required Software

- **Python 3.7+** with pip
- **Databricks CLI** installed and configured
- **AWS CLI** configured with credentials
- **jq** (for JSON processing in scripts)
- **Git**

### 2. Install Required Tools

```bash
# Install Python dependencies
pip install -r scripts/requirements_ingestion.txt

# Install jq (macOS)
brew install jq

# Verify Databricks CLI
databricks --version

# Verify AWS CLI
aws --version
```

### 3. Configure Credentials

#### AWS Credentials
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Default region: eu-west-1
```

Or set environment variables:
```bash
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export AWS_DEFAULT_REGION="eu-west-1"
```

#### Databricks CLI
```bash
databricks configure --profile lapd-crime
# Enter your Databricks host (e.g., https://your-workspace.cloud.databricks.com)
# Enter your personal access token
```

#### Socrata App Token (Optional but Recommended)
```bash
export SOCRATA_APP_TOKEN="adI4e1VB9UbrBu26tygqa6Hzh"
```

## Step-by-Step Setup

### Step 1: Clone and Navigate to Project

```bash
cd /Users/mahimathakur/lapd-crime/lapd-crime
```

### Step 2: Verify S3 Bucket Access

```bash
# Test S3 access
aws s3 ls s3://public-crime-raw-dev-eu-west-1/

# If bucket doesn't exist, create it
aws s3 mb s3://public-crime-raw-dev-eu-west-1 --region eu-west-1
```

### Step 3: Ingest Data from Socrata to S3

This step fetches LAPD crime data from the Socrata API and uploads it to S3.

```bash
# Set environment variables (optional - defaults are in the script)
export SOCRATA_DATASET_ID="2nrs-mtv8"
export S3_BUCKET="public-crime-raw-dev-eu-west-1"
export S3_PREFIX="bronze/lapd_crime/json"

# Run the ingestion script
python scripts/ingest_socrata_to_s3.py

# Or with custom parameters
python scripts/ingest_socrata_to_s3.py \
  --dataset-id "2nrs-mtv8" \
  --bucket "public-crime-raw-dev-eu-west-1" \
  --prefix "bronze/lapd_crime/json" \
  --max-records 100000  # Optional: limit for testing
```

**Expected Output:**
```
============================================================
Socrata to S3 Ingestion
============================================================
Domain: data.lacity.org
Dataset ID: 2nrs-mtv8
S3 Bucket: public-crime-raw-dev-eu-west-1
S3 Prefix: bronze/lapd_crime/json
Max Records: All
============================================================

Fetching data from Socrata: offset=0, limit=50000
Uploading 50000 records to s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/lapd_crime_20241201_120000_part0001.json
✓ Uploaded 50000 records (Total: 50000)
...
============================================================
Ingestion complete! Total records: 2500000
Files uploaded to: s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/
============================================================
```

**Verify files in S3:**
```bash
aws s3 ls s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/ --recursive
```

### Step 4: Set Up Databricks Job

#### 4.1 Get Your SQL Warehouse ID

```bash
# List available warehouses
databricks --profile lapd-crime warehouses list

# Note the warehouse ID (e.g., ba90f266beeaacb2)
```

#### 4.2 Run Setup Script

This script will:
- Grant permissions on the warehouse
- Create SQL queries from your SQL files
- Grant permissions on queries
- Update the workflow JSON with query IDs

```bash
# Make scripts executable (if not already)
chmod +x scripts/*.sh

# Run the setup (replace warehouse_id if different)
./scripts/setup_databricks_job.sh lapd-crime ba90f266beeaacb2
```

**Expected Output:**
```
==========================================
Databricks Job Setup Script
==========================================
Profile: lapd-crime
Warehouse ID: ba90f266beeaacb2
User: mahima.thakur@sigmoidanalytics.com
==========================================

Step 1: Granting CAN_USE permission on warehouse...
Step 2: Creating SQL queries from files...
Creating query: 00_uc_bootstrap from databricks/notebooks/00_uc_bootstrap_with_grants.sql
Created query 00_uc_bootstrap with ID: db49f93c-2f1e-44ff-8f14-a07b215b1ed8
...
Step 3: Updating workflow JSON with query IDs...
==========================================
Setup Complete!
==========================================
```

#### 4.3 Create the Databricks Job

```bash
# Create new job
databricks --profile lapd-crime jobs create \
  --json @databricks/workflows/lapd_crime_pipeline.json

# Save the job_id from the output (e.g., 1019833110683083)
```

Or update existing job:
```bash
databricks --profile lapd-crime jobs update <job_id> \
  --json @databricks/workflows/lapd_crime_pipeline.json
```

### Step 5: Run the Databricks Pipeline

```bash
# Run the job (replace JOB_ID with your actual job ID)
databricks --profile lapd-crime jobs run-now <JOB_ID>

# Or run with no-wait flag
databricks --profile lapd-crime jobs run-now <JOB_ID> --no-wait
```

**Monitor the job:**
- Check job status: `databricks --profile lapd-crime jobs get <JOB_ID>`
- View in Databricks UI: Go to Jobs → Your job → Runs

## Pipeline Overview

The pipeline consists of 7 tasks that run in sequence:

1. **00_uc_bootstrap** - Creates catalog and schemas (bronze/silver/gold) with grants
2. **01_external_location** - Creates storage credential and external location with grants
3. **02_bronze** - Creates external table over S3 JSON files
4. **03_silver_fact** - Creates cleaned fact table with deduplication
5. **04_silver_dim** - Creates location dimension table
6. **05_gold_agg_1** - Creates daily area summary
7. **06_gold_agg_2** - Creates victim profile summary

## Verify Results

### In Databricks SQL Editor

```sql
-- Check bronze data
USE CATALOG public_crime;
USE SCHEMA bronze;
SELECT COUNT(*) FROM lapd_crime_raw_json;
SELECT * FROM lapd_crime_raw_json LIMIT 10;

-- Check silver data
USE SCHEMA silver;
SELECT COUNT(*) FROM crime_event_clean;
SELECT * FROM crime_event_clean LIMIT 10;

-- Check gold aggregations
USE SCHEMA gold;
SELECT * FROM crime_daily_area_summary ORDER BY occurrence_date DESC LIMIT 20;
SELECT * FROM crime_type_victim_summary ORDER BY month DESC LIMIT 20;
```

## Common Issues and Solutions

### Issue: "SQL entity could not be found"
**Solution:** Re-run the setup script to create queries and grant permissions:
```bash
./scripts/setup_databricks_job.sh lapd-crime <warehouse_id>
```

### Issue: "Permission denied" on S3
**Solution:** Check AWS credentials and bucket permissions:
```bash
aws s3 ls s3://public-crime-raw-dev-eu-west-1/
```

### Issue: "Dataset not found" in Socrata
**Solution:** Verify the dataset ID:
```bash
# Test the dataset URL
curl "https://data.lacity.org/resource/2nrs-mtv8.json?\$limit=1"
```

### Issue: Rate limiting from Socrata
**Solution:** Add app token:
```bash
export SOCRATA_APP_TOKEN="your_token"
python scripts/ingest_socrata_to_s3.py
```

## Project Structure

```
lapd-crime/
├── scripts/
│   ├── ingest_socrata_to_s3.py          # Socrata → S3 ingestion
│   ├── setup_databricks_job.sh          # Databricks job setup
│   ├── create_queries.sh                 # Create SQL queries
│   └── update_workflow_with_query_ids.sh # Update workflow JSON
├── databricks/
│   ├── notebooks/                        # SQL files
│   │   ├── 00_uc_bootstrap_with_grants.sql
│   │   ├── 01_external_location_with_grants.sql
│   │   └── ...
│   └── workflows/
│       └── lapd_crime_pipeline.json      # Job definition
├── permissions/                          # Permission templates
├── build/                                # Generated files (query IDs, etc.)
└── SETUP_INSTRUCTIONS.md                 # Detailed setup guide
```

## Next Steps

1. **Schedule Regular Ingestion**: Set up a cron job or AWS Lambda to run ingestion daily
2. **Schedule Databricks Job**: Configure the job to run on a schedule in Databricks UI
3. **Monitor and Alert**: Set up alerts for job failures
4. **Optimize**: Review query performance and optimize as needed

## Quick Reference Commands

```bash
# Ingest data
python scripts/ingest_socrata_to_s3.py

# Setup Databricks
./scripts/setup_databricks_job.sh lapd-crime <warehouse_id>

# Create/update job
databricks --profile lapd-crime jobs create --json @databricks/workflows/lapd_crime_pipeline.json

# Run job
databricks --profile lapd-crime jobs run-now <job_id>

# Check job status
databricks --profile lapd-crime jobs get <job_id>
```

## Getting Help

- **Socrata Ingestion**: See `scripts/README_SOCRATA_INGESTION.md`
- **Databricks Setup**: See `scripts/README_SETUP.md` or `SETUP_INSTRUCTIONS.md`
- **Troubleshooting**: Check the "Common Issues" section above

## Summary Checklist

- [ ] Python dependencies installed
- [ ] AWS credentials configured
- [ ] Databricks CLI configured
- [ ] S3 bucket accessible
- [ ] Data ingested from Socrata to S3
- [ ] Databricks job setup completed
- [ ] Job created/updated in Databricks
- [ ] Job run successfully
- [ ] Data verified in Databricks

---

**Ready to start?** Begin with Step 1 above!

