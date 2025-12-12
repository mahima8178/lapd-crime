# Socrata API to S3 Ingestion

This script fetches LAPD crime data from the Socrata API and uploads it to S3 for processing by the Databricks pipeline.

## Prerequisites

1. **Python 3.7+**
2. **AWS Credentials** configured (via `~/.aws/credentials` or environment variables)
3. **Socrata App Token** (optional but recommended for higher rate limits)
   - Get one at: https://dev.socrata.com/register

## Installation

```bash
pip install -r scripts/requirements_ingestion.txt
```

## Configuration

### Environment Variables

```bash
export SOCRATA_DOMAIN="data.lacity.org"
export SOCRATA_DATASET_ID="2nrs-mtv8"  # Update with actual LAPD dataset ID
export SOCRATA_APP_TOKEN="your_app_token_here"  # Optional
export S3_BUCKET="public-crime-raw-dev-eu-west-1"
export S3_PREFIX="bronze/lapd_crime/json"
export AWS_REGION="eu-west-1"
```

### Finding the Dataset ID

1. Go to https://data.lacity.org
2. Search for "LAPD Crime Data" or similar
3. Open the dataset
4. The dataset ID is in the URL: `data.lacity.org/resource/{DATASET_ID}.json`

## Usage

### Basic Usage

```bash
python scripts/ingest_socrata_to_s3.py
```

### With Custom Parameters

```bash
python scripts/ingest_socrata_to_s3.py \
  --dataset-id "2nrs-mtv8" \
  --app-token "your_token" \
  --bucket "public-crime-raw-dev-eu-west-1" \
  --prefix "bronze/lapd_crime/json" \
  --max-records 100000
```

### With Date Filtering

```bash
# Fetch only records from 2024
python scripts/ingest_socrata_to_s3.py \
  --where 'date_occ >= "2024-01-01"'
```

### Incremental Updates

For incremental updates, you can filter by date:

```bash
# Fetch records from last 7 days
python scripts/ingest_socrata_to_s3.py \
  --where 'date_occ >= "2024-12-01"'
```

## Output

The script will:
1. Fetch data from Socrata API in batches (50,000 records per batch)
2. Upload each batch as a JSON file to S3
3. Files are named: `lapd_crime_YYYYMMDD_HHMMSS_part0001.json`

Example S3 structure:
```
s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/
  ├── lapd_crime_20241201_120000_part0001.json
  ├── lapd_crime_20241201_120000_part0002.json
  └── ...
```

## Integration with Databricks Pipeline

After running the ingestion:

1. **Data is in S3**: Files are at `s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/`

2. **Run Databricks Job**: The bronze table will automatically pick up new files:
   ```bash
   databricks --profile lapd-crime jobs run-now <job_id>
   ```

3. **The bronze SQL** (`02_bronze.sql`) creates an external table that reads from this S3 location:
   ```sql
   CREATE TABLE IF NOT EXISTS lapd_crime_raw_json
   USING JSON
   LOCATION 's3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/';
   ```

## Scheduling

### Option 1: Cron Job (Local)

```bash
# Add to crontab (runs daily at 2 AM)
0 2 * * * cd /path/to/lapd-crime && python scripts/ingest_socrata_to_s3.py --where 'date_occ >= "'$(date -d yesterday +%Y-%m-%d)'"'
```

### Option 2: AWS Lambda + EventBridge

Create a Lambda function that runs this script on a schedule.

### Option 3: Databricks Job

You can also create a Databricks job that runs a Python notebook to fetch from Socrata and write to S3.

## Troubleshooting

### "Dataset not found" (404)
- Verify the dataset ID is correct
- Check the dataset URL in your browser
- Ensure the dataset is public

### "Rate limit exceeded"
- Add an app token: `--app-token "your_token"`
- Reduce batch size (modify `LIMIT` in script)
- Add delays between requests

### "AWS credentials not found"
- Configure AWS CLI: `aws configure`
- Or set environment variables:
  ```bash
  export AWS_ACCESS_KEY_ID="your_key"
  export AWS_SECRET_ACCESS_KEY="your_secret"
  export AWS_DEFAULT_REGION="eu-west-1"
  ```

### "Permission denied" on S3
- Ensure your AWS credentials have `s3:PutObject` permission on the bucket
- Check bucket policy allows your IAM user/role

## Notes

- The script handles pagination automatically
- Each batch is uploaded as a separate JSON file
- Files are timestamped for easy tracking
- The Databricks external table will automatically discover new files

## Next Steps

After ingestion:
1. Verify files in S3: `aws s3 ls s3://public-crime-raw-dev-eu-west-1/bronze/lapd_crime/json/`
2. Run the Databricks pipeline to process the data
3. Check the bronze table: `SELECT COUNT(*) FROM public_crime.bronze.lapd_crime_raw_json;`

