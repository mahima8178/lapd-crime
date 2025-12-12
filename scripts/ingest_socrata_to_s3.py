#!/usr/bin/env python3
"""
Socrata API to S3 Ingestion Script
Fetches LAPD crime data from Socrata API and writes to S3
"""

import os
import json
import boto3
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List
import argparse
from pathlib import Path
import time
import sys

# Socrata API endpoint for LAPD Crime Data
# Common endpoint: data.lacity.org
SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN", "data.lacity.org")
# LAPD Crime Data dataset identifier (update with actual dataset ID)
DATASET_ID = os.getenv("SOCRATA_DATASET_ID", "2nrs-mtv8")  # Example - update with actual ID
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "adI4e1VB9UbrBu26tygqa6Hzh")  # Optional but recommended

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "public-crime-raw-dev-eu-west-1")
S3_PREFIX = os.getenv("S3_PREFIX", "bronze/lapd_crime/json")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# Pagination
LIMIT = 50000  # Socrata API limit per request
MAX_RECORDS = None  # Set to None for all records, or a number to limit


def get_socrata_data(
    domain: str,
    dataset_id: str,
    app_token: Optional[str] = None,
    limit: int = LIMIT,
    offset: int = 0,
    where_clause: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch data from Socrata API
    
    Args:
        domain: Socrata domain (e.g., 'data.lacity.org')
        dataset_id: Dataset identifier
        app_token: Optional app token for higher rate limits
        limit: Number of records per request
        offset: Offset for pagination
        where_clause: Optional WHERE clause for filtering
    
    Returns:
        Response JSON as dictionary
    """
    base_url = f"https://{domain}/resource/{dataset_id}.json"
    
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": ":id"  # Order by ID for consistent pagination
    }
    
    if where_clause:
        params["$where"] = where_clause
    
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    
    print(f"Fetching data from Socrata: offset={offset}, limit={limit}")
    response = requests.get(base_url, params=params, headers=headers)
    response.raise_for_status()
    
    return response.json()


def upload_to_s3(
    data: list,
    bucket: str,
    prefix: str,
    filename: str,
    region: str = "eu-west-1"
) -> str:
    """
    Upload JSON data to S3
    
    Args:
        data: List of records to upload
        bucket: S3 bucket name
        prefix: S3 prefix (path)
        filename: Filename for the JSON file
        region: AWS region
    
    Returns:
        S3 key (full path)
    """
    s3_client = boto3.client('s3', region_name=region)
    
    # Create S3 key
    s3_key = f"{prefix}/{filename}"
    
    # Convert to JSON string
    json_data = json.dumps(data, ensure_ascii=False, indent=2)
    
    # Upload to S3
    print(f"Uploading {len(data)} records to s3://{bucket}/{s3_key}")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json'
    )
    
    return s3_key


def ingest_socrata_to_s3(
    domain: str = SOCRATA_DOMAIN,
    dataset_id: str = DATASET_ID,
    app_token: Optional[str] = SOCRATA_APP_TOKEN,
    bucket: str = S3_BUCKET,
    prefix: str = S3_PREFIX,
    region: str = AWS_REGION,
    max_records: Optional[int] = MAX_RECORDS,
    where_clause: Optional[str] = None
):
    """
    Main ingestion function: Fetch from Socrata and upload to S3
    
    Args:
        domain: Socrata domain
        dataset_id: Dataset identifier
        app_token: Optional app token
        bucket: S3 bucket
        prefix: S3 prefix
        region: AWS region
        max_records: Maximum records to fetch (None for all)
        where_clause: Optional WHERE clause for filtering
    """
    print("=" * 60)
    print("Socrata to S3 Ingestion")
    print("=" * 60)
    print(f"Domain: {domain}")
    print(f"Dataset ID: {dataset_id}")
    print(f"S3 Bucket: {bucket}")
    print(f"S3 Prefix: {prefix}")
    print(f"Max Records: {max_records or 'All'}")
    print("=" * 60)
    print()
    
    offset = 0
    total_records = 0
    file_counter = 1
    
    while True:
        try:
            # Fetch data from Socrata
            data = get_socrata_data(
                domain=domain,
                dataset_id=dataset_id,
                app_token=app_token,
                limit=LIMIT,
                offset=offset,
                where_clause=where_clause
            )
            
            if not data:
                print("No more data to fetch")
                break
            
            num_records = len(data)
            if num_records == 0:
                print("No records returned, stopping")
                break
            
            # Create filename with timestamp
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"lapd_crime_{timestamp}_part{file_counter:04d}.json"
            
            # Upload to S3
            s3_key = upload_to_s3(
                data=data,
                bucket=bucket,
                prefix=prefix,
                filename=filename,
                region=region
            )
            
            total_records += num_records
            print(f"✓ Uploaded {num_records} records (Total: {total_records})")
            print(f"  S3 Key: s3://{bucket}/{s3_key}\n")
            
            # Check if we've reached max records
            if max_records and total_records >= max_records:
                print(f"Reached max records limit: {max_records}")
                break
            
            # Check if we got fewer records than limit (last page)
            if num_records < LIMIT:
                print("Reached end of dataset")
                break
            
            # Prepare for next iteration
            offset += LIMIT
            file_counter += 1
            
            # Small delay to avoid rate limiting
            import time
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            if e.response.status_code == 404:
                print(f"Dataset not found. Check dataset_id: {dataset_id}")
            break
        except Exception as e:
            print(f"Error: {e}")
            raise
    
    print("=" * 60)
    print(f"Ingestion complete! Total records: {total_records}")
    print(f"Files uploaded to: s3://{bucket}/{prefix}/")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest LAPD crime data from Socrata API to S3"
    )
    parser.add_argument(
        "--domain",
        default=SOCRATA_DOMAIN,
        help=f"Socrata domain (default: {SOCRATA_DOMAIN})"
    )
    parser.add_argument(
        "--dataset-id",
        default=DATASET_ID,
        help=f"Dataset ID (default: {DATASET_ID})"
    )
    parser.add_argument(
        "--app-token",
        default=SOCRATA_APP_TOKEN,
        help="Socrata app token (optional, for higher rate limits)"
    )
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"S3 bucket (default: {S3_BUCKET})"
    )
    parser.add_argument(
        "--prefix",
        default=S3_PREFIX,
        help=f"S3 prefix (default: {S3_PREFIX})"
    )
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help=f"AWS region (default: {AWS_REGION})"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum records to fetch (default: all)"
    )
    parser.add_argument(
        "--where",
        default=None,
        help="Socrata WHERE clause for filtering (e.g., 'date_occ >= \"2024-01-01\"')"
    )
    
    args = parser.parse_args()
    
    ingest_socrata_to_s3(
        domain=args.domain,
        dataset_id=args.dataset_id,
        app_token=args.app_token if args.app_token else None,
        bucket=args.bucket,
        prefix=args.prefix,
        region=args.region,
        max_records=args.max_records,
        where_clause=args.where
    )


if __name__ == "__main__":
    main()

