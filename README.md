# LAPD Crime Data Pipeline

A complete data pipeline for ingesting, processing, and analyzing LAPD crime data from Socrata API using Databricks and Unity Catalog.

## Quick Start

### Automated (Recommended)
```bash
# Run everything from scratch in one command
./scripts/run_from_scratch.sh
```

### Manual Steps
1. **Read the Getting Started Guide**: [GETTING_STARTED.md](GETTING_STARTED.md)
2. **Ingest Data**: `python scripts/ingest_socrata_to_s3.py`
3. **Setup Databricks**: `./scripts/setup_databricks_job.sh lapd-crime <warehouse_id>`
4. **Run Pipeline**: `databricks --profile lapd-crime jobs run-now <job_id>`

## Project Overview

This project implements a medallion architecture (Bronze → Silver → Gold) for LAPD crime data:

- **Bronze**: Raw JSON data from Socrata API stored in S3
- **Silver**: Cleaned and deduplicated fact and dimension tables
- **Gold**: Aggregated summaries for analytics

## Architecture

```
Socrata API → S3 (Bronze) → Databricks SQL Warehouse → Unity Catalog
                                                          ├── Bronze Schema
                                                          ├── Silver Schema
                                                          └── Gold Schema
```

## Key Components

- **Ingestion**: Python script to fetch from Socrata API and upload to S3
- **Processing**: SQL-based ETL pipeline in Databricks
- **Storage**: Unity Catalog with external tables on S3
- **Orchestration**: Databricks Jobs with SQL tasks

## Documentation

- [GETTING_STARTED.md](GETTING_STARTED.md) - Complete setup guide
- [SETUP_INSTRUCTIONS.md](SETUP_INSTRUCTIONS.md) - Detailed Databricks setup
- [scripts/README_SOCRATA_INGESTION.md](scripts/README_SOCRATA_INGESTION.md) - Socrata ingestion guide
- [scripts/README_SETUP.md](scripts/README_SETUP.md) - Databricks job setup guide

## Requirements

- Python 3.7+
- Databricks CLI
- AWS CLI with credentials
- jq (for scripts)

## License

See [LICENSE](LICENSE) file.