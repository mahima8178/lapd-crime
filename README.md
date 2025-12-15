# LAPD Crime Data Pipeline  
**Critical Technical Analysis, Trade-offs, and Future Improvements**

**Databricks SQL + Unity Catalog + AWS S3** batch pipeline for LAPD crime data sourced from the Socrata Open Data API.  
Explicitly for **2025 Databricks constraints**: SQL Warehouse–only compute, UC-governed storage, cross-account AWS IAM, and Delta Lake.

---

## 1. What This Project Actually Solves

This project demonstrates how to:

- Ingest **public JSON data** from Socrata into **AWS S3**
- Govern that data using **Unity Catalog external locations**
- Process everything using **Databricks SQL only**  
  (no Spark clusters, no Python notebooks running inside Databricks)
- Implement a **Bronze → Silver → Gold** medallion model that is:
  - idempotent
  - schema-controlled
  - compatible with **Serverless SQL Warehouses**
- Orchestrate the full pipeline using **Databricks Jobs with SQL tasks**

This is **not** a demo notebook project.  
It is an **enterprise-style, SQL-first pipeline** aligned with modern Databricks best practices.

---

## 2. High-Level Architecture
Socrata API
|
| (Python, outside Databricks)
v
AWS S3 (raw JSON)
|
| Unity Catalog External Location
v
Databricks SQL Warehouse
|
| COPY INTO (Bronze Delta)
v
Silver Delta (Typed + Deduped)
|
v
Gold Delta (Aggregations / BI-ready)




### Key architectural constraints that shaped this design:

| Constraint | Impact |
|-----------|-------|
| SQL Warehouse only | No instance profiles, no DBFS mounts |
| Unity Catalog enforced | All S3 access must go via External Locations |
| Cross-account AWS | Requires IAM role + External ID |
| JSON source | Requires schema-tolerant Bronze layer |
| Public dataset | Incremental logic must be resilient to late updates |

---

## 3. Why Unity Catalog (UC) Instead of Hive Metastore

### Hive Metastore (rejected)
- Workspace-scoped
- Cluster-centric
- Poor S3 governance
- Breaks on SQL-only compute
- No external-location abstraction

### Unity Catalog (chosen)
- Centralized governance
- Works with **serverless SQL**
- Explicit S3 trust model:


- Required for:
  - `COPY INTO`
  - Cross-account IAM
  - Least-privilege access

**Trade-off:**  
UC adds upfront complexity (roles, grants, locations).  
Once configured, failures are deterministic and debuggable.

---

## 4. Medallion Layers – Design Review

### Bronze Layer (Raw Delta)

**What it does**
- Stores raw Socrata JSON fields as strings
- No transformations
- No partitions
- Schema drift allowed

**Why**
- Socrata occasionally changes or sparsifies fields
- Early typing causes brittle failures
- `COPY INTO` handles file-level idempotency

**Trade-offs**
- Larger storage footprint
- No predicate pushdown
- Requires downstream casting

Correct choice for **public APIs**.

---

### Silver Layer (Fact + Dimension)

#### Silver Fact: `crime_event_clean`

**What it does**
- Type casting
- Timestamp normalization
- Deduplication by `dr_no`
- Deterministic latest-record selection
- Stable hash generation

**Why**
- Socrata updates records post-publication
- Replay-safe batch logic required
- CDC-compatible design

**Trade-offs**
- Recomputes latest state per run
- Higher compute than naive append

Acceptable for batch public datasets.

#### Silver Dimension: `dim_location`

**What it does**
- De-normalizes location attributes
- Generates surrogate hash key
- One row per unique location

**Why**
- Simplifies BI joins
- Reduces repeated string columns in gold

---

### Gold Layer (Analytics)

#### Gold 1: Daily Area Summary
- Grain: day × area × crime type
- BI-friendly fact table
- Stable dashboard input

#### Gold 2: Victim Profile Summary
- Grain: month × crime × demographic
- Optimized for slicing, not drill-down

**Trade-offs**
- Pre-aggregated (less flexible than Silver)
- Requires recompute on logic change

---

## 5. Orchestration Design (SQL Jobs Only)

### Why SQL Jobs (not notebooks or Airflow)

- Databricks Free / Trial = SQL Warehouse only
- No long-running clusters
- SQL tasks are:
  - version-controlled
  - deterministic
  - auditable

**Job Structure**
00_uc_bootstrap
01_external_location
02_bronze
03_silver_fact
04_silver_dim
05_gold_agg_1
06_gold_agg_2




Each task:
- Runs on SQL Warehouse
- Uses registered SQL queries
- Has explicit dependencies

---

## 6. Known Limitations (Honest Assessment)

### 1. Incremental Logic Is Coarse-Grained
- File-level incremental (`COPY INTO`)
- Silver dedup recomputes latest per `dr_no`

**Reason**
- Socrata does not guarantee strict append semantics
- Safer than timestamp-only logic

### 2. No Streaming / Near-Real-Time
- Batch-only by design

**Reason**
- Public dataset
- SQL Warehouse constraints
- Simplicity prioritized over latency

### 3. External Python Dependency
- Socrata ingestion runs outside Databricks

**Reason**
- SQL Warehouse cannot call REST APIs
- Correct separation of concerns

---

## 7. Security Review

### Implemented Correctly
- No AWS keys stored in Databricks
- IAM role assumption with External ID
- UC-governed S3 access
- Least-privilege bucket access

### Intentionally Avoided
- DBFS mounts
- Instance profiles
- Hard-coded credentials
- Workspace-scoped storage access

---

## 8. Performance Characteristics

| Layer | Cost Driver | Notes |
|-----|------------|------|
| Bronze | Storage | JSON + Delta overhead |
| Silver | Compute | Dedup + casting |
| Gold | Compute | Aggregations |

**Optimizations applied**
- Narrow schemas in Silver
- Hash keys for joins
- SQL-only execution (no cluster spin-up)

---

## 9. Improvements to Make (Next Iteration)


Airflow /Terrform Integration
### Short-term
- `OPTIMIZE` Silver and Gold tables
- Z-ORDER on `occurrence_date`, `area`
- Row-count and freshness checks

### Medium-term
- Use Socrata `:updated_at` watermark
- Add pipeline audit table (run_id, counts)
- Add data quality checks

### Long-term / Enterprise
- CDC-driven Gold updates
- Multi-workspace UC sharing
- BI semantic layer (Lakeview / dbt-style)
- Schema evolution alerts

---

## 10. Who This Architecture Is For

**Good fit**
- SQL-heavy data engineers
- Governance-first organizations
- Serverless compute environments
- Public or semi-structured batch data

**Not ideal for**
- Low-latency streaming
- Python-heavy transformations
- ML feature engineering pipelines

---

## 11. Final Assessment

This project intentionally favors:

- **Governance over convenience**
- **SQL determinism over Spark flexibility**

  Beginner friendly how to0 **Databricks is designed to be used in 2025** for governed, serverless, AWS-backed analytics.





