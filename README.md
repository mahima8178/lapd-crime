```markdown
# LAPD Crime Data Pipeline  
**Critical Technical Analysis, Trade-offs, and Future Improvements**

A production-grade **Databricks SQL + Unity Catalog + AWS S3** batch pipeline for LAPD crime data sourced from the Socrata Open Data API.  
Designed explicitly for **2025 Databricks constraints**: SQL Warehouse–only compute, UC-governed storage, cross-account AWS IAM, and Delta Lake.

---

## 1. What This Project Actually Solves (Clearly)

This project demonstrates how to:

- Ingest **public JSON data** from Socrata into **AWS S3**
- Govern that data using **Unity Catalog external locations**
- Process everything using **Databricks SQL only** (no Spark clusters, no notebooks with Python execution)
- Implement a **Bronze → Silver → Gold** medallion model that:
  - is idempotent
  - is schema-controlled
  - works on **Serverless SQL Warehouse**
- Orchestrate end-to-end execution using **Databricks Jobs (SQL tasks)**

This is **not** a demo notebook project.  
It is an **enterprise-style SQL-first pipeline**.

---

## 2. High-Level Architecture (Why It Looks This Way)

```

Socrata API
|
| (Python, outside Databricks)
v
AWS S3 (raw JSON)
|
|  Unity Catalog External Location
v
Databricks SQL Warehouse
|
|  COPY INTO (Bronze Delta)
v
Silver Delta (Typed + Deduped)
|
v
Gold Delta (Aggregations / BI-ready)

```

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
```

Storage Credential → External Location → Table

```
- Required for:
- `COPY INTO`
- Cross-account IAM
- Least-privilege access

**Trade-off:**  
UC adds upfront complexity (roles, grants, locations).  
But once configured, failures are deterministic and debuggable.

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

**Correct choice for public APIs**

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
- Need replay-safe logic
- Enables CDC if needed later

**Trade-offs**
- Recomputes latest state per run (not row-level CDC)
- More CPU than naive append

**Acceptable for batch public data**

#### Silver Dimension: `dim_location`

**What it does**
- De-normalizes location attributes
- Generates surrogate hash key
- One row per unique location

**Why**
- Simplifies BI joins
- Avoids repeated text fields in gold

---

### Gold Layer (Analytics)

#### Gold 1: Daily Area Summary
- Grain: day × area × crime type
- BI-friendly fact table
- Stable for dashboards

#### Gold 2: Victim Profile Summary
- Grain: month × crime × demographic
- Designed for slicing, not drill-down

**Trade-offs**
- Pre-aggregated (less flexible than raw silver)
- Requires recompute if logic changes

---

## 5. Orchestration Design (SQL Jobs Only)

### Why SQL Jobs, not notebooks or Airflow

- Databricks Free / Trial = SQL Warehouse only
- No long-running clusters
- SQL tasks are:
- version-controlled
- deterministic
- easier to audit

**Job Structure**
```

00_uc_bootstrap
01_external_location
02_bronze
03_silver_fact
04_silver_dim
05_gold_agg_1
06_gold_agg_2

```

Each task:
- Runs on SQL Warehouse
- Uses registered SQL queries
- Has explicit dependencies

---

## 6. Known Limitations (Honest Assessment)

### 1. Incremental Logic is Coarse-Grained
- File-level incremental (`COPY INTO`)
- Silver dedup recomputes latest per `dr_no`

**Why**
- Socrata does not guarantee strict append semantics
- Safer than relying on timestamps alone

### 2. No Streaming / Near-Real-Time
- Batch-only by design

**Why**
- Public dataset
- SQL Warehouse constraints
- Simplicity > latency

### 3. External Python Dependency
- Socrata ingestion runs outside Databricks

**Why**
- SQL Warehouse cannot call REST APIs
- Correct architectural separation

---

## 7. Security Review

### What is done correctly
- No AWS keys in Databricks
- IAM role assumption with External ID
- UC-governed S3 access
- No wildcard S3 access beyond required prefixes

### What is intentionally avoided
- DBFS mounts
- Instance profiles
- Hard-coded credentials
- Workspace-scoped permissions

---

## 8. Performance Characteristics

| Layer | Cost Driver | Notes |
|-----|------------|------|
| Bronze | Storage | JSON + Delta overhead |
| Silver | Compute | Dedup + casts |
| Gold | Compute | Group-bys |

**Optimizations already applied**
- Narrow schemas in Silver
- Hash keys for joins
- SQL-only execution (no cluster spin-up)

---

## 9. Improvements to Make (Next Iteration)

### Short-term (easy wins)
- Add `OPTIMIZE` on Silver/Gold
- Add Z-ORDER on `occurrence_date`, `area`
- Add row counts + freshness checks

### Medium-term
- Use Socrata `:updated_at` watermark for smarter incremental Silver
- Introduce audit table:
  - run_id
  - row counts
  - timestamps
- Add data quality rules (NULL %, domain checks)

### Long-term / Enterprise
- CDC-driven Gold updates
- Multi-workspace UC sharing
- BI semantic layer (Databricks Lakeview / dbt-style models)
- Automated schema evolution alerts

---

## 10. Who This Architecture Is For

**Good fit**
- SQL-heavy data engineers
- Governance-first environments
- Serverless / managed compute
- Public or semi-structured batch data

**Not ideal for**
- Low-latency streaming
- Python-centric transformations
- ML feature pipelines

---

## 11. Final Assessment

This project intentionally favors:

- **Correctness over cleverness**
- **Governance over convenience**
- **SQL determinism over Spark flexibility**

It reflects how **Databricks is meant to be used in 2025** for governed, serverless, AWS-backed analytics.

The complexity you experienced is **not a personal failure**—it is the real cost of doing things *the right way* on modern Databricks.

---
```
