# DE Iceberg Lakehouse Project

## Overview
End-to-end Lakehouse pipeline using Medallion Architecture (Bronze → Silver → Gold-ready)

* Source: Oracle ATP (JDBC), SQL Server
* Processing: PySpark (Dataproc)
* Storage: Apache Iceberg tables on GCS
* Orchestration: Apache Airflow

## Pipeline
1. Create ephemeral Dataproc cluster
2. Run Spark jobs:
    * Bronze ingestion
    * Silver transformations
    * Gold aggregation
3. Delete cluster (cost optimization)

  ## Bronze Layer
  * Raw data ingestion from source systems
  * Minimal transformation
  * Preserves source schema

  ## Silver Layer
  * Clean, structured, analytics-ready data
  * Business logic applied
  * Deduplication and data quality handling

  ## Gold Layer
  * The Gold layer contains business-level aggregated datasets optimized for reporting, dashboards, and KPI analysis.

## Project Structure
* dags/ → Airflow DAGs
* spark_jobs/
    - Bronze jobs
    - Silver transformation jobs
* utils/ → shared utilities (secrets, configs)
* scripts/ → cluster init scripts

## Features
* Apache Iceberg for:
    - ACID transactions
    - Time travel
    - Schema evolution
* Ephemeral Dataproc clusters (cost-efficient)
* Modular Spark jobs
* Airflow orchestration
* Structured logging (JSON)

## Secrets Management
* Managed via GCP Secret Manager
* Accessed through utils/secrets.py
* Passed at runtime via --py-files

## Dependencies

This pipeline depends on an external Python package:

- `utils.zip`
  - Must be uploaded to GCS
  - Contains shared utilities (e.g., secrets handling)