# OpenDataHub SQL (TER — M2 SID 2025/26)

OpenDataHub SQL is a data engineering platform built for the M2 SID TER project (2025/26).

Goal: ingest heterogeneous French open data from **data.gouv.fr**, store **raw files** on **AWS S3**, convert them into a unified **Parquet** format, and make them queryable in a **warehouse** (DuckDB locally, with an optional Snowflake load). The pipeline is orchestrated with **Apache Airflow** and includes **dbt** model generation (Bronze/Silver). ALl of that is monitored by DashBoard created on Streamlit

---

## Architecture Overview

**Pipeline (main path):**
1. **Ingestion** (data.gouv.fr API → download to temp → upload to S3 RAW)
2. **Conversion** (S3 RAW → convert by file type → upload Parquet to S3)
3. **Warehouse Load (Snowflake)** (S3 Parquet → create/replace tables in Snowflake)
4. **dbt Model Generation** (auto-generate `source.yml` + Bronze/Silver SQL models from warehouse tables)


---

## Project Structure

```bash
├── airflow/                              # Airflow DAGs
│   ├── datagouv_s3_dag.py                # data.gouv.fr → temp → S3 RAW
│   ├── conversion_to_parquet_dag.py      # S3 RAW → S3 Parquet (by format/category)
│   ├── conversion_parquet_to_table_dag.py    # S3 Parquet → Snowflake tables
│   └── dbt_dag.py                        # Generate dbt sources + Bronze/Silver models
│
├── ingestion/
│   ├── ingestion_to_S3/                  # data.gouv.fr client + downloader + S3 uploader
│   │   ├── datagouv_client.py
│   │   ├── downloader.py
│   │   └── s3_uploader.py
│   ├── s3/                               # S3 helpers
│   │   ├── client.py
│   │   ├── io.py
│   │   └── get_parquets_files.py
│   └── ingestion_to_warehouse/           # Warehouse ingestion code  Snowflake
│       ├── ingestion.py
│       └── config.yml
│
├── transformation/
│   ├── transformat_files_to_parquet/
│   │   ├── convert_to_parquet/           # Converters by category/extension
│   │   └── parquet/                      # Parquet writing utilities
│   ├── transforme_with_duckdb/           # DuckDB-based conversions (optional and made on local test only) 
│   └── transforme_with_dbt/dbt/          # dbt project (models/macros/profiles)
│
├── utils/                                # Config + formats dictionary + naming helpers
│   ├── config.py
│   ├── dictionnaire.py
│   └── get_table_name.py
│
├── tests/                                # Unit tests + conversion tests
│   ├── testsUniformisation/
│   ├── test_dbt/
│   └── test_snowflake/                   # Snowflake tests (mocked if not installed)
│
├── monitoring/                           # Streamlit monitoring dashboard
│   └── dashboard_streamlit.py
│
└── warehouse/
    └── warehouse.duckdb                  # (optionel) Local DuckDB file (generated/updated)

```
---

## Data Lake Layout on S3

The pipeline uses two prefixes:
- `S3_INPUT_PREFIX` for raw files (it was optional idea because every dataset is dedicated to their own folder)
- `S3_OUTPUT_PREFIX` for parquet files

Recommended layout:

```bash
s3://<bucket>/
├── tabular/
├── geospatial_vector/
├── geospatial_raster/
├── databases/
├── archives/
├── documents/
└── others/

s3://<bucket>/<S3_OUTPUT_PREFIX>/

```

Notes:

* Each dataset slug may contain multiple resources, so a single ingestion run can upload multiple files.
* Some resources can return 404 (external portals). The ingestion code should ideally skip failing URLs and continue.

---

## Requirements

* Docker + Docker Compose
* AWS credentials with access to your S3 bucket (read/write)
* Snowflake credentials if using the Snowflake loading DAG
* Python locally for running unit tests (optional)

---

## Environment Configuration (`.env`)

Create a `.env` file at the project root.

### AWS / S3

```bash
# S3 prefixes
S3_INPUT_PREFIX=raw_files/
S3_OUTPUT_PREFIX=parquets_files/
S3_BUCKET=your-bucket-name

# AWS region + creds
AWS_REGION=eu-north-1
AWS_DEFAULT_REGION=eu-north-1
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_SESSION_TOKEN=                       # optional

# Airflow S3Hook connection name (if used)
AWS_CONN_ID=aws_default

# data.gouv.fr
DATA_GOUV_API_ROOT=https://www.data.gouv.fr/api/1
DATASET_SLUG=catalogue-des-donnees-de-data-gouv-fr
```

### Airflow (UI credentials)

```bash
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin123!
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

### Optional: Snowflake

```bash
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=...

# External stage + file format configured in Snowflake
SNOWFLAKE_STAGE=MY_S3_STAGE
SNOWFLAKE_PARQUET_FORMAT=MY_PARQUET_FORMAT
```

---

## Running the Platform (Docker)

### 1) Build & start

```bash
docker compose build --no-cache
docker compose up -d
```

### 2) Check status

```bash
docker compose ps
```

### 3) Open Airflow UI

* URL: [http://localhost:8080](http://localhost:8080)
* Login: `admin / admin123!` (or your `.env` values)

### 4) Stop everything

```bash
docker compose down -v
```

---

## Running the DAGs

### A) Ingestion (data.gouv.fr → S3 RAW)

DAG: `ingestion_dag`

What it does:

* Fetch the **N last updated datasets** (slugs)
* For each slug: fetch metadata, pick resources matching supported formats
* Download to a temp folder (isolated per run)
* Upload all downloaded files to S3 RAW layout
* Trigger conversion DAG

### B) Conversion to Parquet (S3 RAW → S3 Parquet)

DAG: `conversion_to_parquet_dag`

What it does:

* Scan S3 RAW folders
* Skip files already processed using an Airflow Variable (`s3_processed_files`)
* Route files by category/extension
* Convert each file to Parquet using the corresponding converter
* Mark processed keys in the Airflow Variable

### C) Snowflake Warehouse Load (S3 Parquet → DuckDB)

DAG: `ingestion_warehouse_orchestrated`

What it does:

* Read `ingestion/ingestion_to_warehouse/config.yml`
* List Parquet files under `parquets_files/`
* Create/replace  tables using file names as table names
* Write into `warehouse/warehouse.duckdb`

### D) dbt Model Generation (from Snowflake tables)

DAG: `transformation_dbt_orchestrated`

What it does:

* Connect to Snowfkaj
* Extract table names
* Generate `models/source/source.yml`
* Generate Bronze SQL models (`models/bronze/`)
* Generate Silver SQL models (`models/silver/`)

### Optional: Snowflake Load (S3 Parquet → Snowflake tables)

DAG: `conversion_parquet_to_table_dag`

What it does:

* List Parquet files on S3
* For each file: infer schema via stage, create/replace table, copy into Snowflake

---

## Logs & Debugging (CLI)

### View container logs

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

### Read an Airflow task log file from inside the scheduler container

```bash
docker compose exec airflow-scheduler bash -lc \
"tail -n 200 '/opt/airflow/logs/dag_id=<DAG_ID>/run_id=<RUN_ID>/task_id=<TASK_ID>/attempt=1.log'"
```

### List DAG import errors

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

## Monitoring Dashboard (Streamlit)

A Streamlit dashboard is provided to monitor:

* S3 RAW vs Parquet volumes (count/size/latest object)
* Recent Airflow DAG runs (states, durations)
* Simple pipeline health warnings

If you enable the `streamlit` service in `docker-compose.yml` (port `8501`), open:

* [http://localhost:8501](http://localhost:8501)

---

## Testing

### Run all tests

```bash
pytest -q
```

### Run a single conversion test (example)

```bash
pytest -q "tests/testsUniformisation/test_tabular_to_parquet.py::test_convert_csv_to_parquet" -s
```

### Snowflake tests without installing the connector (recommended)

If you don’t want to install `snowflake-connector-python` locally (Windows may require MSVC build tools),
mock it only for Snowflake tests using:

* `tests/test_snowflake/conftest.py` (note: must be named **conftest.py**)

Then run:

```bash
pytest -q tests/test_snowflake
```

---

## Common Issues

### 1) PermissionError writing temp files in Airflow container

Use `/tmp/...` paths inside containers (writable for the airflow user), or mount a dedicated volume.

### 2) data.gouv.fr resources returning 404

Some datasets reference external portals that can break links.
Best practice: log the failure and continue downloading other resources.

### 3) Dependency conflicts when building the Airflow image

Airflow uses strict constraints. Avoid installing `dbt-snowflake` inside the Airflow image unless you pin versions
compatible with Airflow constraints. Prefer running dbt in a separate container/service when needed.

---

## License / Academic Context

This repository is an final year project developed as part of the TER (M2 SID, 2025/26).

