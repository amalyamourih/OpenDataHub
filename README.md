# OpenDataHub SQL (TER M2 SID 2025/26)

OpenDataHub SQL is a data engineering platform built for the M2 SID TER.  
Goal: ingest heterogeneous French open-data (data.gouv.fr), store raw files, convert them to Parquet, load them into a warehouse, and make them queryable via SQL with quality checks.

Core stack:
- Apache Airflow (Dockerized) for orchestration
- AWS S3 as data lake storage
- Parquet as unified analytical format
- DuckDB as local warehouse (and optional Snowflake target)
- dbt for SQL transformations (Bronze/Silver) and tests

---

## Project Structure

```bash
├── airflow/ # Airflow DAGs

│ ├── datagouv_s3_dag.py # Ingestion (data.gouv.fr → tmp → S3)
│ ├── conversion_to_parquet_dag.py # Conversion (S3 raw → S3 parquet)
│ ├── conversion_parquet_to_table_dag.py
│ └── dbt_dag.py # dbt run/test (warehouse transformations)

├── ingestion/
│ ├── ingestion_to_S3/ # API client + download + upload to S3
│ ├── s3/ # S3 utilities (client, io, list keys)
│ └── ingestion_to_warehouse/ # Parquet (S3) → DuckDB warehouse

├── transformation/
│ ├── transformat_files_to_parquet/
│ │ ├── convert_to_parquet/ # converters by category/extension
│ │ └── parquet/ # parquet writer utilities
│ ├── transforme_with_duckdb/
│ └── transforme_with_dbt/dbt/ # dbt project (models/macros/profiles)

├── utils/ # config, formats dictionary, naming helpers

├── tests/ # unit tests + conversion tests

└── warehouse/warehouse.duckdb # local DuckDB warehouse file
```


---

## Data Lake Layout on S3

The pipeline uses two prefixes:
- `S3_INPUT_PREFIX` for raw files
- `S3_OUTPUT_PREFIX` for parquet files

Recommended layout:

s3://<bucket>/<S3_INPUT_PREFIX>/
├── tabular/
├── geospatial_vector/
├── geospatial_raster/
├── databases/
├── archives/
├── documents/
└── others/

s3://<bucket>/<S3_OUTPUT_PREFIX>/
├── tabular/
├── geospatial_vector/
├── geospatial_raster/
├── databases/
├── archives/
├── documents/
└── others/


---

## Requirements

- Docker + Docker Compose
- AWS credentials with access to your S3 bucket
- (Optional) Snowflake credentials if running dbt on Snowflake

---

## Environment Configuration (`.env`)

Create a `.env` file at project root.

### AWS / S3
```bash
S3_INPUT_PREFIX=raw_files/
S3_OUTPUT_PREFIX=parquets_files/
S3_BUCKET=your-bucket-name
AWS_REGION=eu-north-1
AWS_DEFAULT_REGION=eu-north-1
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_SESSION_TOKEN=                # optional
AWS_CONN_ID=aws_default           # used by Airflow S3Hook (if configured)



# Deploiement de projet (Docker)

Ou 

# Tester les projet 

pour les tests d'uniformisation mettre la commande :

 pytest "tests\testsUniformisation\test_tabular_to_parquet.py::test_convert_csv_to_parquet" -s

# Idée pour S3 :

suivre cette structure pour les fichiers qui vont venir sur S3 :
    s3://bucket/input_files/
    ├── tabular/
    ├── geospatial_vector/
    ├── geospatial_raster/
    ├── databases/
    ├── archives/
    └── others/

après conversion les mettres sur cette fichier  s3://bucket/parquets_files/

## Deploiement d'environnement via Docker puis Airflow
- Mettre en place cet .env 

```bash
S3_INPUT_PREFIX=
S3_OUTPUT_PREFIX=parquets_files
S3_BUCKET=amzn-s3-opendatahub
AWS_REGION=eu-north-1
AWS_DEFAULT_REGION=eu-north-1
AWS_ACCESS_KEY_ID=ta-clé
AWS_SECRET_ACCESS_KEY=ton-secret
```
- Ouvrir l'application docker

- Lancer ces commande 

```bash
docker compose build --no-cache
docker compose up -d
```
pour arreter de build 

```bash
docker compose down -v
```

verification de tout 

```bash
docker compose ps
```

pour Accéder à l'interface :

URL : http://localhost:8080
Login : admin / admin123!
