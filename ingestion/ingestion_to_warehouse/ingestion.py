import duckdb
import yaml
import os

from ingestion.s3.get_parquets_files import list_s3_keys
from utils.get_table_name import get_table_name_from_key
from utils.config import S3_BUCKET, AWS_REGION


def load_config(config_path="..config.yml"):
    current_dir = os.path.dirname(__file__)  
    config_path = os.path.join(current_dir, "config.yml")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)



def build_db_path():
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    db_path = os.path.join(project_root, "warehouse", "warehouse.duckdb")
    return db_path


def prepare_database_file(db_path):
    if os.path.exists(db_path) and os.path.getsize(db_path) == 0:
        os.remove(db_path)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)


def create_duckdb_connection(db_path):
    conx = duckdb.connect(db_path)

    conx.execute("INSTALL httpfs;")
    conx.execute("LOAD httpfs;")
    conx.execute(f"SET s3_region='{AWS_REGION}';")

    return conx


def ingest_warehouse(conx, bucket, prefix, formats):
    keys = list_s3_keys(bucket, prefix)
    print(f"{len(keys)} fichiers trouvés dans S3")

    for key in keys:
        if not key.endswith(formats):
            continue

        table_name = get_table_name_from_key(key)
        s3_path = f"s3://{bucket}/{key}"

        print(f"Ingestion {s3_path} --> {table_name}")

        if key.endswith(".parquet"):
            conx.execute(f"""
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM read_parquet('{s3_path}');
            """)

