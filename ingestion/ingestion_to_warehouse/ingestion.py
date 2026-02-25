import os
from typing import Iterable, List, Optional

import snowflake.connector

from ingestion.s3.get_parquets_files import list_s3_keys
from utils.get_table_name import get_table_name_from_key
from utils.config import S3_BUCKET


def create_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )


def load_parquets_to_snowflake(
    conx,
    prefix: str = "parquets_files/",
    stage_name: str = "MY_S3_STAGE",
    file_format: str = "MY_PARQUET_FORMAT",
    formats: Iterable[str] = (".parquet",),
) -> List[str]:
    cursor = conx.cursor()
    created_tables: List[str] = []

    try:
        keys = list_s3_keys(S3_BUCKET, prefix=prefix)
        suffixes = tuple(s.lower() for s in formats)

        parquet_keys = [k for k in keys if k.lower().endswith(suffixes)]
        print(f"{len(parquet_keys)} fichiers trouvés sous s3://{S3_BUCKET}/{prefix}")

        for key in parquet_keys:
            table_name = get_table_name_from_key(key).upper()
            stage_path = f"@{stage_name}/{key}"

            try:
                print(f"{stage_path} -> {table_name}")

                cursor.execute(
                    f"""
                    CREATE OR REPLACE TABLE "{table_name}"
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '{stage_path}',
                                FILE_FORMAT => '{file_format}'
                            )
                        )
                    );
                    """
                )

                cursor.execute(
                    f"""
                    COPY INTO "{table_name}"
                    FROM '{stage_path}'
                    FILE_FORMAT = (FORMAT_NAME = '{file_format}')
                    MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
                    """
                )

                print(f"OK: {table_name}")
                created_tables.append(table_name)

            except Exception as e:
                print(f"Erreur sur {key} -> {table_name}: {e}")

    finally:
        try:
            cursor.close()
        except Exception:
            pass

    return created_tables