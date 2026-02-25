# tests/test_ingestion_snowflake.py
import sys
import os
from pathlib import Path

from unittest.mock import MagicMock
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
import pytest

from ingestion.ingestion_to_warehouse.ingestion import (
    create_snowflake_connection,
    load_parquets_to_snowflake,
)


def test_create_snowflake_connection_calls_connector(monkeypatch):
    # patch la fonction connect dans le module déjà importé
    fake_connect = MagicMock(return_value="CONN")
    monkeypatch.setattr(
        "ingestion.ingestion_to_warehouse.ingestion.snowflake.connector.connect",
        fake_connect
    )

    monkeypatch.setenv("SNOWFLAKE_USER", "u")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acc")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "ROLE")

    conx = create_snowflake_connection()

    assert conx == "CONN"
    fake_connect.assert_called_once()


def test_load_parquets_to_snowflake_filters_and_executes_sql(monkeypatch):
    fake_keys = [
        "parquets_files/file1.parquet",
        "parquets_files/file2.PARQUET",
        "parquets_files/not_parquet.csv",
    ]

    # patch les dépendances DANS le module ingestion.py
    monkeypatch.setattr(
        "ingestion.ingestion_to_warehouse.ingestion.list_s3_keys",
        lambda bucket, prefix: fake_keys,
    )
    monkeypatch.setattr(
        "ingestion.ingestion_to_warehouse.ingestion.get_table_name_from_key",
        lambda key: os.path.splitext(os.path.basename(key))[0],
    )
    monkeypatch.setattr(
        "ingestion.ingestion_to_warehouse.ingestion.S3_BUCKET",
        "my-bucket",
    )

    cursor = MagicMock()
    conx = MagicMock()
    conx.cursor.return_value = cursor

    tables = load_parquets_to_snowflake(
        conx,
        prefix="parquets_files/",
        stage_name="MY_S3_STAGE",
        file_format="MY_PARQUET_FORMAT",
        formats=(".parquet",),
    )

    assert tables == ["FILE1", "FILE2"]
    assert cursor.execute.call_count == 4
    cursor.close.assert_called_once()