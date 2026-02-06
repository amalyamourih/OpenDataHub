"""
Tests de conversion des autres formats vers Parquet
Formats: JSON, XML
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import io
import json

from transformation.transforme_with_duckdb.transforme_to_parquet import (
    convert_json_to_parquet,
    convert_xml_to_parquet
)


def test_convert_json_to_parquet_array(mock_boto3_client):
    json_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30}
    ]
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(json.dumps(json_data).encode('utf-8'))
    }
    
    convert_json_to_parquet("test_files/data.json", lines=False)
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    assert len(df_result) == 2


def test_convert_json_to_parquet_lines(mock_boto3_client):
    json_lines = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}'
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(json_lines.encode('utf-8'))
    }
    convert_json_to_parquet("test_files/data.jsonl", lines=True)
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())


def test_convert_json_to_parquet_nested(mock_boto3_client):
    json_data = [
        {"id": 1, "info": {"name": "Alice", "city": "Paris"}},
        {"id": 2, "info": {"name": "Bob", "city": "Lyon"}}
    ]

    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(json.dumps(json_data).encode('utf-8'))
    }
    
    convert_json_to_parquet("test_files/nested.json", lines=False)
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())


def test_convert_xml_to_parquet(mock_boto3_client):
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <data>
        <record><id>1</id><name>Alice</name></record>
        <record><id>2</id><name>Bob</name></record>
        <record><id>3</id><name>Charlie</name></record>
    </data>"""
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(xml_content.encode('utf-8'))
    }
    
    convert_xml_to_parquet("test_files/data.xml", xpath=".//record")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_xml_to_parquet_custom_xpath(mock_boto3_client):
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <root>
        <items>
            <item id="1" name="Item A"/>
            <item id="2" name="Item B"/>
        </items>
    </root>"""
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(xml_content.encode('utf-8'))
    }
    
    convert_xml_to_parquet("test_files/items.xml", xpath=".//item")
    assert mock_boto3_client.put_object.called


def test_convert_xml_to_parquet_with_attributes(mock_boto3_client):
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <data>
        <record id="1" status="active"><name>Alice</name></record>
        <record id="2" status="inactive"><name>Bob</name></record>
    </data>"""
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(xml_content.encode('utf-8'))
    }
    convert_xml_to_parquet("test_files/attrs.xml", xpath=".//record")
    
    assert mock_boto3_client.put_object.called