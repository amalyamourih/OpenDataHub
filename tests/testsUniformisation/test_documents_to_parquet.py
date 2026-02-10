"""
Tests de conversion des formats de documents vers Parquet
Formats: PDF, DOCX, ODT
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import io

from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_pdf_to_parquet,
    convert_docx_to_parquet,
    convert_odt_to_parquet
)


def test_convert_pdf_to_parquet(mock_boto3_client):
    fitz = pytest.importorskip("fitz")
    
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), "Hello World!")
    page.insert_text((50, 100), "Test PDF content.")
    
    pdf_buffer = io.BytesIO()
    doc.save(pdf_buffer)
    doc.close()
    pdf_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(pdf_buffer.getvalue())}
    
    convert_pdf_to_parquet("test_files/document.pdf")

    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    assert 'page_number' in df_result.columns
    assert 'text' in df_result.columns


def test_convert_pdf_to_parquet_multiple_pages(mock_boto3_client):
    fitz = pytest.importorskip("fitz")
    
    doc = fitz.open()
    for i in range(3):
        page = doc.new_page()
        page.insert_text((50, 50), f"Page {i + 1}")
    
    pdf_buffer = io.BytesIO()
    doc.save(pdf_buffer)
    doc.close()
    pdf_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(pdf_buffer.getvalue())}
    
    convert_pdf_to_parquet("test_files/multipage.pdf")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_docx_to_parquet(mock_boto3_client):
    Document = pytest.importorskip("docx").Document
    
    doc = Document()
    doc.add_paragraph("First paragraph")
    doc.add_paragraph("Second paragraph")
    doc.add_paragraph("Third paragraph")
    
    docx_buffer = io.BytesIO()
    doc.save(docx_buffer)
    docx_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(docx_buffer.getvalue())}
    
    convert_docx_to_parquet("test_files/document.docx")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    assert 'paragraph_number' in df_result.columns
    assert 'text' in df_result.columns
    assert len(df_result) == 3

def test_convert_docx_to_parquet_with_headings(mock_boto3_client):
    Document = pytest.importorskip("docx").Document
    
    doc = Document()
    doc.add_heading("Title", level=0)
    doc.add_paragraph("Content")
    doc.add_heading("Section 1", level=1)
    
    docx_buffer = io.BytesIO()
    doc.save(docx_buffer)
    docx_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(docx_buffer.getvalue())}
    
    convert_docx_to_parquet("test_files/headings.docx")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
    
    assert 'paragraph_number' in df_result.columns
    assert 'text' in df_result.columns
    assert 'style' in df_result.columns
    assert len(df_result) == 3
    
def test_convert_odt_to_parquet(mock_boto3_client):
    odf = pytest.importorskip("odf.opendocument")
    from odf.text import P
    from odf.opendocument import OpenDocumentText
    
    doc = OpenDocumentText()
    doc.text.addElement(P(text="First paragraph"))
    doc.text.addElement(P(text="Second paragraph"))
    
    odt_buffer = io.BytesIO()
    doc.save(odt_buffer)
    odt_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(odt_buffer.getvalue())}
    
    convert_odt_to_parquet("test_files/document.odt")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    df_result = pd.read_parquet(io.BytesIO(put_call[1]['Body']))
    print(df_result.head())
