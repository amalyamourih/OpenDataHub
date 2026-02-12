import os
import tempfile
from docx import Document
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes

def convert_docx_to_parquet(path_to_docx_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_docx_key)
    
    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = Document(tmp_path)
        records = []
        
        for para_num, para in enumerate(doc.paragraphs):
            if para.text.strip():  # Ignorer les paragraphes vides
                records.append({
                    'paragraph_number': para_num + 1,
                    'text': para.text,
                    'style': para.style.name if para.style else None,
                    'char_count': len(para.text),
                    'word_count': len(para.text.split())
                })
        
        df = pd.DataFrame(records)
        
        parquet_buffer = dataframe_to_parquet_bytes(df)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_docx_key).rsplit('.', 1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer)
        
        print(f"DOCX converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)