import os
import tempfile
import fitz  
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes


def convert_pdf_to_parquet(path_to_pdf_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_pdf_key)
    
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = fitz.open(tmp_path)
        records = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            records.append({
                'page_number': page_num + 1,
                'text': text,
                'char_count': len(text),
                'word_count': len(text.split())
            })
        
        doc.close()
        df = pd.DataFrame(records)
        
        parquet_buffer = dataframe_to_parquet_bytes(df)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_pdf_key).rsplit('.', 1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer.read())
        
        print(f"PDF converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)
