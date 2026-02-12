import os
import tempfile
import pandas as pd
from odf.opendocument import load as load_odf
from odf.text import P
from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes



def convert_odt_to_parquet(path_to_odt_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_odt_key)
    
    with tempfile.NamedTemporaryFile(suffix=".odt", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = load_odf(tmp_path)
        paragraphs = doc.getElementsByType(P)
        
        records = []
        for para_num, para in enumerate(paragraphs):
            text = "".join([str(node) for node in para.childNodes if node.nodeType == node.TEXT_NODE])
            if text.strip():
                records.append({
                    'paragraph_number': para_num + 1,
                    'text': text,
                    'char_count': len(text),
                    'word_count': len(text.split())
                })
        
        df = pd.DataFrame(records)
        
        parquet_buffer = dataframe_to_parquet_bytes(df)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_odt_key).rsplit('.', 1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer)
        
        print(f"ODT converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)