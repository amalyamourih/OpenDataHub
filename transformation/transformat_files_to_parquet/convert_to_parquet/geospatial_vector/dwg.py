import os
import tempfile
import ezdxf
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes



def convert_dwg_to_parquet(path_to_dwg_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_dwg_key)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        dwg_path = os.path.join(tmpdir, "file.dwg")
        with open(dwg_path, 'wb') as f:
            f.write(content)
        
        doc = ezdxf.readfile(dwg_path)  

        records = []
        for e in doc.modelspace():
            records.append(e.dxfattribs())
        
        df = pd.DataFrame(records)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        parquet_buffer = dataframe_to_parquet_bytes(df)

        parquet_key = f"parquets_files/{os.path.basename(path_to_dwg_key).rsplit('.', 1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer)
        print(f"DWG converti : {parquet_key}")

