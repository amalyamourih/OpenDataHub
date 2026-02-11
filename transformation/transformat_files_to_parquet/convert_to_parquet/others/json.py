import io 
import pandas as pd
from convert_to_parquet.converts.structured import StructuredFileConverter
from ingestion.s3.io import read_s3_object

class JSONConverter(StructuredFileConverter):

    def __init__(self, lines=False):
        self.lines = lines

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)
        df = pd.read_json(io.BytesIO(content), lines=self.lines)

        self._export(df, s3_key)
