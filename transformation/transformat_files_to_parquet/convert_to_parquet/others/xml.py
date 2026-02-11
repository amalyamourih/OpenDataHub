from convert_to_parquet.converts.structured import StructuredFileConverter
import io 
import pandas as pd
from ingestion.s3.io import read_s3_object

class XMLConverter(StructuredFileConverter):

    def __init__(self, xpath=".//record"):
        self.xpath = xpath

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)
        df = pd.read_xml(io.BytesIO(content), xpath=self.xpath)

        self._export(df, s3_key)
