import io
import pandas as pd
import ezdxf

from converts.cad import CADToParquet
from ingestion.s3.io import read_s3_object


class DXFConverter(CADToParquet):

    def convert(self, s3_key: str):
        content = io.BytesIO(read_s3_object(s3_key))
        doc = ezdxf.readfile(content)

        records = [e.dxfattribs() for e in doc.modelspace()]
        df = pd.DataFrame(records)

        self._export(df, s3_key)
