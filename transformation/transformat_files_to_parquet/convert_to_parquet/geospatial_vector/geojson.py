import io
import geopandas as gpd
from ingestion.s3.io import read_s3_object
from converts.geo import GeoToParquet


class GeoJSONConverter(GeoToParquet):

    def convert(self, s3_key: str):
        content = io.BytesIO(read_s3_object(s3_key))
        gdf = gpd.read_file(content)

        self._export(gdf, s3_key)
