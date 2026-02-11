import io
import geopandas as gpd
from converts.geo import GeoToParquet
from ingestion.s3.io import read_s3_object


class KMLConverter(GeoToParquet):

    def convert(self, s3_key: str):
        content = io.BytesIO(read_s3_object(s3_key))
        gdf = gpd.read_file(content, driver="KML")

        self._export(gdf, s3_key)
