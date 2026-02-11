import tempfile
import geopandas as gpd

from converts.geo import GeoToParquet
from ingestion.s3.io import read_s3_object

class GPKGConverter(GeoToParquet):

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)

        with tempfile.NamedTemporaryFile(suffix=".gpkg") as tmp:
            tmp.write(content)
            tmp.flush()
            gdf = gpd.read_file(tmp.name)

        self._export(gdf, s3_key)
