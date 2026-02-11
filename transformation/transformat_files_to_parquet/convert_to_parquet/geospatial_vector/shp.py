from ingestion.s3.io import read_s3_object
import tempfile
import os
import geopandas as gpd
from converts.geo import GeoToParquet

class SHPConverter(GeoToParquet):

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)

        with tempfile.TemporaryDirectory() as tmpdir:
            shp_path = os.path.join(tmpdir, "file.shp")
            with open(shp_path, "wb") as f:
                f.write(content)

            gdf = gpd.read_file(shp_path)

        self._export(gdf, s3_key)
