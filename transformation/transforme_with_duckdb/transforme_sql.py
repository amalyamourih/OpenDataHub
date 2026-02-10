
import pandas as pd
import boto3
import csv
import tempfile
import sqlite3
import numpy as np
import json
import geopandas as gpd
import zipfile
import io
import rasterio
import duckdb
from utils.config import S3_BUCKET
def read_tabular_data(file , file_name , extension , path_to_key):
    conx = duckdb.connect()
    # CSV 
    if extension == 'csv':
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=path_to_key)
        sample = obj['Body'].read(2048).decode('utf-8')  
        dialect = csv.Sniffer().sniff(sample)
        sep = dialect.delimiter
        conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM read_csv_auto('s3://{S3_BUCKET}/{path_to_key}' ,delim='{sep}', strict_mode=false, header = true)
            """)
        print(f"Table '{file_name}' " + "créée avec succès à partir du fichier CSV.")
        tables = conx.execute("SHOW TABLES").fetchall()
        return tables


    #TSV
    elif extension == 'tsv':
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=path_to_key)
        conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM read_csv_auto('s3://{S3_BUCKET}/{path_to_key}' , delim='\t' , strict_mode=false)
            """)
        tables = conx.execute("SHOW TABLES").fetchall()
        return tables
    # XLS / XLSX
    elif extension in ['xls', 'xlsx']:
        conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM read_excel_auto('s3://mon-bucket/{path_to_key}', strict_mode=false)
            """)
    # ODS
    elif extension == 'ods':
        df = pd.read_excel(io.BytesIO(file['Body'].read()), engine='odf')
        conx.register("df_temp", df)
        conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM df_temp
            """)

    # TXT condition que si les separateurs est connu
    elif extension == 'txt':
        conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM read_csv_auto('s3://mon-bucket/{path_to_key}')
            """)
    # PARQUET / PQ
    elif extension in ['parquet', 'pq']:
         conx.execute(f"""
            CREATE TABLE '{file_name}' AS
            SELECT * FROM read_parquet('s3://mon-bucket/{path_to_key}')
            """)
    else:
        raise ValueError(f"Format de fichier non supporté: {extension}")
"""   
Pour cela duckdb il peut pas lire directement les formats SHP ,KMZ , DWG/DXF ,
KML , etc..


"""
def read_geospatial_vector_data(file , file_name , extension, path_to_key):
    conx = duckdb.connect()
    # Shapefile (SHP)
    if extension == 'shp':
        gdf = gpd.read_file(io.BytesIO(file['Body'].read()))
        gdf.to_parquet("temp.parquet")
        conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM read_parquet('temp.parquet')")
        print(f"Table '{file_name}' " + "créée avec succès à partir du fichier SHP.")

    elif extension == 'shz': 
        with zipfile.ZipFile(io.BytesIO(file['Body'].read())) as z:
            gdf = gpd.read_file(f"zip://{z.filename}")
            gdf.to_parquet("temp.parquet")
            conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM read_parquet('temp.parquet')")
            print(f"Table '{file_name}' " + "créée avec succès à partir du fichier SHZ.")
        
    
    # GeoJSON
    elif extension == 'geojson':
        content = file['Body'].read()
        geojson_dict = json.loads(content)

        gdf = gpd.GeoDataFrame.from_features(geojson_dict["features"])
        conx.register("temp_gdf", gdf)
        conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM temp_gdf")
    
    # KML / KMZ
    elif extension == 'kml':
        content = file['Body'].read()
        with open("temp.kml", "wb") as f:
            f.write(content)
        gdf = gpd.read_file("temp.kml", driver='KML')
        conx.register("temp_gdf", gdf)
        conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM temp_gdf")

    elif extension == 'kmz':
        with zipfile.ZipFile(io.BytesIO(file['Body'].read())) as z:
            kml_files = [f for f in z.namelist() if f.endswith('.kml')]
            if not kml_files:
                raise ValueError("Aucun fichier KML trouvé dans le KMZ")
            return gpd.read_file(f"zip://{z.filename}!{kml_files[0]}")
    
    # GeoPackage
    elif extension == 'gpkg':
        content = file['Body'].read()

        temp_path = "temp.gpkg"
        with open(temp_path, "wb") as f:
            f.write(content)

        gdf = gpd.read_file(temp_path)

        conx.register("temp_gdf", gdf)
        conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM temp_gdf")
    
    # DWG / DXF (AutoCAD)
    elif extension in ['dwg', 'dxf']:
        return gpd.read_file(io.BytesIO(file['Body'].read()))
    
    else:
        raise ValueError(f"Format géospatial {extension} non supporté")
    
    # Conversion de la géométrie en WKT pour DuckDB
    if 'geometry' in gdf.columns:
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    
    # Création de la table DuckDB
    conx.register("temp_gdf", gdf)
    conx.execute(f"""
        CREATE TABLE {file_name} AS
        SELECT * FROM temp_gdf
    """)
    
    print(f"Table '{file_name}' créée avec succès dans DuckDB.")



def read_geospatial_raster_data_duckdb(file, file_name, extension, path_to_key):
    # Ouvrir le raster en mémoire
    with rasterio.MemoryFile(file['Body'].read()) as memfile:
        with memfile.open() as dataset:
            # Extraire les métadonnées
            meta = {
                'width': dataset.width,
                'height': dataset.height,
                'count': dataset.count,
                'crs': str(dataset.crs),
                'transform': dataset.transform,
                'bounds': dataset.bounds
            }
            all_bands = dataset.read()  # shape = (bandes, height, width)

            # Créer un tableau avec coordonnées et valeurs de pixels
            rows, cols = np.meshgrid(np.arange(dataset.height), np.arange(dataset.width), indexing='ij')
            xs, ys = rasterio.transform.xy(dataset.transform, rows, cols)
            xs = np.array(xs).flatten()
            ys = np.array(ys).flatten()

            # Pour chaque bande, aplatir les valeurs
            data = {'x': xs, 'y': ys}
            for i in range(dataset.count):
                data[f'band_{i+1}'] = all_bands[i].flatten()

            import pandas as pd
            df = pd.DataFrame(data)

            # Charger dans DuckDB
            conx = duckdb.connect(database=':memory:')
            conx.register("raster_table", df)
            conx.execute(f"CREATE TABLE {file_name} AS SELECT * FROM raster_table")

            print("La table est crée !!")
    

# Lire les fichiers de base de données (sql / db)
def read_database_file(file , file_name , extension ,path_to_key):
    conx = duckdb.connect()
    if extension == 'sql':
        sql_script = file['Body'].read().decode('utf-8')
        conx.execute(sql_script)
        
    
    elif extension == 'db':
        # Créer un fichier temporaire pour SQLite
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp_db:
            tmp_db.write(file['Body'].read())
            tmp_db.flush()  # s'assurer que le fichier est écrit

            # Connexion à SQLite
            conn_sqlite = sqlite3.connect(tmp_db.name)
            
            # Connexion à DuckDB en mémoire
            conx = duckdb.connect(database=':memory:')
            
            # Lister toutes les tables dans SQLite
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn_sqlite)
            
            # Charger chaque table dans DuckDB
            for table_name in tables['name']:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn_sqlite)
                conx.register("tmp_table", df)
                conx.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_table")
            
    else:
        raise ValueError(f"Format de base de données non supporté: {extension}")

def others_file(file , file_name , extension , path_to_key):
    if extension == 'json': 
        content = file['Body'].read()  # bytes du JSON
        data = json.loads(content)
        df = pd.DataFrame(data)

        con = duckdb.connect(':memory:')
        con.register('json_table', df)
        con.execute("CREATE TABLE my_json AS SELECT * FROM json_table")



