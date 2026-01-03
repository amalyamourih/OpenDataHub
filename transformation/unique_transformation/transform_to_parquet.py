import os
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession
import pandas as pd

# Configuration
INPUT_DIR = "samples_for_test/raw_data"
OUTPUT_DIR = "samples_for_test/parquet_data"

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Spark Session (local mode, single partition)
spark = SparkSession.builder \
    .appName("Converter") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()


def convert_to_parquet_pandas(filepath: str, output_path: str) -> bool:
    """Convertit n'importe quel format en Parquet avec pandas"""
    file_type = filepath.split('.')[-1].lower()
    
    try:
        logger.info(f"⏳ {Path(filepath).name} ({file_type})")
        
        # Lire le fichier
        if file_type == 'csv':
            df = pd.read_csv(filepath)
        
        elif file_type == 'tsv':
            df = pd.read_csv(filepath, sep='\t')
        
        elif file_type == 'json':
            df = pd.read_json(filepath)
        
        elif file_type == 'xlsx':
            df = pd.read_excel(filepath, sheet_name=0, engine='openpyxl')
        
        elif file_type == 'xls':
            df = pd.read_excel(filepath, sheet_name=0, engine='xlrd')
        
        elif file_type == 'ods':
            df = pd.read_excel(filepath, sheet_name=0, engine='odf')
        
        elif file_type == 'txt':
            # Lire comme texte brut
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            df = pd.DataFrame({'content': lines})
        
        elif file_type == 'sql':
            # Lire comme texte brut
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            df = pd.DataFrame({'statement': lines})
        
        elif file_type == 'db':
            import sqlite3
            conn = sqlite3.connect(filepath)
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            if len(tables) > 0:
                table = tables.iloc[0, 0]
                df = pd.read_sql(f"SELECT * FROM {table}", conn)
            else:
                return False
        
        elif file_type == 'parquet':
            df = pd.read_parquet(filepath)
        
        elif file_type == 'geojson':
            with open(filepath, 'r') as f:
                data = json.load(f)
            rows = [f['properties'] for f in data.get('features', [])]
            df = pd.DataFrame(rows)
        
        elif file_type in ['gz', 'bz2']:
            # Fichiers compressés en tant que texte
            if file_type == 'gz':
                import gzip
                with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
            else:
                import bz2
                with bz2.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
            df = pd.DataFrame({'content': lines})
        
        elif file_type == 'shp':
            import geopandas as gpd
            gdf = gpd.read_file(filepath)
            gdf['geometry'] = gdf['geometry'].astype(str)
            df = gdf
        
        elif file_type == 'zip':
            import zipfile
            import tempfile
            tmpdir = tempfile.mkdtemp()
            with zipfile.ZipFile(filepath) as z:
                z.extractall(tmpdir)
            
            rows = []
            for root, dirs, files in os.walk(tmpdir):
                for f in files:
                    if f.endswith('.csv'):
                        pdf = pd.read_csv(os.path.join(root, f))
                        rows.extend(pdf.to_dict('records'))
            
            if not rows:
                return False
            df = pd.DataFrame(rows)
        
        else:
            logger.error(f" Format non supporté")
            return False
        
        if df is None or df.empty:
            logger.error(f"  Aucune donnée")
            return False
        
        # Nettoyer les NaN
        df = df.fillna('')
        
        # Convertir en Parquet via Spark
        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f" {len(df)} lignes → Parquet")
        return True
        
    except Exception as e:
        logger.error(f" {str(e)[:80]}")
        return False


def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    logger.info("\n" + "="*60)
    logger.info("CONVERSION → PARQUET (Pandas + Spark)")
    logger.info("="*60 + "\n")
    
    supported = ['csv', 'tsv', 'json', 'parquet', 'txt', 'xlsx', 'xls', 'ods',
                 'geojson', 'sql', 'db', 'shp', 'zip', 'bz2', 'gz']
    ignore = ['shx', 'prj', 'cpg', 'dbf']
    
    success, failed, skipped = 0, 0, 0
    
    for filename in sorted(os.listdir(INPUT_DIR)):
        filepath = os.path.join(INPUT_DIR, filename)
        if not os.path.isfile(filepath):
            continue
        
        ext = filename.split('.')[-1].lower()
        
        if ext in ignore:
            logger.info(f"⊘ {filename} (fichier associé)\n")
            skipped += 1
            continue
        
        if ext not in supported:
            logger.info(f"⊘ {filename} (format non supporté)\n")
            skipped += 1
            continue
        
        base_name = filename.rsplit('.', 1)[0]
        output_path = os.path.join(OUTPUT_DIR, base_name + ".parquet")
        
        if convert_to_parquet_pandas(filepath, output_path):
            success += 1
        else:
            failed += 1
        logger.info("")
    
    logger.info("="*60)
    logger.info(f"✅ {success} | ❌ {failed} | ⊘ {skipped}")
    logger.info(f"Résultats: {OUTPUT_DIR}")
    logger.info("="*60 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()