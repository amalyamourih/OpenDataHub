import duckdb
import yaml
import os 

from utils import list_s3_keys, get_table_name_from_key
from src.ingestion_to_S3.config import S3_BUCKET, AWS_REGION

# Charger la configuration statique 
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

prefix = config["s3"]["prefix"]
db_path = config["duckdb"]["database_path"]
formats = tuple(f".{fmt}" for fmt in config["formats"])

# Chemin relatif depuis ingestion/ vers le vrai warehouse/
current_dir = os.path.dirname(__file__)  # dossier de ingest.py
db_path = os.path.join(current_dir, "..", "warehouse", "warehouse.duckdb")
db_path = os.path.abspath(db_path)  # chemin absolu pour éviter les erreurs

# Supprimer le fichier vide si existant
if os.path.exists(db_path) and os.path.getsize(db_path) == 0:
    os.remove(db_path)

os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Connexion avec DuckDB
conx = duckdb.connect(db_path)

conx.execute("INSTALL httpfs;")
conx.execute("LOAD httpfs;")

conx.execute(f"SET s3_region='{AWS_REGION}';")

keys = list_s3_keys(S3_BUCKET, prefix)
print(f"{len(keys)} fichiers trouvés dans S3")

for key in keys:
    if not key.endswith(formats):
        continue
    table_name = get_table_name_from_key(key)
    s3_path = f"s3://{S3_BUCKET}/{key}"

    print(f"ingestion {s3_path} -- > {table_name}")

    if key.endswith(".csv"):
        conx.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{s3_path}');
        """)

    elif key.endswith(".tsv"):
        conx.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{s3_path}', delim='\\t');
        """)
conx.close()
print("Ingestion terminée ! ")




