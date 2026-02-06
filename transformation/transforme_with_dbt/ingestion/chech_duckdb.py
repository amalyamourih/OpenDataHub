import duckdb
import os

current_dir = os.path.dirname(__file__)  # dossier de ingest.py
db_path = os.path.join(current_dir, "..", "warehouse", "warehouse.duckdb")
db_path = os.path.abspath(db_path)  # chemin absolu pour éviter les erreurs

# Connexion à la base DuckDB
conx = duckdb.connect(db_path)

# Lister toutes les tables
tables = conx.execute("SHOW TABLES").fetchall()
print("Tables disponibles : ", [t[0] for t in tables])

# Afficher les 5 premières lignes de chaque table
for table_name in [t[0] for t in tables]:
    print(f"\nTable: {table_name}")
    df = conx.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
    print(df)
    schema = conx.execute(f"DESCRIBE {table_name}").fetchall()
    for row in schema:
        print(f" {row[0]} : {row[1]}")
