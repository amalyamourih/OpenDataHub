import yaml
import os

# Chemin
source_file = "../models/source/source.yml"
bronze_folder = "../models/bronze/"

# Lire le fichier source.yml
with open(source_file, "r") as f:
    sources = yaml.safe_load(f)

# Parcourir toutes les tables de la source
for source in sources.get("sources", []):
    if source["name"] == "opendatahub":
        for table in source.get("tables", []):
            table_name = table["name"]
            sql_file_path = os.path.join(bronze_folder, f"bronze_{table_name}.sql")
            with open(sql_file_path, "w") as f_sql:
                f_sql.write(f"""-- Modèle bronze pour {table_name}
                            select *
                            from {{ source('opendatahub', '{table_name}') }};
                        """)

print(f"Les fichiers SQL sont génerer dans le dossier {bronze_folder}")
