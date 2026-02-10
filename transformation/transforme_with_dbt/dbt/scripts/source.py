import duckdb
import yaml
import os

db_path = "../../../warehouse/warehouse.duckdb"
# Connexion à la base Duckdb
conx = duckdb.connect(db_path)

tables = [t[0] for t in conx.execute("SHOW TABLES").fetchall()]

# Crée la structure YAML pour DBT 
sources_dict = {
    'version' : 2 ,
    'sources' : [
        {
            'name': 'opendatahub' ,
            'tables': [{'name': table} for table in tables]
        }
    ]
}

# Sauvgarder dans dbt/models/sources/source.yml
with open('../models/source/source.yml', 'w') as f:
    yaml.dump(sources_dict, f, sort_keys=False)

print("sources.yml généré avec succès !")