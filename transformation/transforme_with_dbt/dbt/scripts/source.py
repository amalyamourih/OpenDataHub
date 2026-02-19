import duckdb
import yaml
from pathlib import Path
import os


def get_duckdb_connection(db_path: str):

    return duckdb.connect(db_path)

def get_table_names(conx) -> list:

    return [t[0] for t in conx.execute("SHOW TABLES").fetchall()]

def build_sources_dict(tables: list, source_name: str = "opendatahub") -> dict:
 
    return {
        'version': 2,
        'sources': [
            {
                'name': source_name,
                'tables': [{'name': table} for table in tables]
            }
        ]
    }

def save_yaml(data: dict, output_path: str):

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)  
    with open(output_file, 'w') as f:
        yaml.dump(data, f, sort_keys=False)
    print(f"{output_file} généré avec succès !")

