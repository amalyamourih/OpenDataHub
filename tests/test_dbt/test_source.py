import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

from transformation.transforme_with_dbt.dbt.scripts.source import (
    get_duckdb_connection,
    get_table_names,
    build_sources_dict,
    save_yaml
)

def generate_dbt_sources():
    warehouse_path = os.path.join(project_root, "warehouse", "warehouse.duckdb")
    conx = get_duckdb_connection(warehouse_path)
    tables = get_table_names(conx)
    sources_dict = build_sources_dict(tables)
    save_yaml(sources_dict, os.path.join(project_root, "transformation","transforme_with_dbt", "dbt", "models", "source", "source.yml"))
    conx.close()

# --- Exemple d'utilisation ---
if __name__ == "__main__":
    generate_dbt_sources()