import os
from transformation.transforme_with_dbt.dbt.scripts.generate_sql_bronze import (
    load_sources_yaml,
    generate_bronze_sql_files
)

if __name__ == "__main__":

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    source_file = os.path.join(project_root, "transformation", "transforme_with_dbt", "dbt", "models", "source", "source.yml")
    bronze_folder = os.path.join(project_root, "transformation", "transforme_with_dbt", "dbt", "models", "bronze")

    sources = load_sources_yaml(source_file)
    generate_bronze_sql_files(sources, bronze_folder)


