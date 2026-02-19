import os
from transformation.transforme_with_dbt.dbt.scripts.generate_sql_silver import (
    generate_silver_models
)   

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    bronze_folder = os.path.join(project_root, "transformation", "transforme_with_dbt", "dbt", "models", "bronze")
    silver_folder = os.path.join(project_root, "transformation", "transforme_with_dbt", "dbt", "models", "silver")
    generate_silver_models(bronze_folder, silver_folder)
    