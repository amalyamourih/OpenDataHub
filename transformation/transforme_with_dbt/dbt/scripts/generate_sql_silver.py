from pathlib import Path


def generate_silver_models(bronze_folder_path, silver_folder_path, macro_name="auto_clean_pipeline"):

    bronze_folder = Path(bronze_folder_path)
    silver_folder = Path(silver_folder_path)

    # Crée le dossier silver s'il n'existe pas
    silver_folder.mkdir(parents=True, exist_ok=True)

    for bronze_file in bronze_folder.glob("bronze_*.sql"):
        bronze_model_name = bronze_file.stem
        base_name = bronze_model_name.replace("bronze_", "", 1)

        silver_file = silver_folder / f"silver_{base_name}.sql"

        sql_content = f"""{{{{ {macro_name}(ref('{bronze_model_name}')) }}}}
"""

        silver_file.write_text(sql_content, encoding="utf-8")
        print(f"Generated {silver_file}")

