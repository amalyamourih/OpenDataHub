from pathlib import Path

# Chemins
bronze_folder = Path("../models/bronze")
silver_folder = Path("../models/silver")

# Nom de la macro dbt
macro_name = "auto_clean_pipeline"

for bronze_file in bronze_folder.glob("bronze_*.sql"):
    bronze_model_name = bronze_file.stem      
    base_name = bronze_model_name.replace("bronze_", "", 1) 

    silver_file = silver_folder / f"silver_{base_name}.sql"

    sql_content = f"""{{{{ {macro_name}(ref('{bronze_model_name}')) }}}}
"""

    silver_file.write_text(sql_content, encoding="utf-8")
    print(f"✔ Generated {silver_file}")
