import yaml
import os


def load_sources_yaml(source_file_path):
    with open(source_file_path, "r") as f:
        return yaml.safe_load(f)


def generate_bronze_sql_files(sources_dict, bronze_folder, source_name="opendatahub"):

    os.makedirs(bronze_folder, exist_ok=True)

    for source in sources_dict.get("sources", []):
        if source.get("name") == source_name:
            for table in source.get("tables", []):
                table_name = table.get("name")

                sql_file_path = os.path.join(
                    bronze_folder,
                    f"bronze_{table_name}.sql"
                )

                with open(sql_file_path, "w") as f_sql:
                    f_sql.write(f"""-- Modèle bronze pour {table_name}
select *
from {{{{ source('{source_name}', '{table_name}') }}}};
""")

    print(f"Les fichiers SQL sont générés dans le dossier {bronze_folder}")