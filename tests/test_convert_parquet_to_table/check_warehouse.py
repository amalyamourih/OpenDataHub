import duckdb
from pathlib import Path
import os


def check_tables():
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    db_path = os.path.join(project_root, "warehouse", "warehouse.duckdb")

    
    conx = duckdb.connect(db_path)

    # Récupérer les tables
    tables = conx.execute("SHOW TABLES").fetchall()

    if not tables:
        print("Aucune table trouvée dans le warehouse.")
    else:
        print(f"{len(tables)} table(s) trouvée(s) :")
        for table in tables:
            print(f"   - {table[0]}")

    conx.close()


if __name__ == "__main__":
    check_tables()