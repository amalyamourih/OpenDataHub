from src.transforme_with_duckdb.transforme_sql import read_tabular_data

def test_csv_to_sql_table(file, file_name, extension , path): 
    tables = read_tabular_data(file , file_name , extension , path)
    return tables

if __name__ == "__main__": 
    """
    test_file_name = "export-dataservice-20251209-060747.csv"
    test_extension = "csv"
    test_path = "tabular/csv/export-dataservice-20251209-060747.csv"

    show_tables = test_csv_to_sql_table("sdfsdfzd", test_file_name, test_extension, test_path)
    print(show_tables)
    """

    test_file_tsv_name = "test_tsv_file.tsv"
    test_extension = "tsv"
    test_path = "tabular/tsv/test_tsv_file.tsv"

    show_tables = test_csv_to_sql_table("sdfsdfzd", test_file_tsv_name, test_extension, test_path)
    print(show_tables)
