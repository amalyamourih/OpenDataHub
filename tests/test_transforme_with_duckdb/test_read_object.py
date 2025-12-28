from src.transforme_with_duckdb.read_object import read_meta_data_

def test_extract_meta_data(path_to_key):
    file , file_name , extension , path = read_meta_data_(path_to_key)
    return file , file_name , extension , path

if __name__ == "__main__":
    path_to_csv_file = "tabular/csv/export-dataservice-20251209-060747.csv"
    test_file , test_file_name , test_extension , test_path = test_extract_meta_data(path_to_csv_file)
    print(test_file)
    print(test_file_name)
    print(test_extension)
    print(path_to_csv_file)