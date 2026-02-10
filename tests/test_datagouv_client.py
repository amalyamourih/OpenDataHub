import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ingestion.ingestion_to_S3.datagouv_client import get_dataset_metadata, find_resource_for_format

def test_dataset_metadata():
    data = get_dataset_metadata()
    assert "title" in data
    assert "resources" in data
    resource = find_resource_for_format(data)
    assert resource is not None
    print("Test datagouv_client OK")
    print(resource)


if __name__ == "__main__":
    test_dataset_metadata()

