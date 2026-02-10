import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.config import DATA_GOUV_API_ROOT, DATASET_SLUG, AWS_REGION, S3_BUCKET 

def test_env_variables():
    assert DATA_GOUV_API_ROOT is not None
    assert DATASET_SLUG is not None
    assert AWS_REGION is not None
    assert S3_BUCKET is not None

    print("Toutes les variables du .env sont correctement chargées !")
    print("DATA_GOUV_API_ROOT =", DATA_GOUV_API_ROOT)
    print("DATASET_SLUG =", DATASET_SLUG)
    print("AWS_REGION =", AWS_REGION)
    print("S3_BUCKET =", S3_BUCKET)


if __name__ == "__main__":
    test_env_variables()
