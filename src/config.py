import os
from dotenv import load_dotenv

load_dotenv()

DATA_GOUV_API_ROOT = os.getenv("DATA_GOUV_API_ROOT", "https://www.data.gouv.fr/api/1")
DATASET_SLUG = os.getenv("DATASET_SLUG")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET")