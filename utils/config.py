import os
from dotenv import load_dotenv

load_dotenv()
DATA_GOUV_API_ROOT = os.getenv("DATA_GOUV_API_ROOT", "https://www.data.gouv.fr/api/1")
DATASET_SLUG = os.getenv("DATASET_SLUG")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_iNPUT_PREFIX = os.getenv("S3_INPUT_PREFIX", "raw_files")
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "parquets_files")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
