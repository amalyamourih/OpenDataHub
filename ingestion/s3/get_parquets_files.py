from ingestion.s3.client import get_s3_client

s3_client = get_s3_client()
def list_s3_keys(bucket, prefix="parquets_files/"):
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return keys
