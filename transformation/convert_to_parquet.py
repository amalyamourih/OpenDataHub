import os
import json
import logging
import hashlib
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_INPUT_PREFIX = os.getenv("S3_INPUT_PREFIX", "samples_for_test/raw_data").rstrip("/")
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "samples_for_test/parquet_data").rstrip("/")

if not S3_BUCKET:
    raise ValueError("S3_BUCKET est vide. Mets-le dans .env ou en variable d'environnement.")


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# -------------------------
# 3) Mapping catégories -> extensions
# -------------------------

# -------------------------
# 3) Mapping catégories -> extensions
# -------------------------
DATA_FORMATS = {
    "tabular": {
        "csv": {}, "tsv": {}, "xls": {}, "xlsx": {}, "ods": {}, "txt": {}, "dbf": {},
        "parquet": {}, "pq": {}
    },
    "geospatial_vector": {
        "shp": {}, "shz": {}, "geojson": {}, "kml": {}, "kmz": {}, "gpkg": {}, "dwg": {}, "dxf": {}
    },
    "geospatial_raster": {
        "tiff": {}, "tif": {}, "jp2": {}, "ecw": {}
    },
    "databases": {
        "sql": {}, "db": {}
    },
}

SUPPORTED_EXTENSIONS = sorted({ext for cat in DATA_FORMATS for ext in DATA_FORMATS[cat].keys()} | {
    "gz", "bz2", "zip", "shx", "prj", "cpg"
})


def ext_to_category(ext: str) -> str:
    ext = ext.lower()
    for category, exts in DATA_FORMATS.items():
        if ext in exts:
            return category
    return "unknown"



def build_spark() -> SparkSession:
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    builder = (
        SparkSession.builder
        .appName("S3_Converter")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    # Inject creds (si tu utilises IAM role sur EC2/EMR, tu peux enlever ça)
    if access_key and secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key) \
                         .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        if session_token:
            builder = builder.config("spark.hadoop.fs.s3a.session.token", session_token)

    spark = builder.getOrCreate()
    return spark

spark = build_spark()

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def list_s3_objects(bucket: str, prefix: str):
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return keys


def s3a_uri(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{key}"


def parse_layout(key: str, input_prefix: str):
    """
    Attend un layout du genre:
      <input_prefix>/<category>/<ext>/.../filename.ext
    Exemple:
      samples_for_test/raw_data/tabular/csv/2026/01/file.csv

    Retourne (category, ext, filename)
    """
    rel = key[len(input_prefix):].lstrip("/")  # partie après prefix
    parts = rel.split("/")

    if len(parts) < 3:
        # pas assez profond
        filename = parts[-1] if parts else Path(key).name
        ext = filename.split(".")[-1].lower() if "." in filename else ""
        return ("unknown", ext, filename)

    category = parts[0]
    ext_folder = parts[1]
    filename = parts[-1]
    real_ext = filename.split(".")[-1].lower() if "." in filename else ""

    # On fait confiance au dossier ext si présent, sinon ext du fichier
    ext = ext_folder.lower() if ext_folder else real_ext

    # Si le dossier catégorie est pas dans DATA_FORMATS, on déduit via extension
    if category not in DATA_FORMATS:
        category = ext_to_category(real_ext)

    return (category, ext, filename)


# -------------------------
# 6) Conversion (ton approche pandas + spark)
# -------------------------
def convert_to_parquet_pandas(input_s3a: str, output_s3a: str) -> bool:
    file_type = input_s3a.split(".")[-1].lower()

    try:
        logger.info(f"{Path(input_s3a).name} ({file_type})")

        # Pandas ne lit pas directement s3a://
        # Donc: on lit via Spark si possible pour formats simples, sinon download temporaire.
        # Mais comme tu as déjà beaucoup de logique "local file", on fait:
        # -> télécharger vers /tmp, puis appliquer ton code.

        import tempfile
        import shutil

        tmpdir = tempfile.mkdtemp()
        local_path = os.path.join(tmpdir, Path(urlparse(input_s3a).path).name)

        # Download S3 object
        # input_s3a = s3a://bucket/key  => on extrait bucket + key
        u = urlparse(input_s3a.replace("s3a://", "s3://"))
        bucket = u.netloc
        key = u.path.lstrip("/")

        s3_client().download_file(bucket, key, local_path)

        # Lire le fichier local (ta logique)
        filepath = local_path
        file_type = filepath.split(".")[-1].lower()

        df = None

        if file_type == "csv":
            df = pd.read_csv(filepath)
        elif file_type == "tsv":
            df = pd.read_csv(filepath, sep="\t")
        elif file_type == "json":
            df = pd.read_json(filepath)
        elif file_type == "xlsx":
            df = pd.read_excel(filepath, sheet_name=0, engine="openpyxl")
        elif file_type == "xls":
            df = pd.read_excel(filepath, sheet_name=0, engine="xlrd")
        elif file_type == "ods":
            df = pd.read_excel(filepath, sheet_name=0, engine="odf")
        elif file_type == "txt":
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                df = pd.DataFrame({"content": f.readlines()})
        elif file_type == "sql":
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                df = pd.DataFrame({"statement": f.readlines()})
        elif file_type == "db":
            import sqlite3
            conn = sqlite3.connect(filepath)
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            if len(tables) == 0:
                shutil.rmtree(tmpdir, ignore_errors=True)
                return False
            table = tables.iloc[0, 0]
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
        elif file_type == "parquet":
            df = pd.read_parquet(filepath)
        elif file_type == "geojson":
            with open(filepath, "r") as f:
                data = json.load(f)
            rows = [ft["properties"] for ft in data.get("features", [])]
            df = pd.DataFrame(rows)
        elif file_type in ["gz", "bz2"]:
            if file_type == "gz":
                import gzip
                with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
            else:
                import bz2
                with bz2.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
            df = pd.DataFrame({"content": lines})
        elif file_type == "shp":
            import geopandas as gpd
            gdf = gpd.read_file(filepath)
            gdf["geometry"] = gdf["geometry"].astype(str)
            df = gdf
        elif file_type == "zip":
            import zipfile
            import tempfile as _tf
            tmp2 = _tf.mkdtemp()
            with zipfile.ZipFile(filepath) as z:
                z.extractall(tmp2)
            rows = []
            for root, _, files in os.walk(tmp2):
                for f in files:
                    if f.endswith(".csv"):
                        pdf = pd.read_csv(os.path.join(root, f))
                        rows.extend(pdf.to_dict("records"))
            if not rows:
                shutil.rmtree(tmpdir, ignore_errors=True)
                return False
            df = pd.DataFrame(rows)
        elif file_type in ["prj", "cpg"]:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            df = pd.DataFrame([{
                "filename": Path(filepath).name,
                "type": file_type,
                "content": content.strip()
            }])
        elif file_type == "shx":
            with open(filepath, "rb") as f:
                data = f.read()
            df = pd.DataFrame([{
                "filename": Path(filepath).name,
                "type": "shx",
                "size_bytes": len(data),
                "md5": hashlib.md5(data).hexdigest(),
                "head_hex_256": data[:256].hex()
            }])
        elif file_type == "dbf":
            try:
                from dbfread import DBF
                table = DBF(filepath, load=True, ignore_missing_memofile=True, char_decode_errors="ignore")
                df = pd.DataFrame(iter(table))
            except Exception:
                with open(filepath, "rb") as f:
                    data = f.read()
                df = pd.DataFrame([{
                    "filename": Path(filepath).name,
                    "type": "dbf",
                    "size_bytes": len(data),
                    "md5": hashlib.md5(data).hexdigest(),
                    "head_hex_256": data[:256].hex()
                }])
        else:
            logger.error("Format non supporté")
            shutil.rmtree(tmpdir, ignore_errors=True)
            return False

        if df is None or df.empty:
            logger.error("Aucune donnée")
            shutil.rmtree(tmpdir, ignore_errors=True)
            return False

        df = df.fillna("")

        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("overwrite").parquet(output_s3a)

        logger.info(f"{len(df)} lignes → Parquet")
        shutil.rmtree(tmpdir, ignore_errors=True)
        return True

    except Exception as e:
        logger.error(f"{str(e)[:140]}")
        return False


# -------------------------
# 7) Main S3
# -------------------------
def main():
    logger.info("\n" + "=" * 60)
    logger.info("CONVERSION S3 → PARQUET (Pandas + Spark)")
    logger.info("=" * 60 + "\n")

    keys = list_s3_objects(S3_BUCKET, S3_INPUT_PREFIX)

    success, failed, skipped = 0, 0, 0

    for key in sorted(keys):
        filename = Path(key).name
        ext = filename.split(".")[-1].lower() if "." in filename else ""

        # skip "folders" and unsupported
        if not ext or ext not in SUPPORTED_EXTENSIONS:
            skipped += 1
            continue

        category, ext_folder, _ = parse_layout(key, S3_INPUT_PREFIX)

        # output path: <output_prefix>/<category>/<ext>/<base>.parquet
        base_name = filename.rsplit(".", 1)[0]
        out_key = f"{S3_OUTPUT_PREFIX}/{category}/{ext_folder}/{base_name}.parquet"

        input_uri = s3a_uri(S3_BUCKET, key)
        output_uri = s3a_uri(S3_BUCKET, out_key)

        if convert_to_parquet_pandas(input_uri, output_uri):
            success += 1
        else:
            failed += 1

    logger.info("\n" + "=" * 60)
    logger.info(f"OK={success} | FAIL={failed} | SKIP={skipped}")
    logger.info(f"INPUT : s3://{S3_BUCKET}/{S3_INPUT_PREFIX}/")
    logger.info(f"OUTPUT: s3://{S3_BUCKET}/{S3_OUTPUT_PREFIX}/")
    logger.info("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
