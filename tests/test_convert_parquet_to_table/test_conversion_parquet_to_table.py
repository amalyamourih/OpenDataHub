from ingestion.ingestion_to_warehouse.ingestion import ingest_warehouse , create_duckdb_connection, build_db_path , prepare_database_file , load_config
from utils.config import S3_BUCKET


def main():
    config = load_config()

    prefix = config["s3"]["prefix"]
    formats = tuple(f".{fmt}" for fmt in config["formats"])

    db_path = build_db_path()
    prepare_database_file(db_path)

    conx = create_duckdb_connection(db_path)

    ingest_warehouse(conx, S3_BUCKET, prefix, formats)

    conx.close()
    print("Ingestion terminée !")


if __name__ == "__main__":
    main()