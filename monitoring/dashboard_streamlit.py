import os
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import boto3
import psycopg2


st.set_page_config(page_title="OpenDataHub SQL – Monitoring", layout="wide")


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        return ""
    return v


@st.cache_resource
def get_s3_client(region: str):
    return boto3.client("s3", region_name=region)


@st.cache_resource
def get_pg_conn(host: str, port: int, db: str, user: str, pwd: str):
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


def bytes_to_human(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024.0:
            return f"{x:.2f} {u}"
        x /= 1024.0
    return f"{x:.2f} PB"


def list_s3_objects(bucket: str, prefix: str, region: str, max_keys: int = 2000) -> pd.DataFrame:
    s3 = get_s3_client(region)
    paginator = s3.get_paginator("list_objects_v2")

    rows = []
    seen = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            rows.append(
                {
                    "key": key,
                    "size_bytes": int(obj.get("Size", 0)),
                    "last_modified": obj.get("LastModified"),
                }
            )
            seen += 1
            if seen >= max_keys:
                break
        if seen >= max_keys:
            break

    df = pd.DataFrame(rows)
    if not df.empty:
        df["size"] = df["size_bytes"].apply(bytes_to_human)
        df = df.sort_values("last_modified", ascending=False)
    return df


def airflow_recent_runs(
    conn,
    dag_id: Optional[str] = None,
    limit: int = 50
) -> pd.DataFrame:
    """
    Compatible Airflow 2.x metadata schema.
    Tables used:
      - dag_run (run_id, dag_id, state, start_date, end_date, execution_date/data_interval_*)
      - task_instance (optional; not needed here)
    """
    with conn.cursor() as cur:
        if dag_id:
            cur.execute(
                """
                SELECT dag_id, run_id, state, start_date, end_date, execution_date
                FROM dag_run
                WHERE dag_id = %s
                ORDER BY start_date DESC NULLS LAST
                LIMIT %s
                """,
                (dag_id, limit),
            )
        else:
            cur.execute(
                """
                SELECT dag_id, run_id, state, start_date, end_date, execution_date
                FROM dag_run
                ORDER BY start_date DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["dag_id", "run_id", "state", "start_date", "end_date", "execution_date"])
    if not df.empty:
        df["duration_s"] = (df["end_date"] - df["start_date"]).dt.total_seconds()
    return df


def kpis_from_s3(df: pd.DataFrame) -> Tuple[int, int, Optional[datetime]]:
    if df.empty:
        return 0, 0, None
    total_files = len(df)
    total_bytes = int(df["size_bytes"].sum())
    last_ts = df["last_modified"].max()
    return total_files, total_bytes, last_ts


st.title("OpenDataHub SQL – Dashboard de supervision")

with st.sidebar:
    st.header("Configuration")
    st.subheader("S3")
    s3_bucket = st.text_input("S3_BUCKET", value=env("S3_BUCKET", ""))
    s3_region = st.text_input("AWS_REGION", value=env("AWS_DEFAULT_REGION", "eu-west-3"))
    s3_prefix = st.text_input("Préfixe à monitorer", value=env("S3_MONITOR_PREFIX", "parquets_files/"))

    st.subheader("Airflow DB (Postgres)")
    pg_host = st.text_input("PG_HOST", value=env("PG_HOST", "postgres"))
    pg_port = st.number_input("PG_PORT", value=int(env("PG_PORT", "5432")))
    pg_db = st.text_input("PG_DB", value=env("POSTGRES_DB", "airflow"))
    pg_user = st.text_input("PG_USER", value=env("POSTGRES_USER", "airflow"))
    pg_pwd = st.text_input("PG_PASSWORD", value=env("POSTGRES_PASSWORD", "airflow"), type="password")

    st.subheader("Filtre DAG")
    dag_filter = st.text_input("dag_id (optionnel)", value="")

    st.divider()
    refresh = st.button("Rafraîchir")


col1, col2 = st.columns(2)

# --- S3 panel ---
with col1:
    st.subheader("S3 – Fichiers récents")
    if not s3_bucket:
        st.warning("Renseigne S3_BUCKET.")
    else:
        try:
            s3_df = list_s3_objects(s3_bucket, s3_prefix, s3_region, max_keys=2000)
            total_files, total_bytes, last_ts = kpis_from_s3(s3_df)

            k1, k2, k3 = st.columns(3)
            k1.metric("Fichiers", total_files)
            k2.metric("Taille totale", bytes_to_human(total_bytes))
            k3.metric("Dernier objet", str(last_ts) if last_ts else "—")

            st.dataframe(
                s3_df[["key", "size", "last_modified"]].head(200),
                use_container_width=True,
                hide_index=True,
            )
        except Exception as e:
            st.error(f"Erreur S3: {e}")

# --- Airflow panel ---
with col2:
    st.subheader("Airflow – Derniers DAG runs")
    try:
        conn = get_pg_conn(pg_host, pg_port, pg_db, pg_user, pg_pwd)
        runs_df = airflow_recent_runs(conn, dag_id=dag_filter.strip() or None, limit=100)

        if runs_df.empty:
            st.info("Aucun run trouvé (ou mauvais accès DB).")
        else:
            # KPIs
            ok = (runs_df["state"] == "success").sum()
            failed = (runs_df["state"] == "failed").sum()
            running = (runs_df["state"] == "running").sum()

            a1, a2, a3 = st.columns(3)
            a1.metric("Success", int(ok))
            a2.metric("Failed", int(failed))
            a3.metric("Running", int(running))

            st.dataframe(
                runs_df[["dag_id", "run_id", "state", "start_date", "end_date", "duration_s"]].head(100),
                use_container_width=True,
                hide_index=True,
            )
    except Exception as e:
        st.error(f"Erreur Airflow DB: {e}")

st.divider()
st.caption(
    "Notes: ce dashboard lit (1) la base Postgres Airflow pour les runs et (2) S3 pour les objets sous un préfixe."
)