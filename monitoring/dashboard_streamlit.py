import os
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import boto3
import psycopg2


st.set_page_config(page_title="OpenDataHub SQL – Monitoring", layout="wide")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

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


def list_s3_objects(bucket: str, prefix: str, region: str, max_keys: int = 5000) -> pd.DataFrame:
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


def kpis_from_s3(df: pd.DataFrame) -> Tuple[int, int, Optional[datetime]]:
    if df.empty:
        return 0, 0, None
    total_files = len(df)
    total_bytes = int(df["size_bytes"].sum())
    last_ts = df["last_modified"].max()
    return total_files, total_bytes, last_ts


def s3_breakdown_top_folder(df: pd.DataFrame, base_prefix: str) -> pd.DataFrame:
    """
    base_prefix ex: 'parquets_files/'.
    retourne un count par premier dossier après le prefix.
    """
    if df.empty:
        return pd.DataFrame(columns=["group", "files"])
    groups = []
    for k in df["key"].tolist():
        rest = k[len(base_prefix):] if k.startswith(base_prefix) else k
        group = rest.split("/", 1)[0] if "/" in rest else "root"
        groups.append(group)
    out = pd.DataFrame({"group": groups})
    out = out.value_counts().reset_index()
    out.columns = ["group", "files"]
    return out.sort_values("files", ascending=False)


def airflow_recent_runs(conn, dag_id: Optional[str] = None, limit: int = 50) -> pd.DataFrame:
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

    df = pd.DataFrame(
        rows,
        columns=["dag_id", "run_id", "state", "start_date", "end_date", "execution_date"],
    )
    if not df.empty:
        df["duration_s"] = (df["end_date"] - df["start_date"]).dt.total_seconds()
    return df


def airflow_last_state(runs_df: pd.DataFrame, dag_id: str) -> Tuple[str, str]:
    """
    Retourne (state, run_id) du dernier run.
    """
    d = runs_df[runs_df["dag_id"] == dag_id].copy()
    if d.empty:
        return "—", "—"
    d = d.sort_values("start_date", ascending=False)
    return str(d.iloc[0]["state"]), str(d.iloc[0]["run_id"])


def airflow_state_counts(runs_df: pd.DataFrame) -> Tuple[int, int, int]:
    if runs_df.empty:
        return 0, 0, 0
    ok = int((runs_df["state"] == "success").sum())
    failed = int((runs_df["state"] == "failed").sum())
    running = int((runs_df["state"] == "running").sum())
    return ok, failed, running


# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────

st.title("OpenDataHub SQL – Dashboard de supervision")

with st.sidebar:
    st.header("Configuration")

    st.subheader("S3")
    s3_bucket = st.text_input("S3_BUCKET", value=env("S3_BUCKET", ""))
    s3_region = st.text_input("AWS_REGION", value=env("AWS_DEFAULT_REGION", env("AWS_REGION", "eu-north-1")))
    raw_prefix = st.text_input("Préfixe RAW (input)", value=env("S3_INPUT_PREFIX", "raw_files/"))
    parquet_prefix = st.text_input("Préfixe Parquet (output)", value=env("S3_OUTPUT_PREFIX", "parquets_files/"))

    st.subheader("Airflow DB (Postgres)")
    pg_host = st.text_input("PG_HOST", value=env("PG_HOST", "postgres"))
    pg_port = st.number_input("PG_PORT", value=int(env("PG_PORT", "5432")))
    pg_db = st.text_input("PG_DB", value=env("POSTGRES_DB", "airflow"))
    pg_user = st.text_input("PG_USER", value=env("POSTGRES_USER", "airflow"))
    pg_pwd = st.text_input("PG_PASSWORD", value=env("POSTGRES_PASSWORD", "airflow"), type="password")

    st.subheader("DAGs du projet")
    dag_ingestion = st.text_input("DAG ingestion (S3)", value="ingestion_dag")
    dag_convert = st.text_input("DAG conversion -> parquet", value="conversion_to_parquet_dag")
    dag_load_snowflake = st.text_input("DAG parquet -> Snowflake", value="conversion_parquet_to_table_dag")
    dag_dbt = st.text_input("DAG génération modèles dbt", value="transformation_dbt_orchestrated")

    st.divider()
    refresh = st.button("Rafraîchir")

# ──────────────────────────────────────────────────────────────────────────────
# Layout
# ──────────────────────────────────────────────────────────────────────────────

top_left, top_right = st.columns([1.2, 1.0])
bottom_left, bottom_right = st.columns([1.2, 1.0])

# ──────────────────────────────────────────────────────────────────────────────
# S3 Panels
# ──────────────────────────────────────────────────────────────────────────────

with top_left:
    st.subheader("S3 – Stocks de données (RAW vs Parquet)")
    if not s3_bucket:
        st.warning("Renseigne S3_BUCKET.")
    else:
        try:
            raw_df = list_s3_objects(s3_bucket, raw_prefix, s3_region, max_keys=5000)
            pq_df = list_s3_objects(s3_bucket, parquet_prefix, s3_region, max_keys=5000)

            raw_files, raw_bytes, raw_last = kpis_from_s3(raw_df)
            pq_files, pq_bytes, pq_last = kpis_from_s3(pq_df)

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("RAW files", raw_files)
            c2.metric("RAW size", bytes_to_human(raw_bytes))
            c3.metric("RAW latest", str(raw_last) if raw_last else "—")
            c4.metric("Parquet files", pq_files)
            c5.metric("Parquet size", bytes_to_human(pq_bytes))
            c6.metric("Parquet latest", str(pq_last) if pq_last else "—")

            st.caption("Répartition approximative des Parquets par sous-dossier (si présent).")
            breakdown = s3_breakdown_top_folder(pq_df, parquet_prefix)
            if breakdown.empty:
                st.info("Aucun fichier parquet trouvé (ou pas de structure de dossiers).")
            else:
                st.dataframe(breakdown, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Erreur S3: {e}")

with bottom_left:
    st.subheader("S3 – Derniers objets Parquet")
    if not s3_bucket:
        st.warning("Renseigne S3_BUCKET.")
    else:
        try:
            pq_df = list_s3_objects(s3_bucket, parquet_prefix, s3_region, max_keys=2000)
            if pq_df.empty:
                st.info("Aucun objet sous ce préfixe.")
            else:
                # Filtre parquet uniquement
                pq_only = pq_df[pq_df["key"].str.lower().str.endswith(".parquet")].copy()
                st.dataframe(
                    pq_only[["key", "size", "last_modified"]].head(200),
                    use_container_width=True,
                    hide_index=True,
                )
        except Exception as e:
            st.error(f"Erreur S3: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Airflow Panels
# ──────────────────────────────────────────────────────────────────────────────

with top_right:
    st.subheader("Airflow – Santé du pipeline")
    try:
        conn = get_pg_conn(pg_host, pg_port, pg_db, pg_user, pg_pwd)
        runs = airflow_recent_runs(conn, dag_id=None, limit=200)

        # Dernier état par DAG
        st1, rid1 = airflow_last_state(runs, dag_ingestion)
        st2, rid2 = airflow_last_state(runs, dag_convert)
        st3, rid3 = airflow_last_state(runs, dag_load_snowflake)
        st4, rid4 = airflow_last_state(runs, dag_dbt)

        a1, a2 = st.columns(2)
        with a1:
            st.write(f"**{dag_ingestion}** → `{st1}`")
            st.caption(f"run_id: {rid1}")
            st.write(f"**{dag_convert}** → `{st2}`")
            st.caption(f"run_id: {rid2}")
        with a2:
            st.write(f"**{dag_load_snowflake}** → `{st3}`")
            st.caption(f"run_id: {rid3}")
            st.write(f"**{dag_dbt}** → `{st4}`")
            st.caption(f"run_id: {rid4}")

        # Heuristiques simples “health”
        st.divider()
        warnings = []

        # Si ingestion OK mais 0 parquet : suspect conversion
        # (On ne corrèle pas run_id ici, c'est du monitoring simple)
        try:
            pq_df_probe = list_s3_objects(s3_bucket, parquet_prefix, s3_region, max_keys=50) if s3_bucket else pd.DataFrame()
            pq_count = int((pq_df_probe["key"].str.lower().str.endswith(".parquet")).sum()) if not pq_df_probe.empty else 0
        except Exception:
            pq_count = 0

        if st1 == "success" and pq_count == 0:
            warnings.append("Ingestion OK mais aucun Parquet détecté → vérifier conversion_to_parquet_dag / S3_OUTPUT_PREFIX.")
        if st2 == "failed":
            warnings.append("conversion_to_parquet_dag est en échec récemment.")
        if st3 == "failed":
            warnings.append("Chargement Snowflake (conversion_parquet_to_table_dag) est en échec récemment (stage/format/permissions).")
        if st4 == "failed":
            warnings.append("Génération modèles dbt est en échec récemment (warehouse/tables/sources.yml).")

        if warnings:
            for w in warnings:
                st.warning(w)
        else:
            st.success("Aucune alerte majeure détectée (monitoring heuristique).")

    except Exception as e:
        st.error(f"Erreur Airflow DB: {e}")

with bottom_right:
    st.subheader("Airflow – Derniers DAG runs (filtrable)")

    try:
        conn = get_pg_conn(pg_host, pg_port, pg_db, pg_user, pg_pwd)
        runs_df = airflow_recent_runs(conn, dag_id=None, limit=300)

        ok, failed, running = airflow_state_counts(runs_df)
        b1, b2, b3 = st.columns(3)
        b1.metric("Success", ok)
        b2.metric("Failed", failed)
        b3.metric("Running", running)

        # Filtre quick par dag list
        dag_choices = sorted(runs_df["dag_id"].unique().tolist()) if not runs_df.empty else []
        pick = st.multiselect(
            "Filtrer par DAG",
            options=dag_choices,
            default=[d for d in [dag_ingestion, dag_convert, dag_load_snowflake, dag_dbt] if d in dag_choices],
        )

        view = runs_df.copy()
        if pick:
            view = view[view["dag_id"].isin(pick)]

        st.dataframe(
            view[["dag_id", "run_id", "state", "start_date", "end_date", "duration_s"]].head(200),
            use_container_width=True,
            hide_index=True,
        )

    except Exception as e:
        st.error(f"Erreur Airflow DB: {e}")

st.divider()
st.caption(
    "Ce dashboard lit (1) S3 pour la volumétrie et les timestamps (RAW/Parquet) et "
    "(2) la base Postgres d’Airflow pour l’historique d’exécution des DAGs."
)