import os
from datetime import datetime
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


def airflow_task_durations(conn, dag_id: str, run_id: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT task_id, state, start_date, end_date
            FROM task_instance
            WHERE dag_id = %s AND run_id = %s
            ORDER BY start_date ASC NULLS LAST
            """,
            (dag_id, run_id),
        )
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["task_id", "state", "start_date", "end_date"])
    if not df.empty:
        df["duration_s"] = (df["end_date"] - df["start_date"]).dt.total_seconds()
    return df


def airflow_run_durations_by_dag(conn, dag_id: str, limit: int = 50) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, state, start_date, end_date
            FROM dag_run
            WHERE dag_id = %s
            ORDER BY start_date DESC NULLS LAST
            LIMIT %s
            """,
            (dag_id, limit),
        )
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["run_id", "state", "start_date", "end_date"])
    if not df.empty:
        df["duration_s"] = (df["end_date"] - df["start_date"]).dt.total_seconds()
    return df

from pathlib import Path

def tail_text_file(path: str, n: int = 200) -> str:
    try:
        p = Path(path)
        if not p.exists():
            return f"[missing] {path}"
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"[error reading {path}] {e}"


def list_files_info(folder: str, glob_pattern: str = "*.sql") -> pd.DataFrame:
    p = Path(folder)
    if not p.exists():
        return pd.DataFrame(columns=["file", "mtime"])
    rows = []
    for f in p.glob(glob_pattern):
        rows.append({"file": str(f.name), "mtime": datetime.fromtimestamp(f.stat().st_mtime)})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("mtime", ascending=False)
    return df


def snowflake_probe_tables() -> pd.DataFrame:
    try:
        import snowflake.connector  # lazy import
    except Exception as e:
        raise RuntimeError(f"snowflake connector not available: {e}")

    conx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )

    try:
        with conx.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, row_count, bytes, created, last_altered
                FROM information_schema.tables
                WHERE table_schema = CURRENT_SCHEMA()
                ORDER BY last_altered DESC
                LIMIT 50
                """
            )
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["table_name", "row_count", "bytes", "created", "last_altered"])
    finally:
        conx.close()    

# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────

st.title("OpenDataHub SQL – Dashboard de supervision")

with st.sidebar:
    st.header("Configuration")

    st.subheader("S3")
    s3_bucket = st.text_input("S3_BUCKET", value=env("S3_BUCKET", ""))
    s3_region = st.text_input("AWS_REGION", value=env("AWS_DEFAULT_REGION", env("AWS_REGION", "eu-north-1")))
    raw_prefix = st.text_input("Fichiers Raw (mettre préfixe de format)")
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

            c1, c2 = st.columns(2)
            c1.metric("RAW files", raw_files)
            c2.metric("RAW size", bytes_to_human(raw_bytes))
            c3 = st.columns(1)[0]
            c3.metric("RAW latest", str(raw_last)[:-6]  if raw_last else "—")
            c4, c5 = st.columns(2)
            c4.metric("Parquet files", pq_files)
            c5.metric("Parquet size", bytes_to_human(pq_bytes))
            c6 = st.columns(1)[0]
            c6.metric("Parquet latest", str(pq_last)[:-6] if pq_last else "—")


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

        a1, a2, a3, a4 = st.columns(4)
        a1 = st.write(f"**{dag_ingestion}** → `{st1}`")
        a2 =st.write(f"**{dag_convert}** → `{st2}`")
        a3 = st.write(f"**{dag_load_snowflake}** → `{st3}`")
        a4 =st.write(f"**{dag_dbt}** → `{st4}`")
            
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
            st.success("Aucune alerte majeure détectée.")

    except Exception as e:
        st.error(f"Erreur Airflow DB: {e}")

with bottom_right:
    st.subheader("Airflow – Derniers DAG runs")

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
st.subheader("Airflow – Performance (dernier run)")
dag_perf = st.selectbox("DAG à analyser", [dag_ingestion, dag_convert, dag_load_snowflake, dag_dbt])
state_last, run_last = airflow_last_state(runs, dag_perf)
if run_last not in ("—", "", None):
    tasks_df = airflow_task_durations(conn, dag_perf, run_last)
    if tasks_df.empty:
        st.info("Aucune task_instance trouvée pour ce run.")
    else:
        total = tasks_df["duration_s"].sum(skipna=True)
        slowest = tasks_df.sort_values("duration_s", ascending=False).head(1)

        p1, p2 = st.columns(2)
        p1.metric("Run total (s)", f"{total:.1f}")
        if not slowest.empty and pd.notna(slowest.iloc[0]["duration_s"]):
            p2.metric("Task la plus lente", f'{slowest.iloc[0]["task_id"]} ({slowest.iloc[0]["duration_s"]:.1f}s)')

        st.dataframe(
            tasks_df[["task_id", "state", "duration_s"]].sort_values("duration_s", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("Pas de run_id disponible pour ce DAG.")


st.divider()
st.subheader("Warehouse & dbt – Observations")


project_root = "/opt/airflow/project"
dbt_root = f"{project_root}/transformation/transforme_with_dbt/dbt"

source_yml = f"{dbt_root}/models/source/source.yml"
bronze_dir = f"{dbt_root}/models/bronze"
silver_dir = f"{dbt_root}/models/silver"
dbt_log = f"{dbt_root}/logs/dbt.log"  # si présent

    # KPIs "artifacts exist"
c1, c2, c3 = st.columns(3)
c1.metric("source.yml exists", "yes" if Path(source_yml).exists() else "no")
c2.metric("bronze models", int(len(list(Path(bronze_dir).glob('*.sql')))) if Path(bronze_dir).exists() else 0)
c3.metric("silver models", int(len(list(Path(silver_dir).glob('*.sql')))) if Path(silver_dir).exists() else 0)

st.caption("Derniers fichiers générés (Bronze / Silver):")

bdf = list_files_info(bronze_dir, "*.sql")
sdf = list_files_info(silver_dir, "*.sql")

left, right = st.columns(2)
with left:
    st.write("**Bronze**")
    if bdf.empty:
        st.info("No bronze .sql files found.")
    else:
        st.dataframe(bdf.head(30), use_container_width=True, hide_index=True)

with right:
    st.write("**Silver**")
    if sdf.empty:
        st.info("No silver .sql files found.")
    else:
        st.dataframe(sdf.head(30), use_container_width=True, hide_index=True)

st.caption("Extrait dbt.log (si disponible):")
st.code(tail_text_file(dbt_log, n=120), language="text")

st.divider()
st.caption(
    "Ce dashboard lit (1) S3 pour la volumétrie et les timestamps (RAW/Parquet) et "
    "(2) la base Postgres d’Airflow pour l’historique d’exécution des DAGs."
)

