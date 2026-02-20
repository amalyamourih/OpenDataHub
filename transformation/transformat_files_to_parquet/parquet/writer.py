import io

def dataframe_to_parquet_bytes(df):
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.getvalue()
