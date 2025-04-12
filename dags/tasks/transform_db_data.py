import pandas as pd
from airflow.decorators import task

@task(task_id="transform_grammys_data")
def transform_grammys_data(raw_grammys_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_grammys_df.copy()

    cols_to_drop = ['winner', 'workers', 'img', 'published_at', 'title']
    existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]

    if existing_cols_to_drop:
        df = df.drop(columns=existing_cols_to_drop)

    text_columns = ['category', 'nominee', 'artist']

    for col in text_columns:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.lower().str.strip()
            else:
                 df[col] = df[col].astype(str).str.lower().str.strip()
    return df