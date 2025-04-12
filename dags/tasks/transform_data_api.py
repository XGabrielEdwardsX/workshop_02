import pandas as pd
from airflow.decorators import task
import os



@task(task_id="transform_artist_details")
def transform_artist_details(raw_artist_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_artist_df.copy() 


    if 'artist_followers' in df.columns:
        df['artist_followers'] = pd.to_numeric(df['artist_followers'], errors='coerce').fillna(0).astype('int32')
    if 'artist_popularity' in df.columns:
         df['artist_popularity'] = pd.to_numeric(df['artist_popularity'], errors='coerce').fillna(0).astype('int32')


    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip() 

    return df