import pandas as pd
from airflow.decorators import task
import os
import logging

@task(task_id="transform_artist_details")
def transform_artist_details(raw_artist_df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = raw_artist_df.copy()
        
        # 1. Conversión de tipos numéricos
        if 'artist_followers' in df.columns:
            df['artist_followers'] = pd.to_numeric(df['artist_followers'], errors='coerce').fillna(0).astype('int32')
        if 'artist_popularity' in df.columns:
            df['artist_popularity'] = pd.to_numeric(df['artist_popularity'], errors='coerce').fillna(0).astype('int32')
        
        # 2. Limpieza de valores nulos
        if 'artist_name' in df.columns:
            df['artist_name'] = df['artist_name'].fillna("Various Artists")
        
        # 3. Normalización de texto
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower().str.strip()
        
        # 4. Guardado del dataset
        output_path = os.path.join('data', 'api_artist.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logging.info(f"Transformación de artistas completada. Dataset guardado en: {output_path}")
        logging.info(f"Forma del DataFrame: {df.shape}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error durante la transformación de artistas: {e}")
        raise