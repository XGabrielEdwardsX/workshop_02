# dags/tasks/transform_csv_data.py

import pandas as pd
from airflow.decorators import task
import logging
import os

GENRE_CATEGORIES = {
    'Rock': ['alt-rock', 'alternative', 'emo', 'goth', 'grunge', 'hard-rock', 'indie', 'punk-rock', 'punk', 'psych-rock', 'rock', 'rock-n-roll', 'rockabilly'],
    'Pop': ['cantopop', 'indie-pop', 'j-pop', 'k-pop', 'mandopop', 'pop', 'pop-film', 'power-pop', 'synth-pop'],
    'Electronic': ['breakbeat', 'chicago-house', 'club', 'dance', 'deep-house', 'detroit-techno', 'disco', 'drum-and-bass', 'dub', 'dubstep', 'edm', 'electro', 'electronic', 'garage', 'hardstyle', 'house', 'idm', 'minimal-techno', 'progressive-house', 'techno', 'trance', 'trip-hop'],
    'Hip-Hop/R&B': ['hip-hop', 'r-n-b', 'soul'],
    'Metal': ['black-metal', 'death-metal', 'grindcore', 'heavy-metal', 'metal', 'metalcore'],
    'Folk/Acoustic': ['acoustic', 'bluegrass', 'folk', 'singer-songwriter'],
    'Jazz/Blues': ['blues', 'jazz'],
    'Classical/Instrumental': ['classical', 'new-age', 'opera', 'piano', 'sleep', 'study'],
    'Latin': ['forro', 'latin', 'latino', 'mpb', 'pagode', 'salsa', 'samba', 'sertanejo', 'tango'],
    'Reggae/Dancehall': ['dancehall', 'reggae', 'reggaeton'],
    'Country': ['country', 'honky-tonk'],
    'World/Regional': ['afrobeat', 'brazil', 'french', 'german', 'indian', 'iranian', 'malay', 'spanish', 'swedish', 'turkish', 'world-music'],
    'Anime/Japanese': ['anime', 'j-dance', 'j-idol', 'j-rock'],
    'Kids/Comedy': ['children', 'comedy', 'disney', 'kids', 'show-tunes'],
    'Other': ['ambient', 'british', 'chill', 'funk', 'gospel', 'groove', 'guitar', 'happy', 'hardcore', 'party', 'romance', 'sad', 'ska']
}

@task(task_id="transform_spotify_data")
def transform_spotify_data(raw_spotify_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de Spotify: limpia datos, normaliza strings,
    categoriza géneros, convierte duración y elimina columnas innecesarias.
    """
    logging.info("Iniciando transformación del dataset de Spotify...")
    try:
        df = raw_spotify_df.copy()
        logging.info(f"DataFrame inicial con {len(df)} filas y columnas: {df.columns.tolist()}")

        df = df.drop_duplicates(subset=['track_id'])
        logging.info(f"Filas después de eliminar duplicados por track_id: {len(df)}")
        
        cols_to_check_na = ['artists', 'album_name', 'track_name']
        initial_rows = len(df)
        df = df.dropna(subset=[col for col in cols_to_check_na if col in df.columns])
        rows_dropped = initial_rows - len(df)
        if rows_dropped > 0:
             logging.info(f"Se eliminaron {rows_dropped} filas con valores nulos en {cols_to_check_na}. Filas restantes: {len(df)}")
        else:
             logging.info(f"No se encontraron filas con valores nulos en {cols_to_check_na}.")

        logging.info("Normalizando columnas de texto (minúsculas y sin espacios extra)...")
        object_cols = df.select_dtypes(include=['object']).columns
        logging.info(f"Columnas tipo 'object' a normalizar: {object_cols.tolist()}")
        for col in object_cols:
            if col in df.columns:
                 try:
                      df[col] = df[col].astype(str).str.lower().str.strip()
                 except Exception as e:
                      logging.warning(f"No se pudo normalizar la columna '{col}': {e}")
            else:
                 logging.warning(f"La columna '{col}' ya no existe en el DataFrame antes de la normalización.")
        logging.info("Normalización de texto completada.")

        if 'track_genre' in df.columns:
            logging.info("Categorizando géneros...")
            genre_to_category = {genre.lower().strip(): category for category, genres in GENRE_CATEGORIES.items() for genre in genres}
            df['genre_category'] = df['track_genre'].map(genre_to_category).fillna('Other')
            logging.info(f"Categorías de género asignadas: {df['genre_category'].unique().tolist()}")
            df = df.drop(columns=['track_genre'])
            logging.info("Columna 'track_genre' eliminada.")
        else:
            logging.warning("La columna 'track_genre' no se encontró para categorización.")

        if 'duration_ms' in df.columns:
            logging.info("Convirtiendo duración de ms a minutos...")
            df['duration_min'] = (df['duration_ms'] / 60000).round(2)

            df = df.drop(columns=['duration_ms'])
            logging.info("Columna 'duration_ms' eliminada y 'duration_min' creada.")
        elif 'duration_min' not in df.columns:
            logging.warning("No se encontró la columna 'duration_ms' para convertir a minutos.")
        else:
            logging.info("La columna 'duration_min' ya existe.")

        cols_to_drop = [
            'Unnamed: 0', 'key', 'mode', 'speechiness',
            'liveness', 'time_signature', 'loudness',
            'acousticness', 'instrumentalness', 'valence',
            'tempo'
        ]
        cols_actually_dropped = [col for col in cols_to_drop if col in df.columns]
        if cols_actually_dropped:
             df = df.drop(columns=cols_actually_dropped, errors='ignore')
             logging.info(f"Columnas eliminadas: {cols_actually_dropped}")
        else:
             logging.info("No se encontraron columnas adicionales para eliminar de la lista predefinida.")

        airflow_home = os.getenv('AIRFLOW_HOME', '.')
        output_dir = os.path.join(airflow_home, 'data', 'processed')
        output_path = os.path.join(output_dir, 'spotify_dataset_cleaned.csv')
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Asegurando que el directorio de salida exista: {output_dir}")

        df.to_csv(output_path, index=False)
        logging.info(f"Transformación completada. Dataset guardado en: {output_path}")
        logging.info(f"Dataset final con {len(df)} filas.")
        logging.info(f"Columnas finales: {df.columns.tolist()}")

        return df

    except Exception as e:
        logging.error(f"Error durante la transformación de datos de Spotify: {e}", exc_info=True)
        raise