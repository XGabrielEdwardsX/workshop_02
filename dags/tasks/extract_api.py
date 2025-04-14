# dags/tasks/extract_api.py
import pandas as pd
from airflow.decorators import task
import os
import logging 

ARTIST_DETAILS_CSV_REL_PATH = 'data/api_artist.csv'
@task(task_id="extract_artist_details") 
def extract_artist(artist_details_csv_rel_path: str = ARTIST_DETAILS_CSV_REL_PATH) -> pd.DataFrame:

    airflow_home = os.getenv('AIRFLOW_HOME', '.')
    absolute_csv_path = os.path.join(airflow_home, artist_details_csv_rel_path)

    logging.info(f"Leyendo detalles de artistas pre-extraídos desde: {absolute_csv_path}")

    try:
        if not os.path.exists(absolute_csv_path):
             logging.error(f"Archivo CSV de detalles de artista no encontrado en: {absolute_csv_path}")
             raise FileNotFoundError(f"Archivo no encontrado: {absolute_csv_path}")

        # Leer el CSV
        df_artists = pd.read_csv(absolute_csv_path)
        logging.info(f"Lectura de CSV de detalles de artista completada. {len(df_artists)} filas leídas.")

        if df_artists.empty:
            logging.warning(f"El archivo CSV '{absolute_csv_path}' está vacío.")
        
        expected_cols = ['artist_id', 'artist_name', 'followers', 'popularity', 'genres']
        if not all(col in df_artists.columns for col in expected_cols):
            logging.warning(f"Al CSV {absolute_csv_path} le faltan columnas esperadas. Columnas encontradas: {df_artists.columns.tolist()}")

        return df_artists

    except Exception as e:
        logging.error(f"Error durante la lectura del CSV de detalles de artista: {e}")
        raise