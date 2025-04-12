# dags/tasks/extract_csv.py

import pandas as pd
from airflow.decorators import task
import os


SPOTIFY_CSV_REL_PATH = 'data/spotify_dataset.csv'

@task(task_id="extract_spotify_dataset_from_csv")
def extract_spotify(csv_rel_path: str = SPOTIFY_CSV_REL_PATH) -> pd.DataFrame:
    airflow_home = os.getenv('AIRFLOW_HOME', '.')
    absolute_csv_path = os.path.join(airflow_home, csv_rel_path)

    print(f"Iniciando extracción desde CSV (ruta absoluta): {absolute_csv_path}...")

    try:
        if not os.path.exists(absolute_csv_path):
             raise FileNotFoundError(f"Archivo CSV no encontrado en: {absolute_csv_path}")

        df_spotify = pd.read_csv(absolute_csv_path)
        print(f"Extracción de CSV completada. {len(df_spotify)} filas leídas.")

        if df_spotify.empty:
            print(f"Advertencia: El archivo CSV '{absolute_csv_path}' parece estar vacío.")

        return df_spotify

    except Exception as e:
        print(f"Error durante la extracción del CSV: {e}")
        raise