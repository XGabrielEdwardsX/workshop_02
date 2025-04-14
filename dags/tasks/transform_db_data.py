# dags/tasks/transform_db_data.py

import pandas as pd
from airflow.decorators import task
import logging
import os

INPUT_CSV_REL_PATH = 'data/grammys.csv' # Ruta relativa donde se espera encontrar el CSV

@task(task_id="transform_grammys_data")
def transform_grammys_data(grammys_csv_rel_path: str = INPUT_CSV_REL_PATH) -> pd.DataFrame:
    """
    Lee el archivo CSV de Grammys, realiza transformaciones
    (elimina columnas, normaliza texto) y retorna el DataFrame transformado.
    """
    logging.info(f"Iniciando transformación de datos de Grammys desde CSV...")

    airflow_home = os.getenv('AIRFLOW_HOME', '.')
    absolute_input_path = os.path.join(airflow_home, grammys_csv_rel_path)
    logging.info(f"Intentando leer el archivo CSV desde: {absolute_input_path}")

    try:
        # --- NUEVO: Leer desde CSV ---
        if not os.path.exists(absolute_input_path):
            logging.error(f"Archivo CSV de Grammys no encontrado en: {absolute_input_path}")
            raise FileNotFoundError(f"No se encontró el archivo esperado: {absolute_input_path}")

        df = pd.read_csv(absolute_input_path)
        logging.info(f"Archivo CSV de Grammys leído exitosamente. {len(df)} filas encontradas.")
        # --- FIN NUEVO ---

        if df.empty:
            logging.warning("El archivo CSV de Grammys está vacío. Se procederá con las transformaciones si es posible.")
            # No hay necesidad de salir, las transformaciones podrían manejar un DF vacío.

        # --- Transformaciones (lógica original mantenida) ---
        logging.info("Aplicando transformaciones...")
        cols_to_drop = ['winner', 'workers', 'img', 'published_at', 'title']
        existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]

        if existing_cols_to_drop:
            df = df.drop(columns=existing_cols_to_drop)
            logging.info(f"Columnas eliminadas: {existing_cols_to_drop}")
        else:
            logging.info("No se encontraron columnas de la lista 'cols_to_drop' para eliminar.")

        text_columns = ['category', 'nominee', 'artist']
        logging.info(f"Normalizando texto (minúsculas, strip) en columnas: {text_columns}")
        for col in text_columns:
            if col in df.columns:
                # Convertir a string primero para manejar posibles no-strings, luego normalizar
                df[col] = df[col].astype(str).str.lower().str.strip()
                # Manejar valores que originalmente eran NaN y ahora son 'nan' como string
                df[col] = df[col].replace('nan', pd.NA)
            else:
                logging.warning(f"Columna '{col}' no encontrada en el DataFrame para normalización.")

        logging.info("Transformación de datos de Grammys completada.")
        logging.info(f"DataFrame final con {len(df)} filas y columnas: {df.columns.tolist()}")

        return df # Retorna el DataFrame transformado para la tarea de merge

    except FileNotFoundError as e:
        logging.error(f"Error: {e}")
        raise # Relanzar para que Airflow marque la tarea como fallida
    except Exception as e:
        logging.error(f"Error durante la transformación de datos de Grammys: {e}", exc_info=True)
        raise

# --- Fin de transform_db_data.py ---