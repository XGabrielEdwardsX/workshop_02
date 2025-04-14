# dags/tasks/extract_grammys_db.py

import pandas as pd
import sqlalchemy as sa
from airflow.decorators import task
import sys
import os
import logging

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if module_path not in sys.path:
    print(f"Añadiendo al PYTHONPATH para importación: {module_path}")
    sys.path.append(module_path)
else:
    print(f"Ruta {module_path} ya está en PYTHONPATH.")

try:
    from database.db_connection import get_connection
    print("Importación de get_connection exitosa.")
except ImportError as e:
     print(f"Error importando get_connection: {e}")
     print("Verifica que la carpeta 'database' y 'db_connection.py' existan en la raíz del proyecto.")
     raise

TABLE_NAME = 'grammy_awards'
SCHEMA_NAME = 'grammys'
DATABASE_URL = "postgresql://" 
OUTPUT_CSV_REL_PATH = 'data/grammys.csv'

@task(task_id="extract_grammys")
def extract_grammys(schema: str = SCHEMA_NAME, table: str = TABLE_NAME, output_rel_path: str = OUTPUT_CSV_REL_PATH) -> str:
    """
    Extrae datos de la tabla de Grammys y los guarda en un archivo CSV.
    Retorna la ruta absoluta del archivo CSV creado.
    """
    logging.info(f"Iniciando extracción de datos de la tabla '{schema}.{table}'...")
    engine = None
    try:
        engine = sa.create_engine(DATABASE_URL, creator=get_connection, echo=False)
        logging.info("Conexión a la base de datos establecida via SQLAlchemy engine.")

        df_grammys = pd.read_sql_table(table, engine, schema=schema)

        if df_grammys.empty:
            logging.warning(f"Advertencia: No se encontraron datos en la tabla '{schema}.{table}'.")
        else:
            logging.info(f"Extracción de datos de Grammys completada. {len(df_grammys)} registros obtenidos.")

        airflow_home = os.getenv('AIRFLOW_HOME', '.')
        absolute_output_path = os.path.join(airflow_home, output_rel_path)
        output_dir = os.path.dirname(absolute_output_path)

        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Asegurando que el directorio de salida exista: {output_dir}")

        df_grammys.to_csv(absolute_output_path, index=False)
        logging.info(f"Datos de Grammys guardados exitosamente en: {absolute_output_path}")

        return absolute_output_path

    except ImportError:
         logging.error("Error crítico: No se pudo importar la función get_connection.")
         raise
    except Exception as e:
        logging.error(f"Error durante la extracción de datos de la base de datos: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logging.info("Engine de SQLAlchemy desechado.")