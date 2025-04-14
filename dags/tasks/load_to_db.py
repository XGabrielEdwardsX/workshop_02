# dags/tasks/load_to_db.py

import pandas as pd
import os
import logging
import sqlalchemy
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

TARGET_TABLE_NAME = 'spotify_merged_data'
TARGET_SCHEMA_NAME = 'public'

@task(task_id="load_merged_data_to_db")
def load_to_db(df_to_load: pd.DataFrame,
               table_name: str = TARGET_TABLE_NAME,
               schema_name: str = TARGET_SCHEMA_NAME,
               if_exists: str = 'replace'):
    
    logging.info(f"Iniciando carga a Base de Datos: Esquema='{schema_name}', Tabla='{table_name}', Si Existe='{if_exists}'")

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    required_vars = {'DB_USER': db_user, 'DB_PASSWORD': db_password, 'DB_HOST': db_host, 'DB_NAME': db_name}
    missing_vars = [k for k, v in required_vars.items() if v is None]
    if missing_vars:
        error_msg = f"Faltan variables de entorno requeridas para la BD: {', '.join(missing_vars)}"
        logging.error(error_msg)
        raise AirflowFailException(error_msg)

    if df_to_load.empty:
        logging.warning(f"El DataFrame de entrada está vacío. Omitiendo carga a la tabla '{schema_name}.{table_name}'.")
        return

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = None 

    try:
        logging.info(f"Conectando a la base de datos: postgresql://{db_user}:***@{db_host}:{db_port}/{db_name}")

        engine = sqlalchemy.create_engine(db_url, echo=False)

        logging.info(f"Cargando {len(df_to_load)} filas en la tabla '{schema_name}.{table_name}'...")

        df_to_load.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists=if_exists,
            index=False,       
            method='multi'     
        )

        logging.info(f"Datos cargados exitosamente en '{schema_name}.{table_name}'.")

    except Exception as e:
        logging.error(f"Error durante la carga a la base de datos: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logging.info("Motor de base de datos desechado (conexiones cerradas).")