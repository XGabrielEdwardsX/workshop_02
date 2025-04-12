# dags/tasks/extract_grammys_db.py

import pandas as pd
import sqlalchemy as sa
from airflow.decorators import task
import sys
import os
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

@task(task_id="extract_grammys")
def extract_grammys(schema: str = SCHEMA_NAME, table: str = TABLE_NAME) -> pd.DataFrame:
    print(f"Iniciando extracción de datos de la tabla '{schema}.{table}'...")
    try:
        engine = sa.create_engine(DATABASE_URL, creator=get_connection, echo=False)
        with engine.connect() as connection:
             print("Conexión a la base de datos establecida via SQLAlchemy engine.")

        df_grammys = pd.read_sql_table(table, engine, schema=schema)

        if df_grammys.empty:
            print(f"Advertencia: No se encontraron datos en la tabla '{schema}.{table}'.")
        else:
             print(f"Extracción de datos de Grammys completada. {len(df_grammys)} registros obtenidos.")

        engine.dispose()
        print("Engine de SQLAlchemy desechado.")
        return df_grammys

    except ImportError:
         print("Error crítico: No se pudo importar la función get_connection.")
         raise
    except Exception as e:
        print(f"Error durante la extracción de datos de la base de datos: {e}")
        if 'engine' in locals() and engine: engine.dispose()
        raise

# --- Fin de extract_grammys_db.py ---