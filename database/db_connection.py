import psycopg2
from dotenv import load_dotenv
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, '..', 'env', '.env')

if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f".env file not found at {ENV_PATH}")


load_dotenv(ENV_PATH)

def get_connection():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_host, db_port, db_name, db_user, db_password]):
        raise ValueError("Missing environment variables for database connection")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        raise