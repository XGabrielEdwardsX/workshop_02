# Workshop 02 - Proceso ETL con Airflow

**Autor:** Gabriel Eduardo Martinez Martinez

Este proyecto implementa un proceso ETL (Extract, Transform, Load) orquestado con **Airflow**. El objetivo es integrar datos de tres fuentes distintas:

- Un archivo CSV local (`spotify_dataset.csv`).
- Datos de premios Grammy desde una base de datos **PostgreSQL** (originados de `the_grammy_awards.csv`).
- Datos adicionales obtenidos mediante la **API de Spotify**.

Finalmente, la información consolidada y relevante se almacena en un único archivo CSV, que se sube automáticamente a **Google Drive**.

---

## 🛠 Herramientas Principales

- **Sistema Operativo:** Linux (recomendado)
- **Lenguaje:** Python
- **Orquestación:** Apache Airflow
- **Base de Datos:** PostgreSQL
- **Visualización:** PowerBI
- **Adicionales:** DBeaver, Google Cloud Console

---

## 📂 Estructura del Proyecto
Aquí tienes la estructura de directorios convertida a una tabla en formato Markdown:

| Ruta | Tipo | Descripción |
|------|------|-------------|
| `airflow.cfg` | Archivo | Configuración de Airflow |
| `airflow.db` | Archivo | Base de datos de Airflow |
| `dags/` | Directorio | Contiene los DAGs de Airflow |
| `dags/spotify_pipeline_dag.py` | Archivo | DAG para el pipeline de Spotify |
| `dags/tasks/` | Directorio | Tareas relacionadas con los DAGs |
| `data/` | Directorio | Almacena datasets |
| `data/grammys.csv` | Archivo | Dataset de los premios Grammy |
| `data/merge_dataset.csv` | Archivo | Dataset combinado |
| `data/processed/` | Directorio | Datos procesados |
| `data/spotify_dataset.csv` | Archivo | Dataset de Spotify |
| `data/the_grammy_awards.csv` | Archivo | Dataset de los Grammy Awards |
| `database/` | Directorio | Configuración de la base de datos |
| `database/db_connection.py` | Archivo | Script para conexión a la base de datos |
| `drive_config/` | Directorio | Contendrá credenciales de Google Drive |
| `env/` | Directorio | Archivos de configuración de entorno (Sensibles) |
| `notebooks/` | Directorio | Notebooks de Jupyter |
| `notebooks/api_spotify_003.ipynb` | Archivo | Notebook para API de Spotify |
| `notebooks/data/` | Directorio | Datos usados en notebooks |
| `notebooks/grammys_eda_002.ipynb` | Archivo | Análisis exploratorio de datos de Grammy |
| `notebooks/merge.ipynb` | Archivo | Notebook para combinar datasets |
| `notebooks/spotify_eda_001.ipynb` | Archivo | Análisis exploratorio de datos de Spotify |
| `pdf_info/` | Directorio | Información en formato PDF |
| `pdf_info/guide.pdf` | Archivo | Guía en formato PDF |
| `README.md` | Archivo | Documentación del proyecto (Tú estas aquí) |
| `requirements.txt` | Archivo | Dependencias del proyecto |



> **Nota Importante:** Las carpetas `env/` y `drive_config/` no están incluidas directamente en el repositorio por seguridad. Deberás crearlas y configurar tus propias credenciales.

---

## 🚀 Configuración Inicial

### 1. Clonar el Repositorio

```
git clone https://github.com/XGabrielEdwardsX/workshop_02
cd workshop_02
```

### 2.  Preparar el Entorno (Linux con Python 3.11)

#### Actualizar e instalar dependencias de Python
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.11 python3.11-venv

####  Crear y activar el entorno virtual
python3.11 -m venv venv_workshop
source venv_workshop/bin/activate

####  Instalar dependencias del proyecto
pip install -r requirements.txt

### 3. Configuración de Credenciales
a.
Crea las siguientes carpetas en la raíz del proyecto:

+ env

+ drive_config

b.

+ Crea un archivo llamado .env dentro de la carpeta env/ con el siguiente contenido (reemplaza con tus valores):
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_tu_db
DB_USER=tu_usuario_db
DB_PASSWORD=tu_contraseña_db

SPOTIFY_CLIENT_ID=tu_spotify_client_id
SPOTIFY_CLIENT_SECRET=tu_spotify_client_secret

CLIENT_SECRETS_PATH="drive_config/client_secrets.json"
SETTINGS_PATH="env/settings.yaml"
SAVED_CREDENTIALS_PATH="drive_config/saved_credentials.json"
FOLDER_ID="tu_google_drive_folder_id"
```

c. 
+ Crea un archivo llamado settings.yaml dentro de env/ con el siguiente contenido (reemplaza los valores de Google):
```
client_config_backend: file
client_config:
  client_id: tu_google_client_id
  client_secret: tu_google_client_secret
  redirect_uris: ["http://localhost:8090/"]
  auth_uri: https://accounts.google.com/o/oauth2/auth
  token_uri: https://accounts.google.com/o/oauth2/token

save_credentials: true
save_credentials_backend: file
save_credentials_file: drive_config/saved_credentials.json

get_refresh_token: true

oauth_scope:
  - https://www.googleapis.com/auth/drive
  ```

d. En **drive_config:**
 Credenciales de Google (client_secrets.json)
+ Ve a Google Cloud Console

+ Crea o selecciona un proyecto.

+ Habilita la API de Google Drive.

+ Crea credenciales tipo ID de cliente OAuth.

+ Elige "Aplicación web".

+ Descarga el archivo JSON y renómbralo como: **drive_config/client_secrets.json**

+ Luego ejecuta el archivo **auth_drive** para que Google Drive acepte tu autorización

### 🏃 Ejecución del Proyecto
Extracción Inicial de Spotify:

**Importante: Antes de activar el DAG en Airflow, ejecuta el notebook notebooks/api_spotify_003.ipynb.
Este notebook extrae los datos de la API de Spotify y los guarda localmente para evitar pasarnos con el número de peticiones.**

+ Activar y Ejecutar el DAG en Airflow:

+ Abre Airflow en http://localhost:8080

+ Busca el DAG spotify_pipeline_dag

+ Actívalo usando el interruptor

+ Ejecuta manualmente con el botón "Play" o espera su ejecución programada

+ Verificar Resultados:

**Google Drive:** Revisa la carpeta de Google Drive especificada por FOLDER_ID. Debería aparecer merge_dataset.csv.

**Base de Datos:** Conéctate con DBeaver o psql para verificar que las tablas (como grammys) estén pobladas.

**Logs de Airflow:** Revisa los logs por tarea desde la interfaz web para confirmar la correcta ejecución.

### 📊 Análisis y Visualización
Los notebooks en notebooks/ contienen análisis detallados (EDA):

* spotify_eda_001.ipynb: Análisis del dataset de Spotify.

* grammys_eda_002.ipynb: Análisis de premios Grammy.

* api_spotify_003.ipynb: Análisis de premios Grammy.

* merge_004.ipynb: Unión de datasets.

El dashboard final de PowerBI está en la carpeta dashboard/

