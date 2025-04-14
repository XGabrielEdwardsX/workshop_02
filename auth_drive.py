# test_auth.py (Versión para usar LocalWebserverAuth)

import os
import logging
from dotenv import load_dotenv
try:
    from dags.tasks.store import auth_drive
    print("Función 'auth_drive' importada correctamente.")
except ImportError as e:
    print(f"Error al importar 'auth_drive': {e}")
    print("\n*** POR FAVOR, EDITA LA LÍNEA 'from ... import auth_drive' EN ESTE SCRIPT (test_auth.py) ***")
    print("Asegúrate de que apunte a la ubicación correcta de tu función 'auth_drive' que usa 'LocalWebserverAuth()'.")
    exit() 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

dotenv_path = "env/.env"
loaded = load_dotenv(dotenv_path)
if not loaded:
     print(f"ADVERTENCIA: No se pudo cargar el archivo .env desde '{dotenv_path}'. Asegúrate de que la ruta es correcta.")
     print("Las variables de entorno (PATHS, FOLDER_ID) deben estar definidas de otra manera.")
else:
    print(f"Archivo .env cargado desde: {dotenv_path}")

settings_file = os.getenv('SETTINGS_PATH')
credentials_file = os.getenv('SAVED_CREDENTIALS_PATH')
client_secrets_file = os.getenv('CLIENT_SECRETS_PATH')

print("-" * 50)
print("Verificando configuración para la autenticación manual (LocalWebserverAuth):")
print(f" - Archivo de configuración (settings.yaml): {settings_file}")
print(f" - Archivo de secretos del cliente (client_secrets.json): {client_secrets_file}")
print(f" - Archivo donde se guardarán las credenciales: {credentials_file}")
print("-" * 50)

if not settings_file or not os.path.exists(settings_file):
    print(f"ERROR: Archivo settings no encontrado en '{settings_file}'. Verifica la variable SETTINGS_PATH en tu .env")
    exit()
if not client_secrets_file or not os.path.exists(client_secrets_file):
     print(f"ERROR: Archivo client_secrets no encontrado en '{client_secrets_file}'. Verifica la variable CLIENT_SECRETS_PATH en tu .env")
     exit()
if not credentials_file:
    print("ERROR: La variable SAVED_CREDENTIALS_PATH no está definida en tu .env")
    exit()

if os.path.exists(credentials_file):
    print(f"\n¡¡¡ ERROR CRÍTICO !!!")
    print(f"El archivo de credenciales guardadas '{credentials_file}' YA EXISTE.")
    print("Para generar un nuevo token de actualización (refresh_token), debes")
    print("ELIMINAR este archivo manualmente ANTES de volver a ejecutar este script.")
    print("Deteniendo el script.")
    print("-" * 50)
else:
    print(f"\nOK: El archivo de credenciales '{credentials_file}' no existe.")
    print("Procediendo a iniciar la autenticación manual interactiva (LocalWebserverAuth)...")
    print("Se abrirá una ventana del navegador para que autorices la aplicación.")
    print("\n*** ¡ASEGÚRATE DE CONCEDER EL PERMISO DE ACCESO OFFLINE CUANDO SE TE PIDA! ***")
    print("(Puede mencionar 'acceso sin conexión', 'ver y administrar...', etc.)")
    print("\n*** ASEGÚRATE de que 'http://localhost:8080' esté en las URIs de redirección autorizadas en Google Cloud Console ***")
    print("-" * 50)

    try:
        drive_instance = auth_drive()

        if drive_instance:
            print("\n" + "=" * 50)
            print("¡AUTENTICACIÓN MANUAL (LocalWebserverAuth) COMPLETADA EXITOSAMENTE!")
            print(f"Se ha creado/actualizado el archivo de credenciales en:")
            print(f"  {credentials_file}")
            print("Este archivo AHORA debería contener un 'refresh_token'.")
            print("\nPróximos pasos:")
            print(" 1. Verifica (opcional) que el archivo JSON contiene una clave 'refresh_token' con un valor (no null).")
            print(" 2. Asegúrate de que este archivo y el 'settings.yaml' estén disponibles para tu worker de Airflow.")
            print(" 3. Ejecuta tu DAG 'spotify_pipeline' en Airflow.")
            print("=" * 50)
        else:
             print("\nERROR INESPERADO: La función auth_drive() retornó None sin lanzar excepción.")

    except Exception as e:
        print("\n" + "!" * 50)
        logging.error("ERROR durante el proceso de autenticación manual (LocalWebserverAuth):", exc_info=True)
        if isinstance(e, OSError) and "Address already in use" in str(e):
            print("\nCAUSA PROBABLE: El puerto 8080 (o el configurado) ya está en uso en tu máquina.")
            print("Puedes intentar detener el proceso que usa ese puerto o configurar un puerto diferente")
            print("en 'settings.yaml' (auth_local_webserver_port) Y añadirlo en Google Cloud Console.")
        elif "No code found in redirect" in str(e):
            print("\nCAUSA PROBABLE: La URI de redirección 'http://localhost:8080' (o el puerto configurado)")
            print("NO está correctamente autorizada en Google Cloud Console para tu Client ID.")
            print("Por favor, verifica y añade la URI correcta en la sección 'Credenciales' de Google Cloud Console.")
        print("!" * 50)