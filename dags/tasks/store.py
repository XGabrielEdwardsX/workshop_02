from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv
import os
import pandas as pd
import logging
import json
from typing import Union
from airflow.decorators import task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

load_dotenv("env/.env")

client_secrets_file = os.getenv('CLIENT_SECRETS_PATH')
settings_file = os.getenv('SETTINGS_PATH')
credentials_file = os.getenv('SAVED_CREDENTIALS_PATH')
folder_id = os.getenv("FOLDER_ID")

def auth_drive():
    """
    Authenticates and returns a Google Drive instance using the PyDrive library.
    """
    try:
        logging.info("Starting Google Drive authentication process.")

        logging.info(f"Client secrets path: {client_secrets_file}")
        logging.info(f"Settings file path: {settings_file}")
        logging.info(f"Credentials file path: {credentials_file}")
        logging.info(f"Folder ID: {folder_id}")

        if not os.path.exists(client_secrets_file):
            raise FileNotFoundError(f"Client secrets file not found at {client_secrets_file}")
        if not os.path.exists(settings_file):
            raise FileNotFoundError(f"Settings file not found at {settings_file}")

        os.makedirs(os.path.dirname(credentials_file), exist_ok=True)

        gauth = GoogleAuth(settings_file=settings_file)

        if os.path.exists(credentials_file):
            logging.info("Loading saved credentials.")
            gauth.LoadCredentialsFile(credentials_file)
            if gauth.access_token_expired:
                logging.info("Access token expired, refreshing token.")
                gauth.Refresh()
                gauth.SaveCredentialsFile(credentials_file)
                logging.info("Token refreshed and saved.")
            else:
                logging.info("Using saved credentials.")
        else:
            logging.info("Saved credentials not found, performing web authentication.")
            gauth.LoadClientConfigFile(client_secrets_file)
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(credentials_file)
            logging.info("Local webserver authentication completed and credentials saved successfully.")

        drive = GoogleDrive(gauth)
        logging.info("Google Drive authentication completed successfully.")
        return drive

    except Exception as e:
        logging.error(f"Authentication error: {e}", exc_info=True)
        raise

@task
def store_merged_data(title: str, df: Union[pd.DataFrame, str, dict]) -> None:
    """
    Stores a given DataFrame as a CSV file on Google Drive.

    Parameters:
        title (str): The title of the file to be stored on Google Drive.
        df (Union[pd.DataFrame, str, dict]): The DataFrame to be stored as a CSV file.
            Can be a DataFrame, JSON string, or dictionary (from XCom deserialization).

    Returns:
        None

    Raises:
        ValueError: If the input DataFrame is empty or if title is empty.
        Exception: If there is an error during the upload process.
    """
    try:
        # Handle different input types from XCom
        if isinstance(df, str):
            try:
                # Try to parse as JSON string
                df = pd.DataFrame(json.loads(df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided: {str(e)}")
                raise
        elif isinstance(df, dict):
            # Handle dictionary (common XCom serialization format)
            df = pd.DataFrame(df)
        elif not isinstance(df, pd.DataFrame):
            raise ValueError(f"Unsupported input type: {type(df)}. Expected DataFrame, JSON string, or dict.")

        # Validate DataFrame
        if df.empty:
            raise ValueError("Input DataFrame is empty")
        if not title or not isinstance(title, str):
            raise ValueError("Invalid title provided")

        # Authenticate Google Drive
        drive = auth_drive()

        logging.info(f"Storing {title} on Google Drive.")
        logging.info(f"DataFrame has {len(df)} rows and {len(df.columns)} columns.")

        # Convert DataFrame to CSV string
        csv_file = df.to_csv(index=False)

        # Create and upload file to Google Drive
        file = drive.CreateFile({
            "title": title,
            "parents": [{"kind": "drive#fileLink", "id": folder_id}],
            "mimeType": "text/csv"
        })

        file.SetContentString(csv_file)
        file.Upload()

        logging.info(f"File {title} uploaded successfully.")

    except Exception as e:
        logging.error(f"Error storing data on Google Drive: {e}", exc_info=True)
        raise