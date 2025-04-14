# dags/spotify_pipeline_dag.py
from __future__ import annotations
import pendulum
import logging
import os
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

try:
    from tasks.extract_api import extract_artist
    from tasks.extract_grammys_db import extract_grammys
    from tasks.extract_csv import extract_spotify
    from tasks.transform_data_api import transform_artist_details
    from tasks.transform_db_data import transform_grammys_data
    from tasks.transform_csv_data import transform_spotify_data
    from tasks.merge_data import merge
    from tasks.store import store_merged_data
    from tasks.load_to_db import load_to_db
except ImportError as e:
    logging.error(f"Error importing tasks: {e}")
    raise

@dag(
    dag_id='spotify_pipeline',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spotify', 'etl', 'merge', 'gdrive', 'database', 'final']
)
def spotify_pipeline_dag():
    extracted_artists_df = extract_artist()
    extracted_grammys_df = extract_grammys()
    extracted_spotify_df = extract_spotify()

    transformed_artists_df = transform_artist_details(extracted_artists_df)

    transformed_grammys_df = transform_grammys_data(extracted_grammys_df)
    transformed_spotify_df = transform_spotify_data(extracted_spotify_df)

    final_merged_df = merge(
        cleaned_spotify_df=transformed_spotify_df,
        cleaned_artists_df=transformed_artists_df,
        cleaned_grammys_df=transformed_grammys_df
    )

    gdrive_file_title = "merged_data.csv"
    upload_task = store_merged_data(title=gdrive_file_title, df=final_merged_df)

    db_load_task = load_to_db(df_to_load=final_merged_df, if_exists='replace')

    [extracted_artists_df >> transformed_artists_df,
     extracted_grammys_df >> transformed_grammys_df,
     extracted_spotify_df >> transformed_spotify_df]

    [transformed_artists_df, transformed_grammys_df, transformed_spotify_df] >> final_merged_df

    final_merged_df >> db_load_task >> upload_task

spotify_pipeline_dag()