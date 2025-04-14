import pandas as pd
import re
import numpy as np
import logging
import os
from airflow.decorators import task

@task(task_id="merge_and_finalize_data")
def merge(
    cleaned_spotify_df: pd.DataFrame,
    cleaned_artists_df: pd.DataFrame,
    cleaned_grammys_df: pd.DataFrame
) -> pd.DataFrame:
    logging.info("Iniciando merge de los DataFrames limpios...")
    
    if cleaned_spotify_df is None or cleaned_spotify_df.empty:
        raise ValueError("DataFrame de Spotify vacío/None.")
    if cleaned_artists_df is None or cleaned_artists_df.empty:
        logging.warning("DataFrame de Artistas vacío/None. Se procederá sin información de artista.")
        cleaned_artists_df = pd.DataFrame(columns=['track_id', 'artist_name', 'artist_followers', 'artist_popularity', 'artist_id'])
    if cleaned_grammys_df is None or cleaned_grammys_df.empty:
        logging.warning("DataFrame de Grammys vacío/None. Se procederá sin información de Grammys.")
        cleaned_grammys_df = pd.DataFrame(columns=['id', 'year', 'category', 'nominee', 'artist'])

    logging.info(f"Spotify DF: {cleaned_spotify_df.shape}")
    logging.info(f"Artistas DF: {cleaned_artists_df.shape}")
    logging.info(f"Grammys DF: {cleaned_grammys_df.shape}")

    def normalize_name(name):
        name = str(name).lower() if not pd.isna(name) else ""
        name = re.sub(r'\(.*?\)|\[.*?\]|\{.*?\}', '', name)
        name = re.sub(r'\b(featuring|feat|ft)\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'&', ' ', name)
        name = re.sub(r'[^a-z0-9\s]', '', name)
        return ' '.join(name.split())

    def normalize_spotify_artists(artists_str):
        if pd.isna(artists_str): return ""
        primary_artist = str(artists_str).split(';')[0]
        return normalize_name(primary_artist)

    cleaned_grammys_df['artist_normalized'] = cleaned_grammys_df['artist'].apply(normalize_name)
    cleaned_grammys_df['nominee_normalized'] = cleaned_grammys_df['nominee'].apply(normalize_name)


    cleaned_spotify_df['artists_normalized_primary'] = cleaned_spotify_df['artists'].apply(normalize_spotify_artists)
    cleaned_spotify_df['track_name_normalized'] = cleaned_spotify_df['track_name'].apply(normalize_name)
    cleaned_spotify_df['album_name_normalized'] = cleaned_spotify_df['album_name'].apply(normalize_name)

    combined_spotify = pd.merge(
        cleaned_spotify_df,
        cleaned_artists_df[['track_id', 'artist_name', 'artist_followers', 'artist_popularity', 'artist_id']],
        on='track_id',
        how='inner'
    ).drop_duplicates(subset=['track_id'])

    grammy_nominations_by_work = cleaned_grammys_df.groupby(
        ['artist_normalized', 'nominee_normalized']
    ).size().reset_index(name='work_grammy_nominations')

    merged_step1 = pd.merge(
        combined_spotify,
        grammy_nominations_by_work,
        left_on=['track_name_normalized'],
        right_on=['nominee_normalized'],
        how='left'
    )

    merged_step2 = pd.merge(
        merged_step1,
        grammy_nominations_by_work,
        left_on=['album_name_normalized'],
        right_on=['nominee_normalized'],
        how='left',
        suffixes=('_track', '_album')
    )

    for col in ['work_grammy_nominations_track', 'work_grammy_nominations_album', 
                'artist_normalized_track', 'artist_normalized_album', 'artists_normalized_primary']:
        merged_step2[col] = merged_step2[col].fillna(0 if 'nominations' in col else '')

    cond_track_match = (
        (merged_step2['work_grammy_nominations_track'] > 0) &
        merged_step2.apply(lambda row: row['artists_normalized_primary'] in row['artist_normalized_track'], axis=1)
    )

    cond_album_match = (
        (merged_step2['work_grammy_nominations_album'] > 0) &
        merged_step2.apply(lambda row: row['artists_normalized_primary'] in row['artist_normalized_album'], axis=1)
    )

    merged_step2['track_grammy_nominations'] = np.where(
        cond_track_match,
        merged_step2['work_grammy_nominations_track'],
        0
    ).astype(int)

    merged_step2['album_grammy_nominations'] = np.where(
        cond_album_match,
        merged_step2['work_grammy_nominations_album'],
        0
    ).astype(int)

    merged_step2['has_grammy_nomination'] = (merged_step2['track_grammy_nominations'] > 0) | \
                                          (merged_step2['album_grammy_nominations'] > 0)

    final_columns = [
        'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'explicit',
        'danceability', 'energy', 'genre_category', 'duration_min',
        'artist_name', 'artist_followers', 'artist_popularity',
        'has_grammy_nomination', 'track_grammy_nominations', 'album_grammy_nominations'
    ]

    cols_to_drop = [
        'artists_normalized_primary', 'track_name_normalized', 'album_name_normalized',
        'nominee_normalized_track', 'artist_normalized_track', 'work_grammy_nominations_track',
        'nominee_normalized_album', 'artist_normalized_album', 'work_grammy_nominations_album'
    ]
    cols_to_drop_existing = [col for col in cols_to_drop if col in merged_step2.columns]
    final_df = merged_step2.drop(columns=cols_to_drop_existing)[final_columns]

    if not final_df.empty:
        nom_sum = final_df['track_grammy_nominations'] + final_df['album_grammy_nominations']
        final_df = final_df.loc[nom_sum.groupby(final_df['track_id']).idxmax()]
    output_path = os.path.join('data', 'merge_dataset.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)
    
    logging.info(f"Archivo guardado exitosamente en: {output_path}")
    logging.info(f"Merge completado. Forma final: {final_df.shape}")
    logging.info(f"Tracks con nominaciones: {final_df['has_grammy_nomination'].sum()}")
    
    return final_df