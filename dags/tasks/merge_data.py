# dags/tasks/merge_data.py

import pandas as pd
import os
from airflow.decorators import task
import logging

@task(task_id="merge_and_finalize_data")
def merge(
    cleaned_spotify_df: pd.DataFrame,
    cleaned_artists_df: pd.DataFrame,
    cleaned_grammys_df: pd.DataFrame
) -> pd.DataFrame:
    logging.info("Iniciando merge de los DataFrames limpios...")

    if cleaned_spotify_df is None or cleaned_spotify_df.empty:
        logging.error("El DataFrame de Spotify limpio está vacío o es None. No se puede continuar.")
        raise ValueError("DataFrame de Spotify vacío/None.")
    if cleaned_artists_df is None or cleaned_artists_df.empty:
        logging.warning("El DataFrame de Artistas limpio está vacío o es None. Se procederá sin información de artista.")
        cleaned_artists_df = pd.DataFrame(columns=['track_id', 'artist_id', 'artist_followers', 'artist_popularity'])
    if cleaned_grammys_df is None or cleaned_grammys_df.empty:
        logging.warning("El DataFrame de Grammys limpio está vacío o es None. Se procederá sin información de Grammys.")
        cleaned_grammys_df = pd.DataFrame(columns=['id', 'year', 'category', 'nominee', 'artist'])

    logging.info(f"Spotify DF: {cleaned_spotify_df.shape}")
    logging.info(f"Artistas DF: {cleaned_artists_df.shape}")
    logging.info(f"Grammys DF: {cleaned_grammys_df.shape}")

    merged_df = pd.merge(
        cleaned_spotify_df,
        cleaned_artists_df[['track_id', 'artist_id', 'artist_followers', 'artist_popularity']],
        on='track_id',
        how='left'
    )

    merged_df['track_name_norm'] = merged_df['track_name'].astype(str).str.lower().str.strip()
    merged_df['artists_norm'] = merged_df['artists'].astype(str).str.lower().str.strip()
    cleaned_grammys_df['nominee_norm'] = cleaned_grammys_df['nominee'].astype(str).str.lower().str.strip()
    cleaned_grammys_df['artist_norm'] = cleaned_grammys_df['artist'].astype(str).str.lower().str.strip()

    merged_df['artists_list'] = merged_df['artists_norm'].str.split(';')
    exploded_spotify = merged_df.explode('artists_list')
    exploded_spotify['artists_list'] = exploded_spotify['artists_list'].str.strip()

    # Asegurarse que las columnas necesarias existen en grammys antes del merge
    grammy_cols_needed_exact = ['id', 'year', 'category', 'nominee', 'artist', 'nominee_norm', 'artist_norm']
    if not cleaned_grammys_df.empty and all(col in cleaned_grammys_df.columns for col in grammy_cols_needed_exact):
        exact_match = pd.merge(
            exploded_spotify,
            cleaned_grammys_df,
            left_on=['track_name_norm', 'artists_list'],
            right_on=['nominee_norm', 'artist_norm'],
            how='left',
            suffixes=('', '_grammy')
        )
    else:
        logging.warning("Grammys DF vacío o faltan columnas para merge exacto. Creando columnas vacías.")
        exploded_spotify['id'] = pd.NA
        exploded_spotify['year'] = pd.NA
        exploded_spotify['category'] = pd.NA
        exploded_spotify['nominee'] = pd.NA
        exploded_spotify['artist'] = pd.NA
        exploded_spotify['nominee_norm'] = pd.NA
        exploded_spotify['artist_norm'] = pd.NA
        exact_match = exploded_spotify

    exact_match = exact_match.drop_duplicates(subset=['track_id'], keep='first')

    no_match_mask = exact_match['id'].isna()
    final_df_parts = [exact_match[~no_match_mask]]

    grammy_cols_needed_song = ['id', 'year', 'category', 'nominee', 'artist', 'nominee_norm']
    if no_match_mask.any() and not cleaned_grammys_df.empty and all(col in cleaned_grammys_df.columns for col in grammy_cols_needed_song):
        logging.info(f"Intentando merge por canción para {no_match_mask.sum()} tracks.")
        tracks_without_match = merged_df[merged_df['track_id'].isin(exact_match.loc[no_match_mask, 'track_id'])].copy()

        song_match = pd.merge(
            tracks_without_match,
            cleaned_grammys_df,
            left_on='track_name_norm',
            right_on='nominee_norm',
            how='left',
            suffixes=('', '_grammy_song')
        )

        song_match_found = song_match[song_match['id'].notna()].copy()
        song_match_found = song_match_found.drop_duplicates(subset=['track_id'], keep='first')

        if not song_match_found.empty:
            final_df_parts.append(song_match_found)

        matched_ids_so_far = pd.concat([df['track_id'] for df in final_df_parts]).unique()
        remaining_tracks = merged_df[~merged_df['track_id'].isin(matched_ids_so_far)].copy()
        if not remaining_tracks.empty:
            logging.info(f"Añadiendo {len(remaining_tracks)} tracks sin ningún match.")
            grammy_cols_orig = ['id', 'year', 'category', 'nominee', 'artist', 'nominee_norm', 'artist_norm']
            for col in grammy_cols_orig:
                if col in cleaned_grammys_df.columns and col not in remaining_tracks.columns:
                    remaining_tracks[col] = pd.NA
            final_df_parts.append(remaining_tracks)

    elif no_match_mask.any():
         logging.info(f"Añadiendo {no_match_mask.sum()} tracks sin match (no se intentó merge por canción).")
         remaining_tracks = exact_match[no_match_mask].copy()
         # Asegurar columnas grammy básicas
         if 'id' not in remaining_tracks.columns: remaining_tracks['id'] = pd.NA
         if 'year' not in remaining_tracks.columns: remaining_tracks['year'] = pd.NA
         if 'category' not in remaining_tracks.columns: remaining_tracks['category'] = pd.NA
         # No añadir a final_df_parts, ya están en exact_match[no_match_mask] que es el primer elemento

    if final_df_parts:
        final_df = pd.concat(final_df_parts, ignore_index=True, sort=False)
    else:
        logging.warning("No se generaron partes para el DataFrame final.")
        final_df = merged_df.copy() # Fallback
        final_df['id'] = pd.NA
        final_df['year'] = pd.NA
        final_df['category'] = pd.NA


    cols_to_drop = [
        'track_name_norm', 'artists_norm', 'artists_list',
        'nominee_norm', 'artist_norm',
        'nominee', 'artist',
        'id_grammy', 'year_grammy', 'category_grammy', 'nominee_grammy', 'artist_grammy',
        'id_grammy_song', 'year_grammy_song', 'category_grammy_song', 'nominee_grammy_song', 'artist_grammy_song'
    ]
    final_df = final_df.drop(columns=[col for col in cols_to_drop if col in final_df.columns], errors='ignore')

    rename_map = {
        'id': 'grammy_id',
        'year': 'grammy_year',
        'category': 'grammy_category'
    }
    final_rename_map = {k: v for k, v in rename_map.items() if k in final_df.columns and v not in final_df.columns}
    if final_rename_map:
        final_df = final_df.rename(columns=final_rename_map)

    for col in ['grammy_id', 'grammy_year', 'grammy_category']:
        if col not in final_df.columns:
            final_df[col] = pd.NA

    if 'grammy_year' in final_df.columns:
        final_df['grammy_year'] = pd.to_numeric(final_df['grammy_year'], errors='coerce')

    final_df = final_df.drop_duplicates(subset=['track_id'], keep='first')

    if 'grammy_id' in final_df.columns:
        final_df['grammy_win'] = final_df['grammy_id'].notna()
    else:
        final_df['grammy_win'] = False

    logging.info(f"Merge y finalización completados. Forma final: {final_df.shape}")
    logging.info(f"Columnas finales: {final_df.columns.tolist()}")
    logging.info(f"Tracks con match en Grammys: {final_df['grammy_id'].notna().sum()}")

    return final_df