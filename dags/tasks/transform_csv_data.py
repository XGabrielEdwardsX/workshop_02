import pandas as pd
from airflow.decorators import task
import os

genre_categories = {
    'Rock': ['alt-rock', 'alternative', 'emo', 'goth', 'grunge', 'hard-rock', 'indie', 'punk-rock', 'punk', 'psych-rock', 'rock', 'rock-n-roll', 'rockabilly'],
    'Pop': ['cantopop', 'indie-pop', 'j-pop', 'k-pop', 'mandopop', 'pop', 'pop-film', 'power-pop', 'synth-pop'],
    'Electronic': ['breakbeat', 'chicago-house', 'club', 'dance', 'deep-house', 'detroit-techno', 'disco', 'drum-and-bass', 'dub', 'dubstep', 'edm', 'electro', 'electronic', 'garage', 'hardstyle', 'house', 'idm', 'minimal-techno', 'progressive-house', 'techno', 'trance', 'trip-hop'],
    'Hip-Hop/R&B': ['hip-hop', 'r-n-b', 'soul'],
    'Metal': ['black-metal', 'death-metal', 'grindcore', 'heavy-metal', 'metal', 'metalcore'],
    'Folk/Acoustic': ['acoustic', 'bluegrass', 'folk', 'singer-songwriter'],
    'Jazz/Blues': ['blues', 'jazz'],
    'Classical/Instrumental': ['classical', 'new-age', 'opera', 'piano', 'sleep', 'study'],
    'Latin': ['forro', 'latin', 'latino', 'mpb', 'pagode', 'salsa', 'samba', 'sertanejo', 'tango'],
    'Reggae/Dancehall': ['dancehall', 'reggae', 'reggaeton'],
    'Country': ['country', 'honky-tonk'],
    'World/Regional': ['afrobeat', 'brazil', 'french', 'german', 'indian', 'iranian', 'malay', 'spanish', 'swedish', 'turkish', 'world-music'],
    'Anime/Japanese': ['anime', 'j-dance', 'j-idol', 'j-rock'],
    'Kids/Comedy': ['children', 'comedy', 'disney', 'kids', 'show-tunes'],
    'Other': ['ambient', 'british', 'chill', 'funk', 'gospel', 'groove', 'guitar', 'happy', 'hardcore', 'party', 'romance', 'sad', 'ska']
}

genre_to_category = {genre: category for category, genres in genre_categories.items() for genre in genres}

@task(task_id="transform_spotify_data")
def transform_spotify_data(raw_spotify_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_spotify_df.copy()

    df = df.drop(columns=['Unnamed: 0', 'key', 'mode', 'speechiness', 'liveness', 'time_signature'], errors='ignore')
    df = df.drop_duplicates(subset=['track_id'])
    df = df.dropna(subset=['artists', 'album_name', 'track_name'])

    df['genre_category'] = df['track_genre'].map(genre_to_category).fillna('Other')

    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()
    return df