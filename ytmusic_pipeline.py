from ytmusicapi import YTMusic
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from googleapiclient.discovery import build

# ----------------------
# 1. YouTube Music Setup
# ----------------------
ytmusic = YTMusic() 
PLAYLIST_ID = "PL4fGSI1pDJn6puJdseH2Rt9sMvt9E2M4i"
TOP_N = 100

# ----------------------
# 2. YouTube Data API Setup
# ----------------------
YOUTUBE_API_KEY = "AIzaSyAnRguuBVkj2fWruKyE8DjLQG2uEP99vEA"  
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# ----------------------
# 3. Database Setup
# ----------------------
DB_URL = "postgresql+psycopg2://musicuser:musicpass@localhost:5432/musicdb"
engine = create_engine(DB_URL)

# ----------------------
# 4. Helper Functions
# ----------------------
def get_video_details(video_ids):
    videos = {}
    for i in range(0, len(video_ids), 50):  
        chunk = video_ids[i:i+50]
        response = youtube.videos().list(
            part="snippet",
            id=",".join(chunk)
        ).execute()
        for item in response['items']:
            vid = item['id']
            publish_date = item['snippet']['publishedAt']
            videos[vid] = {'video_publish_date': publish_date}
    return videos

def popularity_from_rank(rank, max_rank):
    # Scale rank to 1-100 popularity
    return round((max_rank - rank + 1) / max_rank * 100)

# ----------------------
# 5. Fetch Playlist Tracks
# ----------------------
def fetch_ytmusic_playlist():
    playlist = ytmusic.get_playlist(PLAYLIST_ID)
    tracks = []
    for idx, t in enumerate(playlist['tracks'][:TOP_N], 1):
        tracks.append({
            'track_name': t['title'],
            'track_id': t.get('videoId', None),
            'artist_name': ", ".join([a['name'] for a in t['artists']]) if t.get('artists') else None,
            'album_name': t['album']['name'] if t.get('album') else None,
            'duration_ms': t.get('duration_seconds', 0) * 1000,
            'rank': idx
        })

    df = pd.DataFrame(tracks)
    
    # Fetch video publish date
    video_ids = [vid for vid in df['track_id'] if vid]  
    video_data = get_video_details(video_ids)
    df['video_publish_date'] = df['track_id'].map(lambda vid: video_data.get(vid, {}).get('video_publish_date') if vid else None)
    
    # Popularity based on rank
    max_rank = len(df)
    df['popularity_score'] = df['rank'].apply(lambda r: popularity_from_rank(r, max_rank))
    
    df['last_updated'] = datetime.now()
    return df

# ----------------------
# 6. ETL Pipeline
# ----------------------
def run_ytmusic_pipeline():
    df = fetch_ytmusic_playlist()
    df.to_sql('ytmusic_tracks_final', engine, if_exists='replace', index=False)
    print(f"[{datetime.now()}] YouTube Music Pipeline: {len(df)} records loaded.")

# ----------------------
# 7. Run Pipeline
# ----------------------
if __name__ == "__main__":
    run_ytmusic_pipeline()

