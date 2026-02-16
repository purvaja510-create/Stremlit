import spotipy
from spotipy.oauth2 import SpotifyOAuth
import psycopg2

# -----------------------------
# Spotify API Setup
# -----------------------------
SPOTIFY_CLIENT_ID = "535ff957b1954c09a5ab3ec9ef996e85"
SPOTIFY_CLIENT_SECRET = "c704454728814742aceb5aa68b2ebed3"
SPOTIFY_REDIRECT_URI = "http://127.0.0.1:8888/callback"
PLAYLIST_ID = "6Pdq6cnFzk85BOMB2Smof9"

scope = "playlist-read-private"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=SPOTIFY_REDIRECT_URI,
    scope=scope
))

# -----------------------------
# Database Setup
# -----------------------------
DB_HOST = "localhost"
DB_NAME = "musicdb"
DB_USER = "musicuser"
DB_PASSWORD = "musicpass"
DB_PORT = 5432

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()

# Drop old table (optional)
cur.execute("DROP TABLE IF EXISTS spotify_tracks_final")
conn.commit()

# Create table with ranking column
cur.execute("""
CREATE TABLE spotify_tracks_final (
    track_id VARCHAR(50) PRIMARY KEY,
    track_name VARCHAR(200),
    artist_name VARCHAR(200),
    album_name VARCHAR(200),
    release_date DATE,
    popularity INT,
    track_rank INT
)
""")
conn.commit()

# -----------------------------
# Fetch Playlist Tracks
# -----------------------------
def fetch_playlist_tracks(playlist_id):
    results = sp.playlist_items(playlist_id)
    tracks = results['items']

    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])

    return tracks

tracks = fetch_playlist_tracks(PLAYLIST_ID)

# -----------------------------
# Insert Tracks into Database
# -----------------------------
count_inserted = 0

for rank, item in enumerate(tracks, start=1):  
    track = item['track']
    if track is None or track['id'] is None:
        print(f"Skipped track: {track['name'] if track else 'Unknown'}")
        continue

    track_id = track['id']
    track_name = track['name']
    artist_name = ", ".join([artist['name'] for artist in track['artists']])
    album_name = track['album']['name']
    release_date = track['album']['release_date']
    popularity = track['popularity']

    # ✅ Normalize release_date
    if release_date:
        if len(release_date) == 4:
            release_date = f"{release_date}-01-01"
        elif len(release_date) == 7:
            release_date = f"{release_date}-01"

    try:
        cur.execute("""
        INSERT INTO spotify_tracks_final 
        (track_id, track_name, artist_name, album_name, release_date, popularity, track_rank)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO UPDATE SET
            track_name = EXCLUDED.track_name,
            artist_name = EXCLUDED.artist_name,
            album_name = EXCLUDED.album_name,
            release_date = EXCLUDED.release_date,
            popularity = EXCLUDED.popularity,
            track_rank = EXCLUDED.track_rank
        """, (track_id, track_name, artist_name, album_name, release_date, popularity, rank))
        conn.commit()
        count_inserted += 1
    except Exception as e:
        print(f"Error inserting {track_name}: {e}")
        conn.rollback()

print(f"✅ {count_inserted} tracks processed and stored in database with ranking.")

cur.close()
conn.close()


