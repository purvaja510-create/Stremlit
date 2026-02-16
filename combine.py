import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import fuzz, process

# ----------------------
# 1. Connect to Database
# ----------------------
DB_URL = "postgresql+psycopg2://musicuser:musicpass@localhost:5432/musicdb"
engine = create_engine(DB_URL)

spotify_df = pd.read_sql("SELECT * FROM spotify_tracks_final", engine)
ytmusic_df = pd.read_sql("SELECT * FROM ytmusic_tracks_final", engine)

# ----------------------
# 2. Normalize text for matching
# ----------------------
def clean_text(s):
    if pd.isna(s):
        return ""
    return (
        s.lower()
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("&", "and")
        .replace("feat.", "")
        .replace("ft.", "")
        .strip()
    )

spotify_df["track_clean"] = spotify_df["track_name"].apply(clean_text)
ytmusic_df["track_clean"] = ytmusic_df["track_name"].apply(clean_text)

# ----------------------
# 3. Fuzzy Match Tracks
# ----------------------
matched_indices = set()
results = []

for _, sp_row in spotify_df.iterrows():
    match = process.extractOne(
        sp_row["track_clean"],
        ytmusic_df["track_clean"],
        scorer=fuzz.token_sort_ratio
    )
    if match and match[1] > 85:
        yt_row = ytmusic_df[ytmusic_df["track_clean"] == match[0]].iloc[0]
        results.append({
            "track_name": sp_row["track_name"],
            "artist_spotify": sp_row["artist_name"],
            "artist_youtube": yt_row["artist_name"],
            "spotify_popularity": sp_row.get("popularity", None),
            "ytmusic_popularity": yt_row.get("popularity_score", None)
        })
        matched_indices.add(yt_row.name)
    else:
        # Spotify-only track
        results.append({
            "track_name": sp_row["track_name"],
            "artist_spotify": sp_row["artist_name"],
            "artist_youtube": None,
            "spotify_popularity": sp_row.get("popularity", None),
            "ytmusic_popularity": None
        })

# ----------------------
# 4. Add YT-only Tracks (not matched)
# ----------------------
yt_unmatched = ytmusic_df[~ytmusic_df.index.isin(matched_indices)]

for _, yt_row in yt_unmatched.iterrows():
    results.append({
        "track_name": yt_row["track_name"],
        "artist_spotify": None,
        "artist_youtube": yt_row["artist_name"],
        "spotify_popularity": None,
        "ytmusic_popularity": yt_row.get("popularity_score", None)
    })

# ----------------------
# 5. Compute Mean Popularity & Assign Unique Ranking
# ----------------------
final_df = pd.DataFrame(results)

# Mean popularity across both platforms
final_df["mean_popularity"] = final_df[
    ["spotify_popularity", "ytmusic_popularity"]
].mean(axis=1, skipna=True)

# Assign unique ranks (even if same popularity)
final_df = final_df.sort_values(by="mean_popularity", ascending=False)
final_df["combined_rank"] = (
    final_df["mean_popularity"].rank(ascending=False, method="first").astype(int)
)

# ----------------------
# 6. Save to Database
# ----------------------
final_df.to_sql("all_music_tracks_ranked", engine, if_exists="replace", index=False)

# ----------------------
# 7. Summary Output
# ----------------------
print("✅ Music Data Combined and Ranked with Unique Ranks!")
print(f"🎵 Total tracks ranked: {len(final_df)}")
print(f"📊 Saved to table: 'all_music_tracks_ranked'\n")

# Show top 10 ranked tracks
print("🎧 Top 10 Ranked Songs:")
print(final_df[["combined_rank", "track_name", "mean_popularity"]].head(10).to_string(index=False))
