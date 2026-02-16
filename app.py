
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import subprocess
from datetime import datetime

# ---------------------------
# App Setup
# ---------------------------
st.set_page_config(page_title="🎵 Music Ranking Dashboard", layout="wide")
st.title("🎧 Spotify & YouTube Music Dashboard")
st.markdown("Compare top tracks from **Spotify** and **YouTube Music**, and explore combined popularity insights.")

# ---------------------------
# Database Connection
# ---------------------------
DB_URL = "postgresql+psycopg2://musicuser:musicpass@localhost:5432/musicdb"
engine = create_engine(DB_URL)

# ---------------------------
# Sidebar Controls
# ---------------------------
st.sidebar.header("⚙️ Controls")

if st.sidebar.button("🔄 Refresh All Data"):
    with st.spinner("Fetching latest data from Spotify & YouTube APIs..."):
        try:
            subprocess.run(["python", "spotify_pipeline.py"], check=True)
            subprocess.run(["python", "ytmusic_pipeline.py"], check=True)
            subprocess.run(["python", "combine_pipeline.py"], check=True)
            st.success("✅ Data refreshed successfully! Please reload tabs to see updates.")
        except Exception as e:
            st.error(f"❌ Error refreshing data: {e}")

# 🔍 Search Feature
st.sidebar.header("🔍 Search Songs or Artists")
search_query = st.sidebar.text_input("Enter song or artist name:")

# ---------------------------
# Tabs for Dashboards
# ---------------------------
tab1, tab2, tab3 = st.tabs(["🎵 Spotify", "📺 YouTube Music", "🎶 Combined Rankings"])

# Helper: show last updated time
def show_last_updated(engine):
    try:
        last_updated = pd.read_sql("SELECT NOW() AS current_time", engine).iloc[0, 0]
        st.caption(f"🕒 Last updated on: {last_updated.strftime('%d %B %Y, %I:%M %p')}")
    except:
        st.caption("🕒 Last updated time not available.")

# ---------------------------
# Custom CSS for styling
# ---------------------------
st.markdown("""
    <style>
    /* Common metric label/value style */
    .metric-label {
        font-size: 22px; 
        font-weight: 700;
        margin-bottom: -6px;
    }
    .metric-value {
        font-size: 18px; 
        color: #E0E0E0;
        margin-top: -8px;
    }
    /* Heading colors */
    .spotify-heading {color: #1DB954 !important;}
    .ytmusic-heading {color: #FF0000 !important;}
    .combined-heading {color: #00BFFF !important;}
    </style>
""", unsafe_allow_html=True)

# ---------------------------
# 1️⃣ Spotify Tab
# ---------------------------
with tab1:
    st.markdown('<h3 class="spotify-heading">🎵 Top Spotify Tracks</h3>', unsafe_allow_html=True)
    try:
        spotify_df = pd.read_sql("""
            SELECT 
                track_rank AS rank,
                track_name AS song,
                artist_name AS artist,
                popularity,
                release_date
            FROM spotify_tracks_final
            ORDER BY track_rank ASC
            LIMIT 20
        """, engine)

        top_song = spotify_df.iloc[0]["song"]
        top_artist = spotify_df.iloc[0]["artist"]

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="metric-label" style="color:#1DB954;">🏆 Top Song</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_song}</p>', unsafe_allow_html=True)
        with col2:
            st.markdown('<p class="metric-label" style="color:#1DB954;">🎤 Top Artist</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_artist}</p>', unsafe_allow_html=True)

        st.dataframe(spotify_df)

        fig1 = px.bar(
            spotify_df.sort_values(by="popularity", ascending=True),
            x="popularity", y="song", orientation="h",
            title="Spotify Top 20 Songs by Popularity",
            labels={"popularity": "Popularity", "song": "Track Name"}
        )
        st.plotly_chart(fig1, use_container_width=True)
        show_last_updated(engine)
    except Exception as e:
        st.error(f"Error loading Spotify data: {e}")

# ---------------------------
# 2️⃣ YouTube Music Tab
# ---------------------------
with tab2:
    st.markdown('<h3 class="ytmusic-heading">📺 Top YouTube Music Tracks</h3>', unsafe_allow_html=True)
    try:
        ytmusic_df = pd.read_sql("""
            SELECT 
                rank,
                track_name AS song,
                artist_name AS artist,
                popularity_score AS popularity,
                video_publish_date AS release_date
            FROM ytmusic_tracks_final
            ORDER BY rank ASC
            LIMIT 20
        """, engine)

        top_song = ytmusic_df.iloc[0]["song"]
        top_artist = ytmusic_df.iloc[0]["artist"]

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="metric-label" style="color:#FF0000;">🏆 Top Song</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_song}</p>', unsafe_allow_html=True)
        with col2:
            st.markdown('<p class="metric-label" style="color:#FF0000;">🎤 Top Artist</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_artist}</p>', unsafe_allow_html=True)

        st.dataframe(ytmusic_df)

        fig2 = px.bar(
            ytmusic_df.sort_values(by="popularity", ascending=True),
            x="popularity", y="song", orientation="h",
            title="YouTube Music Top 20 Songs by Popularity",
            labels={"popularity": "Popularity", "song": "Track Name"}
        )
        st.plotly_chart(fig2, use_container_width=True)
        show_last_updated(engine)
    except Exception as e:
        st.error(f"Error loading YouTube data: {e}")

# ---------------------------
# 3️⃣ Combined Rankings Tab
# ---------------------------
with tab3:
    st.markdown('<h3 class="combined-heading">🎶 Combined Spotify + YouTube Rankings</h3>', unsafe_allow_html=True)
    try:
        combined_df = pd.read_sql("""
            SELECT 
                combined_rank AS rank,
                track_name AS song,
                COALESCE(artist_spotify, artist_youtube) AS artist,
                mean_popularity AS popularity
            FROM all_music_tracks_ranked
            ORDER BY combined_rank ASC
            LIMIT 20
        """, engine)

        top_song = combined_df.iloc[0]["song"]
        top_artist = combined_df.iloc[0]["artist"]

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="metric-label" style="color:#00BFFF;">🏆 Top Combined Song</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_song}</p>', unsafe_allow_html=True)
        with col2:
            st.markdown('<p class="metric-label" style="color:#00BFFF;">🎤 Top Artist</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="metric-value">{top_artist}</p>', unsafe_allow_html=True)

        st.dataframe(combined_df)

        fig3 = px.bar(
            combined_df.sort_values(by="popularity", ascending=True),
            x="popularity", y="song", orientation="h",
            title="Top Combined Songs by Mean Popularity",
            labels={"popularity": "Mean Popularity", "song": "Track Name"}
        )
        st.plotly_chart(fig3, use_container_width=True)
        show_last_updated(engine)
    except Exception as e:
        st.error(f"Error loading combined data: {e}")

# ---------------------------
# 🔍 Handle Search Query
# ---------------------------
if search_query:
    st.markdown("---")
    st.subheader(f"🔍 Search Results for: *{search_query}*")
    query = f"""
        SELECT * FROM all_music_tracks_ranked
        WHERE LOWER(track_name) LIKE LOWER('%{search_query}%')
        OR LOWER(artist_spotify) LIKE LOWER('%{search_query}%')
        OR LOWER(artist_youtube) LIKE LOWER('%{search_query}%')
        ORDER BY combined_rank ASC
        LIMIT 20
    """
    try:
        search_results = pd.read_sql(query, engine)
        st.dataframe(search_results)
    except Exception as e:
        st.error(f"Error loading search results: {e}")
