#  Music Analytics Dashboard (Spotify + YouTube Music)

An end-to-end data analytics project that integrates data from **Spotify** and **YouTube Music** to analyze and compare song performance across platforms.

---

##  Project Overview

This project builds a complete data pipeline and dashboard system to:

- Extract music data from APIs (Spotify & YouTube Music)
- Clean, transform, and unify datasets
- Perform cross-platform song matching using fuzzy logic
- Generate combined rankings based on popularity
- Visualize insights through an interactive dashboard

---

##  Architecture

1. **Data Extraction**
   - Spotify API (playlist tracks)
   - YouTube Music API (playlist + metadata)

2. **Data Storage**
   - PostgreSQL database

3. **Data Processing**
   - Data cleaning & normalization
   - Fuzzy matching (RapidFuzz) for cross-platform track matching
   - Combined popularity score calculation

4. **Visualization**
   - Streamlit dashboard
   - Plotly visualizations

---

##  Features

-  Spotify Top Tracks Dashboard  
-  YouTube Music Top Tracks Dashboard  
-  Combined Cross-Platform Rankings  
-  Search songs or artists across platforms  
-  One-click data refresh using pipelines  
-  Interactive charts and KPI highlights  

---

##  Tech Stack

- **Programming:** Python  
- **Libraries:** Pandas, NumPy, RapidFuzz  
- **Visualization:** Plotly, Streamlit  
- **Database:** PostgreSQL  
- **APIs:** Spotify API, YouTube Music API  

---

##  Data Pipelines

- `spotify_pipeline.py` → Fetch & store Spotify data  
- `ytmusic_pipeline.py` → Fetch & enrich YouTube data  
- `combine_pipeline.py` → Match, merge & rank tracks  

---

##  Key Concepts Implemented

- ETL Pipelines  
- API Integration  
- Data Cleaning & Transformation  
- Fuzzy Matching (String Similarity)  
- Cross-Platform Data Integration  
- Ranking Algorithms  
- Dashboard Development  

---

##  Dashboard Preview

* <img width="641" height="394" alt="image" src="https://github.com/user-attachments/assets/b14aace8-e1f2-441d-8cd5-1e6d747946fa" />


---

##  Future Improvements

- Add real-time streaming data  
- Improve matching accuracy using advanced NLP  
- Deploy dashboard on cloud (AWS/GCP)  
- Add user-based recommendation system  

---

##  About

This project demonstrates practical skills in **Data Analytics, Data Engineering, and Visualization**, simulating real-world cross-platform analytics use cases.

---
