# === FILE: api/app.py ===
from flask import Flask, render_template, request
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
import os
import io  # Used to handle in-memory file
import json
from datetime import datetime, date, timedelta, timezone

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

ROUTES = [
    ("SEITHUNGANALLUR", "ARUMUGANERI", 50),
    ("KURUKKUSALAI", "ARUMUGANERI", 70)
]

MODERATE_CONGESTION_OFFSET = 30
HEAVY_CONGESTION_OFFSET = 60
MAX_TRAVEL_TIME_MINS = 4 * 60 # 4 hours

def get_gdrive_service():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("The GOOGLE_CREDENTIALS_JSON environment variable is not set.")
    creds_json = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def get_files_for_date(service, target_date: date):
    """Gets all Excel files from G-Drive modified on a specific date."""
    if not GDRIVE_FOLDER_ID:
        raise ValueError("The GDRIVE_FOLDER_ID environment variable is not set.")

    # Construct a date range for the query in UTC
    start_of_day = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end_of_day = start_of_day + timedelta(days=1)
    
    start_of_day_iso = start_of_day.isoformat()
    end_of_day_iso = end_of_day.isoformat()

    try:
        query = (
            f"'{GDRIVE_FOLDER_ID}' in parents and "
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
            "trashed=false and "
            f"modifiedTime >= '{start_of_day_iso}' and modifiedTime < '{end_of_day_iso}'"
        )
        results = service.files().list(
            q=query,
            pageSize=150,  # Get all potential files for a day (24*4 = 96)
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime asc"  # Process files in chronological order
        ).execute()

        return results.get('files', [])
    except HttpError as error:
        print(f"An error occurred while listing files: {error}")
        return []

def download_file_from_gdrive(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # print(f"Download {int(status.progress() * 100)}%.") # Commented out for cleaner logs
        file_buffer.seek(0)
        return file_buffer
    except HttpError as error:
        print(f"An error occurred during download: {error}")
        return None

def process_data(date_filter_str=None):
    # Determine the target date for data fetching
    if date_filter_str:
        target_date = pd.to_datetime(date_filter_str).date()
    else:
        # Default to the current date in UTC
        target_date = datetime.now(timezone.utc).date()

    try:
        service = get_gdrive_service()
        files_for_day = get_files_for_date(service, target_date)

        if not files_for_day:
            return [f"<p>No data files found for {target_date.strftime('%Y-%m-%d')}.</p>"], "Not available"

        last_updated_str = files_for_day[-1]['modifiedTime']

        all_dfs = []
        for file_info in files_for_day:
            file_buffer = download_file_from_gdrive(service, file_info['id'])
            if file_buffer:
                try:
                    temp_df = pd.read_excel(file_buffer)
                    all_dfs.append(temp_df)
                except Exception as e:
                    print(f"Could not read file {file_info['name']}: {e}")

        if not all_dfs:
            return [f"<p>Found files for {target_date.strftime('%Y-%m-%d')}, but could not read their contents.</p>"], "Not available"

        # Combine data from all files for the day into one DataFrame
        df = pd.concat(all_dfs, ignore_index=True)
        # Remove duplicate rows that might exist from file overlaps
        df.drop_duplicates(inplace=True)

        last_updated = pd.to_datetime(last_updated_str).strftime("%Y-%m-%d %H:%M:%S UTC")

    except Exception as e:
        return [f"<p style='color:red;'>An error occurred: {e}</p>"], "Not available"

    # --- Start of existing processing logic on the combined DataFrame ---
    df["Device Name"] = df["Device Name"].str.upper().str.replace(" C.POST", "", regex=False).str.strip()
    df["License Plate"] = df["License Plate"].str.upper().str.strip()
    df["Passing Time"] = pd.to_datetime(df["Passing Time"], errors='coerce')
    df = df.dropna(subset=["Passing Time", "License Plate", "Device Name"])

    # Filter data to only include records from the target date as a sanity check
    df = df[df["Passing Time"].dt.date == target_date]

    route_graphs = []

    for start_cp, end_cp, google_time in ROUTES:
        df_start = df[df["Device Name"] == start_cp]
        df_end = df[df["Device Name"] == end_cp]
        merged = pd.merge(df_start, df_end, on="License Plate", suffixes=("_start", "_end"))
        merged["Travel Time (mins)"] = (merged["Passing Time_end"] - merged["Passing Time_start"]).dt.total_seconds() / 60
        merged = merged[(merged["Travel Time (mins)"] > 0) & (merged["Travel Time (mins)"] <= MAX_TRAVEL_TIME_MINS)]
        
        if merged.empty:
            continue
            
        merged["Time Interval"] = merged["Passing Time_start"].dt.floor("15min")
        report = merged.groupby("Time Interval").agg(
            avg_travel_time=('Travel Time (mins)', 'mean'),
            vehicle_count=('License Plate', 'count')
        ).reset_index()

        fig = go.Figure()
        moderate_level = google_time + MODERATE_CONGESTION_OFFSET
        heavy_level = google_time + HEAVY_CONGESTION_OFFSET
        max_y_val = report["avg_travel_time"].max() if not report.empty else heavy_level
        graph_top = max(heavy_level + 20, max_y_val * 1.1)

        fig.add_hrect(y0=moderate_level, y1=heavy_level, fillcolor="yellow", opacity=0.2, layer="below", line_width=0)
        fig.add_hrect(y0=heavy_level, y1=graph_top, fillcolor="red", opacity=0.2, layer="below", line_width=0)

        fig.add_trace(go.Scatter(
            x=report["Time Interval"], y=report["avg_travel_time"], mode='lines+markers', name="Actual Avg Travel Time",
            customdata=report[['vehicle_count']],
            hovertemplate="<b>Time</b>: %{x|%H:%M}<br><b>Avg Travel Time</b>: %{y:.1f} mins<br><b>Vehicles Reached</b>: %{customdata[0]}<extra></extra>"
        ))
        fig.add_trace(go.Scatter(x=report["Time Interval"], y=[google_time] * len(report), mode='lines', name=f"Google Avg: {google_time} mins", line=dict(color='green', dash='dash')))
        fig.add_trace(go.Scatter(x=report["Time Interval"], y=[moderate_level] * len(report), mode='lines', name=f"Moderate Threshold (+{MODERATE_CONGESTION_OFFSET} mins)", line=dict(color='orange', dash='dash')))
        fig.add_trace(go.Scatter(x=report["Time Interval"], y=[heavy_level] * len(report), mode='lines', name=f"Heavy Threshold (+{HEAVY_CONGESTION_OFFSET} mins)", line=dict(color='red', dash='dash')))

        fig.update_layout(title=f"Avg Travel Time: {start_cp} → {end_cp}", xaxis_title="Time", yaxis_title="Travel Time (mins)", height=450, yaxis_range=[0, graph_top])
        route_graphs.append(pio.to_html(fig, full_html=False))

    if not route_graphs:
        checkpoints = df['Device Name'].unique().tolist() if not df.empty else []
        msg = f"<p>No journey matches found for the defined routes on {target_date.strftime('%Y-%m-%d')}.<br>Available checkpoints in the data for this day: {checkpoints}</p>"
        return [msg], last_updated

    return route_graphs, last_updated

@app.route("/")
def dashboard():
    date_filter = request.args.get("date")
    graphs, last_updated = process_data(date_filter)
    return render_template("dashboard.html", graphs=graphs, last_updated=last_updated, selected_date=date_filter)

# Required for Vercel deployment
handler = app
