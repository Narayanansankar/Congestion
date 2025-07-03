# === FILE: api/app.py ===
from flask import Flask, render_template, request
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
import os
import io
import json
from datetime import datetime, date, timedelta, timezone

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

# --- Configuration ---
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

ROUTES = [
    ("SEITHUNGANALLUR", "ARUMUGANERI", 50),
    ("KURUKKUSALAI", "ARUMUGANERI", 70)
]

MODERATE_CONGESTION_OFFSET = 30
HEAVY_CONGESTION_OFFSET = 60
MAX_TRAVEL_TIME_MINS = 4 * 60  # 4 hours
REQUIRED_COLUMNS = ["Device Name", "License Plate", "Passing Time"]

# --- Google Drive Functions ---

def get_gdrive_service():
    """Establishes a connection to the Google Drive API."""
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("The GOOGLE_CREDENTIALS_JSON environment variable is not set.")
    creds_json = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        creds_json, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def get_all_files_for_period(service, target_date=None):
    """
    Gets a list of all relevant Excel files from Google Drive.
    - If target_date is specified, gets all files for that single day.
    - If target_date is None, gets all files in the entire folder for a full history view.
    """
    if not GDRIVE_FOLDER_ID:
        raise ValueError("The GDRIVE_FOLDER_ID environment variable is not set.")

    query_parts = [
        f"'{GDRIVE_FOLDER_ID}' in parents",
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        "trashed=false"
    ]

    # If a date is provided, add it to the search query
    if target_date:
        start_of_day = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
        end_of_day = start_of_day + timedelta(days=1)
        query_parts.append(f"modifiedTime >= '{start_of_day.isoformat()}'")
        query_parts.append(f"modifiedTime < '{end_of_day.isoformat()}'")

    query = " and ".join(query_parts)

    try:
        all_files = []
        page_token = None
        while True:
            response = service.files().list(
                q=query,
                pageSize=1000,
                fields="nextPageToken, files(id, name, modifiedTime)",
                orderBy="modifiedTime",  # Process files in chronological order
                pageToken=page_token
            ).execute()
            all_files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        return all_files
    except HttpError as error:
        print(f"An error occurred while finding files: {error}")
        return []

def download_file_from_gdrive(service, file_id):
    """Downloads a file's content from Google Drive into memory."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_buffer.seek(0)
        return file_buffer
    except HttpError as error:
        print(f"An error occurred during download: {error}")
        return None

# --- Data Processing and Graphing ---

def process_data(date_filter_str=None):
    """Main function to fetch, process, and generate graphs from the data."""
    target_date = None
    if date_filter_str:
        try:
            target_date = pd.to_datetime(date_filter_str).date()
        except ValueError:
             return [f"<p style='color:red;'>Invalid date format: '{date_filter_str}'. Please use YYYY-MM-DD.</p>"], "Not available"

    try:
        service = get_gdrive_service()
        # Get ALL files for the specified period (a single day or all time)
        files_to_process = get_all_files_for_period(service, target_date)

        if not files_to_process:
            date_msg = f"for {target_date.strftime('%Y-%m-%d')}" if target_date else "in the Drive folder"
            return [f"<p>No data files found {date_msg}.</p>"], "Not available"

        all_dfs = []
        # The last file in the sorted list has the most recent update time
        last_updated_str = files_to_process[-1]['modifiedTime']

        for file_info in files_to_process:
            file_buffer = download_file_from_gdrive(service, file_info['id'])
            if file_buffer:
                df_temp = pd.read_excel(file_buffer)
                # Validate that the file has the necessary columns
                missing_columns = [col for col in REQUIRED_COLUMNS if col not in df_temp.columns]
                if missing_columns:
                    found_columns = ', '.join(df_temp.columns.tolist()) if not df_temp.columns.empty else 'None'
                    error_msg = f"<p style='color:red;'>File Error in '{file_info['name']}': Missing columns: {', '.join(missing_columns)}. Found: {found_columns}</p>"
                    return [error_msg], "Not available"
                all_dfs.append(df_temp)

        if not all_dfs:
            return ["<p>Files were found, but none could be read.</p>"], "Not available"

        # Combine all data from incremental files and remove duplicates
        df = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
        last_updated = pd.to_datetime(last_updated_str).strftime("%Y-%m-%d %H:%M:%S UTC")

    except Exception as e:
        return [f"<p style='color:red;'>An error occurred: {e}</p>"], "Not available"

    # Clean and prepare the combined DataFrame
    df["Device Name"] = df["Device Name"].str.upper().str.replace(" C.POST", "", regex=False).str.strip()
    df["License Plate"] = df["License Plate"].str.upper().str.strip()
    df["Passing Time"] = pd.to_datetime(df["Passing Time"], errors='coerce')
    df.dropna(subset=REQUIRED_COLUMNS, inplace=True)
    
    # If a date was selected, filter the combined dataframe to that date.
    if target_date:
        df = df[df["Passing Time"].dt.date == target_date]
    
    if df.empty:
        date_msg = f"on {target_date.strftime('%Y-%m-%d')}" if target_date else "for the selected period"
        return [f"<p>No valid data found {date_msg}.</p>"], last_updated

    # --- Generate Graphs for Each Route ---
    route_graphs = []
    for start_cp, end_cp, google_time in ROUTES:
        df_start = df[df["Device Name"] == start_cp]
        df_end = df[df["Device Name"] == end_cp]

        # Graph 1: Average Travel Time
        merged = pd.merge(df_start, df_end, on="License Plate", suffixes=("_start", "_end"))
        merged["Travel Time (mins)"] = (merged["Passing Time_end"] - merged["Passing Time_start"]).dt.total_seconds() / 60
        merged = merged[(merged["Travel Time (mins)"] > 0) & (merged["Travel Time (mins)"] <= MAX_TRAVEL_TIME_MINS)]
        
        travel_time_html = ""
        if not merged.empty:
            merged["Time Interval"] = merged["Passing Time_start"].dt.floor("15min")
            report = merged.groupby("Time Interval").agg(avg_travel_time=('Travel Time (mins)', 'mean'), vehicle_count=('License Plate', 'count')).reset_index()
            
            fig_travel = go.Figure()
            moderate_level = google_time + MODERATE_CONGESTION_OFFSET
            heavy_level = google_time + HEAVY_CONGESTION_OFFSET
            max_y_val = report["avg_travel_time"].max()
            graph_top = max(heavy_level + 20, max_y_val * 1.1)

            fig_travel.add_hrect(y0=moderate_level, y1=heavy_level, fillcolor="yellow", opacity=0.2, layer="below", line_width=0)
            fig_travel.add_hrect(y0=heavy_level, y1=graph_top, fillcolor="red", opacity=0.2, layer="below", line_width=0)

            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=report["avg_travel_time"], mode='lines+markers', name="Actual Avg Travel Time", customdata=report[['vehicle_count']], hovertemplate="<b>Time</b>: %{x|%Y-%m-%d %H:%M}<br><b>Avg Travel Time</b>: %{y:.1f} mins<br><b>Vehicles Reached</b>: %{customdata[0]}<extra></extra>"))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[google_time] * len(report), mode='lines', name=f"Google Avg: {google_time} mins", line=dict(color='green', dash='dash')))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[moderate_level] * len(report), mode='lines', name=f"Moderate Threshold (+{MODERATE_CONGESTION_OFFSET} mins)", line=dict(color='orange', dash='dash')))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[heavy_level] * len(report), mode='lines', name=f"Heavy Threshold (+{HEAVY_CONGESTION_OFFSET} mins)", line=dict(color='red', dash='dash')))
            
            fig_travel.update_layout(title=f"Avg Travel Time: {start_cp} → {end_cp}", xaxis_title="Time (Trip Start)", yaxis_title="Travel Time (mins)", height=450, yaxis_range=[0, graph_top])
            travel_time_html = pio.to_html(fig_travel, full_html=False)
        else:
            travel_time_html = f"<h3>Avg Travel Time: {start_cp} → {end_cp}</h3><p>No completed journeys found for this route in the selected period.</p>"

        # Graph 2: Vehicle Volume at Start Point
        volume_html = ""
        if not df_start.empty:
            df_start_volume = df_start.copy()
            df_start_volume['Time Interval'] = df_start_volume['Passing Time'].dt.floor('15min')
            volume_report = df_start_volume.groupby('Time Interval').agg(vehicle_count=('License Plate', 'nunique')).reset_index()
            
            fig_volume = go.Figure()
            fig_volume.add_trace(go.Bar(x=volume_report['Time Interval'], y=volume_report['vehicle_count'], name='Vehicle Count', hovertemplate="<b>Time</b>: %{x|%Y-%m-%d %H:%M}<br><b>Vehicles Started</b>: %{y}<extra></extra>"))
            fig_volume.update_layout(title=f"Vehicle Volume at Start Point: {start_cp}", xaxis_title="Time (15 min intervals)", yaxis_title="Number of Vehicles", height=400, bargap=0.2)
            volume_html = pio.to_html(fig_volume, full_html=False)
        else:
            volume_html = f"<h3>Vehicle Volume at Start Point: {start_cp}</h3><p>No vehicles detected at this start point in the selected period.</p>"

        # Combine both graphs for this route and add a separator
        route_graphs.append(travel_time_html + volume_html + "<hr>")

    if not route_graphs:
        date_msg = f"on {target_date.strftime('%Y-%m-%d')}" if target_date else ""
        checkpoints = df['Device Name'].unique().tolist()
        msg = f"<p>No data found for any routes {date_msg}.<br>Available checkpoints in data: {checkpoints}</p>"
        return [msg], last_updated

    return route_graphs, last_updated

# --- Flask Route ---

@app.route("/")
def dashboard():
    """Renders the main dashboard page."""
    date_filter = request.args.get("date")
    graphs, last_updated = process_data(date_filter)
    return render_template("dashboard.html", graphs=graphs, last_updated=last_updated, selected_date=date_filter)

# Required for Vercel deployment
handler = app
