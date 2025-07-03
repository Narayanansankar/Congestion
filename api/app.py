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
REQUIRED_COLUMNS = ["Device Name", "License Plate", "Passing Time"]

def get_gdrive_service():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("The GOOGLE_CREDENTIALS_JSON environment variable is not set.")
    creds_json = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

def _extract_file_number(filename):
    try:
        number_str = filename.split('_')[-1].split('.')[0]
        return int(number_str)
    except (ValueError, IndexError):
        return -1

def get_latest_file_for_date(service, target_date: date):
    if not GDRIVE_FOLDER_ID:
        raise ValueError("The GDRIVE_FOLDER_ID environment variable is not set.")

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
            pageSize=200,
            fields="files(id, name, modifiedTime)"
        ).execute()

        files_for_day = results.get('files', [])
        if not files_for_day:
            return None, None, None

        sorted_files = sorted(files_for_day, key=lambda f: _extract_file_number(f['name']), reverse=True)
        
        latest_file = sorted_files[0]
        
        if _extract_file_number(latest_file['name']) == -1:
            print(f"No files with the pattern 'anpr_data_*.xlsx' found for {target_date}")
            return None, None, None

        return latest_file['id'], latest_file['name'], latest_file['modifiedTime']
    except HttpError as error:
        print(f"An error occurred while finding the latest file: {error}")
        return None, None, None

def download_file_from_gdrive(service, file_id):
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

def process_data(date_filter_str=None):
    if date_filter_str:
        try:
            target_date = pd.to_datetime(date_filter_str).date()
        except ValueError:
             return [f"<p style='color:red;'>Invalid date format: '{date_filter_str}'. Please use YYYY-MM-DD.</p>"], "Not available"
    else:
        target_date = datetime.now(timezone.utc).date()

    try:
        service = get_gdrive_service()
        file_id, file_name, last_updated_str = get_latest_file_for_date(service, target_date)

        if not file_id:
            return [f"<p>No data file found for {target_date.strftime('%Y-%m-%d')}.</p>"], "Not available"

        file_buffer = download_file_from_gdrive(service, file_id)
        if not file_buffer:
            return [f"<p>Error downloading file '{file_name}' from Google Drive.</p>"], "Not available"

        df = pd.read_excel(file_buffer)
        last_updated = pd.to_datetime(last_updated_str).strftime("%Y-%m-%d %H:%M:%S UTC")

        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            found_columns = ', '.join(df.columns.tolist()) if not df.columns.empty else 'None'
            error_message = (
                f"<p style='color:red; font-family: monospace;'>"
                f"<strong>File Error:</strong> The file '{file_name}' has incorrect headers.<br>"
                f"<strong>Missing Columns:</strong> {', '.join(missing_columns)}<br>"
                f"<strong>Columns Found:</strong> {found_columns}<br>"
                f"Please check the file and ensure the column names match exactly: "
                f"'Device Name', 'License Plate', 'Passing Time'."
                f"</p>"
            )
            return [error_message], last_updated

    except Exception as e:
        return [f"<p style='color:red;'>An error occurred: {e}</p>"], "Not available"

    df["Device Name"] = df["Device Name"].str.upper().str.replace(" C.POST", "", regex=False).str.strip()
    df["License Plate"] = df["License Plate"].str.upper().str.strip()
    df["Passing Time"] = pd.to_datetime(df["Passing Time"], errors='coerce')
    df = df.dropna(subset=REQUIRED_COLUMNS)
    df = df[df["Passing Time"].dt.date == target_date]
    
    if df.empty:
        return [f"<p>The file for {target_date.strftime('%Y-%m-%d')} ('{file_name}') was found but contained no valid data for that date.</p>"], last_updated

    route_graphs = []

    for start_cp, end_cp, google_time in ROUTES:
        # --- Data Prep for this specific route ---
        df_start = df[df["Device Name"] == start_cp]
        df_end = df[df["Device Name"] == end_cp]

        # --- Graph 1: Average Travel Time (from completed trips) ---
        merged = pd.merge(df_start, df_end, on="License Plate", suffixes=("_start", "_end"))
        merged["Travel Time (mins)"] = (merged["Passing Time_end"] - merged["Passing Time_start"]).dt.total_seconds() / 60
        merged = merged[(merged["Travel Time (mins)"] > 0) & (merged["Travel Time (mins)"] <= MAX_TRAVEL_TIME_MINS)]
        
        travel_time_html = ""
        if not merged.empty:
            merged["Time Interval"] = merged["Passing Time_start"].dt.floor("15min")
            report = merged.groupby("Time Interval").agg(
                avg_travel_time=('Travel Time (mins)', 'mean'),
                vehicle_count=('License Plate', 'count')
            ).reset_index()

            fig_travel = go.Figure()
            moderate_level = google_time + MODERATE_CONGESTION_OFFSET
            heavy_level = google_time + HEAVY_CONGESTION_OFFSET
            max_y_val = report["avg_travel_time"].max()
            graph_top = max(heavy_level + 20, max_y_val * 1.1)

            fig_travel.add_hrect(y0=moderate_level, y1=heavy_level, fillcolor="yellow", opacity=0.2, layer="below", line_width=0)
            fig_travel.add_hrect(y0=heavy_level, y1=graph_top, fillcolor="red", opacity=0.2, layer="below", line_width=0)

            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=report["avg_travel_time"], mode='lines+markers', name="Actual Avg Travel Time", customdata=report[['vehicle_count']], hovertemplate="<b>Time</b>: %{x|%H:%M}<br><b>Avg Travel Time</b>: %{y:.1f} mins<br><b>Vehicles Reached</b>: %{customdata[0]}<extra></extra>"))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[google_time] * len(report), mode='lines', name=f"Google Avg: {google_time} mins", line=dict(color='green', dash='dash')))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[moderate_level] * len(report), mode='lines', name=f"Moderate Threshold (+{MODERATE_CONGESTION_OFFSET} mins)", line=dict(color='orange', dash='dash')))
            fig_travel.add_trace(go.Scatter(x=report["Time Interval"], y=[heavy_level] * len(report), mode='lines', name=f"Heavy Threshold (+{HEAVY_CONGESTION_OFFSET} mins)", line=dict(color='red', dash='dash')))
            fig_travel.update_layout(title=f"Avg Travel Time: {start_cp} → {end_cp}", xaxis_title="Time (Trip Start)", yaxis_title="Travel Time (mins)", height=450, yaxis_range=[0, graph_top])
            travel_time_html = pio.to_html(fig_travel, full_html=False)
        else:
            travel_time_html = f"<h3>Avg Travel Time: {start_cp} → {end_cp}</h3><p>No vehicles completed this journey within {int(MAX_TRAVEL_TIME_MINS/60)} hours on the selected date.</p>"

        # --- Graph 2: Vehicle Volume at Start Point ---
        volume_html = ""
        if not df_start.empty:
            df_start_volume = df_start.copy()
            df_start_volume['Time Interval'] = df_start_volume['Passing Time'].dt.floor('15min')
            volume_report = df_start_volume.groupby('Time Interval').agg(vehicle_count=('License Plate', 'nunique')).reset_index()
            
            fig_volume = go.Figure()
            fig_volume.add_trace(go.Bar(x=volume_report['Time Interval'], y=volume_report['vehicle_count'], name='Vehicle Count', hovertemplate="<b>Time</b>: %{x|%H:%M}<br><b>Vehicles Started</b>: %{y}<extra></extra>"))
            fig_volume.update_layout(title=f"Vehicle Volume at Start Point: {start_cp}", xaxis_title="Time (15 min intervals)", yaxis_title="Number of Vehicles", height=400, bargap=0.2)
            volume_html = pio.to_html(fig_volume, full_html=False)
        else:
            volume_html = f"<h3>Vehicle Volume at Start Point: {start_cp}</h3><p>No vehicles detected at this start point on the selected date.</p>"

        # --- Combine both graphs for this route and add a separator ---
        route_graphs.append(travel_time_html + volume_html + "<hr>")

    if not route_graphs:
        checkpoints = df['Device Name'].unique().tolist()
        msg = f"<p>No data found for any routes on {target_date.strftime('%Y-%m-%d')}.<br>Available checkpoints in the data for this day: {checkpoints}</p>"
        return [msg], last_updated

    return route_graphs, last_updated

@app.route("/")
def dashboard():
    date_filter = request.args.get("date")
    graphs, last_updated = process_data(date_filter)
    return render_template("dashboard.html", graphs=graphs, last_updated=last_updated, selected_date=date_filter)

# Required for Vercel deployment
handler = app
