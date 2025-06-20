import pandas as pd
import json
import os
import requests
import sqlite3
import sys
import time
import math
from datetime import datetime, time as dtime

RADAR_API_KEY = "prj_test_sk_fa8442db575001bba846e9d0cda0e30345db7c30"
CACHE_DB = "radar_geocode_cache.db"

def load_dataframe(input_path):
    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(input_path)
    elif ext == ".json":
        with open(input_path, "r") as f:
            data = json.load(f)
        if isinstance(data, dict) and "semanticSegments" in data:
            df = pd.json_normalize(data["semanticSegments"])
        elif isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            raise ValueError("Unsupported JSON structure.")
    else:
        raise ValueError("Unsupported file extension. Use .csv or .json")
    return df

def init_cache():
    conn = sqlite3.connect(CACHE_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            lat REAL,
            lng REAL,
            address TEXT,
            label TEXT,
            PRIMARY KEY (lat, lng)
        )
    """)
    conn.commit()
    
    # Print cache statistics
    c.execute("SELECT COUNT(*) FROM geocode_cache")
    count = c.fetchone()[0]
    print(f"Connected to cache database: {CACHE_DB}")
    print(f"Current cache contains {count} addresses")
    return conn

def get_cached_address(conn, lat, lng):
    c = conn.cursor()
    c.execute("SELECT address, label FROM geocode_cache WHERE lat=? AND lng=?", (lat, lng))
    row = c.fetchone()
    if row:
        print(f"Cache hit for coordinates ({lat}, {lng})")
        return row[0], row[1]
    print(f"Cache miss for coordinates ({lat}, {lng})")
    return None, None

def cache_address(conn, lat, lng, address, label=""):
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO geocode_cache (lat, lng, address, label) VALUES (?, ?, ?, ?)", (lat, lng, address, label))
    conn.commit()
    print(f"Cached new address for coordinates ({lat}, {lng})")

def radar_reverse_geocode(lat, lng):
    url = f"https://api.radar.io/v1/geocode/reverse?coordinates={lat},{lng}"
    headers = {"Authorization": RADAR_API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            address = data.get("addresses", [{}])[0].get("formattedAddress", "")
            return address
        else:
            print(f"Radar API error: {response.status_code} {response.text}")
            return ""
    except Exception as e:
        print(f"Radar API exception: {e}")
        return ""

def combine_latlng(lat, lng):
    if pd.notnull(lat) and pd.notnull(lng):
        return f"{lat},{lng}"
    return ""

def google_maps_link(lat, lng):
    if pd.notnull(lat) and pd.notnull(lng):
        return f"https://www.google.com/maps?q={lat},{lng}"
    return ""

def format_duration(duration_min):
    if pd.isnull(duration_min):
        return ""
    try:
        mins = int(round(float(duration_min)))
        hours = mins // 60
        mins = mins % 60
        return f"{hours}:{mins:02d}"
    except Exception:
        return ""

def format_confidence(conf):
    if pd.isnull(conf):
        return ""
    try:
        return f"{int(round(float(conf)*100))}%"
    except Exception:
        return ""

def is_overnight(start_time_str, end_time_str):
    try:
        st = datetime.strptime(start_time_str, '%I:%M %p').time()
        et = datetime.strptime(end_time_str, '%I:%M %p').time()
        overnight_start = dtime(23, 0)
        overnight_end = dtime(7, 0)
        # If event starts before 7 AM or ends after 11 PM, or spans midnight
        if st >= overnight_start or et <= overnight_end:
            return True
        if st > et:
            return True
        return False
    except Exception:
        return False

def split_latlng(val):
    """Split a combined lat,lng string into separate values"""
    if pd.isnull(val):
        return None, None
    if isinstance(val, str) and "," in val:
        lat, lng = val.split(",")
        lat = lat.replace('°', '').replace('\u00b0', '').strip()
        lng = lng.replace('°', '').replace('\u00b0', '').strip()
        try:
            return float(lat), float(lng)
        except Exception:
            return None, None
    return None, None

def check_overnight(start_time_str, end_time_str):
    """Check if an event occurs during overnight hours (11 PM - 7 AM)"""
    try:
        st = datetime.strptime(start_time_str, '%I:%M %p').time()
        et = datetime.strptime(end_time_str, '%I:%M %p').time()
        overnight_start = dtime(23, 0)  # 11 PM
        overnight_end = dtime(7, 0)     # 7 AM
        # If event starts before 7 AM or ends after 11 PM, or spans midnight
        if st >= overnight_start or et <= overnight_end:
            return True
        if st > et:  # Spans midnight
            return True
        return False
    except Exception:
        return False

def auto_output_filename(input_path, step=1):
    base, ext = os.path.splitext(input_path)
    if step == 1:
        return f"{base}_processed.csv"
    else:
        return f"{base}_geocoded.csv"

def main():
    if len(sys.argv) < 2:
        print("Usage: reverse_geocode_radar_v3.py input1.csv/json [input2.csv/json ...]")
        print("Examples:")
        print("  reverse_geocode_radar_v3.py file1_processed.csv")
        print("  reverse_geocode_radar_v3.py file1_processed.csv file2_processed.csv")
        sys.exit(1)
    input_files = sys.argv[1:]
    total_files = len(input_files)
    print(f"\nProcessing {total_files} file{'s' if total_files > 1 else ''}...")
    for idx, input_path in enumerate(input_files, 1):
        try:
            print(f"\n[{idx}/{total_files}] Processing: {input_path}")
            # Step detection: use _geocoded for output
            if '_processed' in input_path:
                output_path = auto_output_filename(input_path, step=2)
            else:
                output_path = auto_output_filename(input_path, step=1)
            print(f"Loading input file: {input_path}")
            df = load_dataframe(input_path)
            print(f"Loaded {len(df)} records")
            print("Initializing cache database...")
            conn = init_cache()
            print("Analyzing coordinate columns...")
            start_lat_col = next((c for c in df.columns if c in ["latitude", "start_latitude", "activity.start.latLng", "visit.topCandidate.placeLocation.latLng"]), None)
            start_lng_col = next((c for c in df.columns if c in ["longitude", "start_longitude", "activity.start.latLng", "visit.topCandidate.placeLocation.latLng"]), None)
            end_lat_col = next((c for c in df.columns if c in ["end_latitude", "activity.end.latLng"]), None)
            end_lng_col = next((c for c in df.columns if c in ["end_longitude", "activity.end.latLng"]), None)
            print(f"Found coordinate columns: {start_lat_col}, {start_lng_col}, {end_lat_col}, {end_lng_col}")
            print("Processing coordinates...")
            if start_lat_col and ("latLng" in start_lat_col or "placeLocation.latLng" in start_lat_col):
                print("Extracting embedded start coordinates...")
                df[["start_latitude", "start_longitude"]] = df[start_lat_col].apply(lambda x: pd.Series(split_latlng(x)))
            else:
                df["start_latitude"] = df[start_lat_col] if start_lat_col else None
                df["start_longitude"] = df[start_lng_col] if start_lng_col else None
            if end_lat_col and ("latLng" in end_lat_col):
                print("Extracting embedded end coordinates...")
                df[["end_latitude", "end_longitude"]] = df[end_lat_col].apply(lambda x: pd.Series(split_latlng(x)))
            else:
                df["end_latitude"] = df[end_lat_col] if end_lat_col else None
                df["end_longitude"] = df[end_lng_col] if end_lng_col else None
            print("Processing records and fetching addresses...")
            total_records = len(df)
            start_addresses = []
            start_labels = []
            end_addresses = []
            end_labels = []
            start_latlngs = []
            end_latlngs = []
            start_gmaps = []
            end_gmaps = []
            overnight_flags = []
            for rec_idx, row in df.iterrows():
                if rec_idx % 10 == 0:
                    print(f"Processing record {rec_idx+1} of {total_records} ({(rec_idx+1)/total_records*100:.1f}%)")
                lat, lng = row["start_latitude"], row["start_longitude"]
                start_latlng = combine_latlng(lat, lng)
                start_latlngs.append(start_latlng)
                start_gmaps.append(google_maps_link(lat, lng))
                addr, label = get_cached_address(conn, lat, lng) if pd.notnull(lat) and pd.notnull(lng) else ("", "")
                if not addr and pd.notnull(lat) and pd.notnull(lng):
                    print(f"Fetching address for start coordinates: {lat},{lng}")
                    addr = radar_reverse_geocode(lat, lng)
                    cache_address(conn, lat, lng, addr)
                    label = ''
                    time.sleep(0.25)
                start_addresses.append(addr)
                start_labels.append(label)
                end_lat, end_lng = row["end_latitude"], row["end_longitude"]
                end_latlng = combine_latlng(end_lat, end_lng)
                end_latlngs.append(end_latlng)
                end_gmaps.append(google_maps_link(end_lat, end_lng))
                addr, label = get_cached_address(conn, end_lat, end_lng) if pd.notnull(end_lat) and pd.notnull(end_lng) else ("", "")
                if not addr and pd.notnull(end_lat) and pd.notnull(end_lng):
                    print(f"Fetching address for end coordinates: {end_lat},{end_lng}")
                    addr = radar_reverse_geocode(end_lat, end_lng)
                    cache_address(conn, end_lat, end_lng, addr)
                    label = ''
                    time.sleep(0.25)
                end_addresses.append(addr)
                end_labels.append(label)
                overnight = check_overnight(row.get('start_time', ''), row.get('end_time', ''))
                overnight_flags.append(overnight)
                if overnight:
                    print(f"Detected overnight event: {row.get('start_time', '')} to {row.get('end_time', '')}")
            print("Formatting fields...")
            df["duration_min"] = df["duration_min"].apply(format_duration)
            df["confidence"] = df["confidence"].apply(format_confidence)
            df["distance_miles"] = df.get("distance_miles", "")
            print("Adding computed columns...")
            df["start_address"] = start_addresses
            df["start_label"] = start_labels
            df["end_address"] = end_addresses
            df["end_label"] = end_labels
            df["start_google_map_link"] = start_gmaps
            df["end_google_map_link"] = end_gmaps
            df["start_latlng"] = start_latlngs
            df["end_latlng"] = end_latlngs
            df["overnight"] = overnight_flags
            print("Reordering columns...")
            out_cols = [
                "id", "start_date", "start_day", "start_time", "end_date", "end_day", "end_time", 
                "duration_min", "overnight", "type",
                "start_address", "start_label", "end_address", "end_label", "distance_miles", "confidence",
                "start_google_map_link", "end_google_map_link", "start_latitude", "start_longitude", 
                "end_latitude", "end_longitude", "description", "accuracy"
            ]
            for col in out_cols:
                if col not in df.columns:
                    df[col] = ""
            df = df[out_cols]
            print(f"Writing output to: {output_path}")
            df.to_csv(output_path, index=False)
            print(f"Successfully wrote {len(df)} records with Radar addresses to {output_path}")
            print("\nProcessing Statistics:")
            print(f"Total records processed: {total_records}")
            print(f"Records with start addresses: {len([a for a in start_addresses if a])}")
            print(f"Records with end addresses: {len([a for a in end_addresses if a])}")
            print(f"Overnight events detected: {sum(overnight_flags)}")
            print("Done!")
        except Exception as e:
            print(f"Error processing {input_path}: {str(e)}")
            continue
    print(f"\nProcessing complete! {total_files} file{'s' if total_files > 1 else ''} processed.")

if __name__ == "__main__":
    main()
