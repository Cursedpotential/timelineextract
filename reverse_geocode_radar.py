import pandas as pd
import json
import os
import requests
import sqlite3
import sys
import time

RADAR_API_KEY = "prj_test_sk_fa8442db575001bba846e9d0cda0e30345db7c30"
CACHE_DB = "radar_geocode_cache.db"


def load_dataframe(input_path):
    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(input_path)
    elif ext == ".json":
        with open(input_path, "r") as f:
            data = json.load(f)
        # Try to flatten the JSON into a DataFrame
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
    return conn


def get_cached_address(conn, lat, lng):
    c = conn.cursor()
    c.execute("SELECT address, label FROM geocode_cache WHERE lat=? AND lng=?", (lat, lng))
    row = c.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def cache_address(conn, lat, lng, address, label=""):
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO geocode_cache (lat, lng, address, label) VALUES (?, ?, ?, ?)", (lat, lng, address, label))
    conn.commit()


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


def main():
    if len(sys.argv) != 3:
        print("Usage: reverse_geocode_radar.py input.csv/json output.csv")
        sys.exit(1)

    df = load_dataframe(sys.argv[1])
    conn = init_cache()

    # Determine which columns to use for coordinates
    if "latitude" in df.columns and "longitude" in df.columns:
        lat_col, lng_col = "latitude", "longitude"
    else:
        print(f"Could not find latitude/longitude columns. Available columns: {list(df.columns)}")
        sys.exit(1)
    if "end_latitude" in df.columns and "end_longitude" in df.columns:
        end_lat_col, end_lng_col = "end_latitude", "end_longitude"
    else:
        print(f"Could not find end_latitude/end_longitude columns. Available columns: {list(df.columns)}")
        sys.exit(1)

    start_addresses = []
    start_labels = []
    end_addresses = []
    end_labels = []
    for idx, row in df.iterrows():
        # Start address
        lat, lng = row[lat_col], row[lng_col]
        if pd.notnull(lat) and pd.notnull(lng):
            addr, label = get_cached_address(conn, lat, lng)
            if addr is None:
                addr = radar_reverse_geocode(lat, lng)
                cache_address(conn, lat, lng, addr)
                label = ''
                time.sleep(0.25)
            start_addr = addr
            start_label = label
        else:
            start_addr = ""
            start_label = ""
        start_addresses.append(start_addr)
        start_labels.append(start_label)
        # End address
        end_lat, end_lng = row[end_lat_col], row[end_lng_col]
        if pd.notnull(end_lat) and pd.notnull(end_lng):
            addr, label = get_cached_address(conn, end_lat, end_lng)
            if addr is None:
                addr = radar_reverse_geocode(end_lat, end_lng)
                cache_address(conn, end_lat, end_lng, addr)
                label = ''
                time.sleep(0.25)
            end_addr = addr
            end_label = label
        else:
            end_addr = ""
            end_label = ""
        end_addresses.append(end_addr)
        end_labels.append(end_label)

    # Insert address and label columns before the coordinate columns
    insert_at = df.columns.get_indexer([lat_col])[0]
    df.insert(insert_at, "start_address", start_addresses)
    df.insert(insert_at + 1, "start_label", start_labels)
    insert_at_end = df.columns.get_indexer([end_lat_col])[0]
    df.insert(insert_at_end, "end_address", end_addresses)
    df.insert(insert_at_end + 1, "end_label", end_labels)

    df.to_csv(sys.argv[2], index=False)
    print(f"Wrote {len(df)} records with Radar addresses to {sys.argv[2]}")

if __name__ == "__main__":
    main()
