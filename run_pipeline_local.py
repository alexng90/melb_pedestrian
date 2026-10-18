import os
import requests
import pandas as pd
from datetime import datetime

# API Endpoints
COUNT_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-monthly-counts-per-hour/records"
LOCATION_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-sensor-locations/records"

# Target sensor location IDs
TARGET_LOCATION_IDS = [66, 3, 59, 21, 63, 20, 30, 292]
LOCATIONS_STR = ", ".join(map(str, TARGET_LOCATION_IDS))

# Filter criteria: 5 to 11 Oct 2026, 8am to 8pm
START_DATE = "2026-10-05"
END_DATE = "2026-10-11"
START_HOUR = 8   # 8:00 AM
END_HOUR = 20    # 8:00 PM

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def run_pipeline():
    print("=" * 70)
    print("MELBOURNE PEDESTRIAN TRAFFIC EXTRACTION (5 - 11 OCT 2026, 8AM - 8PM)")
    print("Target Sensor IDs:", LOCATIONS_STR)
    print("=" * 70)

    # 1. Fetch Sensor Metadata
    print("\n[Step 1/3] Fetching Sensor Locations metadata...")
    loc_params = {
        "where": f"location_id in ({LOCATIONS_STR})",
        "limit": 100,
        "offset": 0,
        "order_by": "location_id asc"
    }
    loc_resp = requests.get(LOCATION_URL, params=loc_params, timeout=30)
    loc_resp.raise_for_status()
    sensors = loc_resp.json().get("results", [])
    print(f" -> Successfully retrieved {len(sensors)} sensor locations (1 API call).")

    # 2. Fetch Traffic Counts with Pagination
    where_clause = (
        f"location_id in ({LOCATIONS_STR}) and "
        f"sensing_date >= '{START_DATE}' and sensing_date <= '{END_DATE}' and "
        f"hourday >= {START_HOUR} and hourday <= {END_HOUR}"
    )
    print(f"\n[Step 2/3] Fetching traffic count records...")
    print(f"Filter: {where_clause}")

    all_counts = []
    limit = 100
    offset = 0
    call_count = 0

    while True:
        params = {
            "where": where_clause,
            "limit": limit,
            "offset": offset,
            "order_by": "sensing_date asc, hourday asc, location_id asc"
        }
        resp = requests.get(COUNT_URL, params=params, timeout=30)
        resp.raise_for_status()
        call_count += 1

        data = resp.json()
        total_count = data.get("total_count", 0)
        batch = data.get("results", [])

        if not batch:
            break

        all_counts.extend(batch)
        print(f" -> API Call #{call_count}: fetched {len(batch)} records (Progress: {len(all_counts)} / {total_count})")

        offset += limit
        if offset >= total_count:
            break

    print(f" -> Finished count extraction: {len(all_counts)} records in total using ONLY {call_count} API calls!")

    # 3. Transform & Save
    print("\n[Step 3/3] Merging traffic counts with sensor locations and saving...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    df_sensors = pd.DataFrame(sensors)
    df_counts = pd.DataFrame(all_counts)

    sensor_cols = [c for c in ["location_id", "sensor_description", "status", "latitude", "longitude"] if c in df_sensors.columns]
    df_sensors_clean = df_sensors[sensor_cols] if not df_sensors.empty else pd.DataFrame(columns=["location_id"])

    df_joined = pd.merge(df_counts, df_sensors_clean, on="location_id", how="left")

    counts_csv = os.path.join(OUTPUT_DIR, f"traffic_counts_oct5_11_{timestamp}.csv")
    sensors_csv = os.path.join(OUTPUT_DIR, "sensor_locations_latest.csv")
    joined_csv = os.path.join(OUTPUT_DIR, f"pedestrian_oct5_11_enriched_{timestamp}.csv")

    df_counts.to_csv(counts_csv, index=False)
    if not df_sensors.empty:
        df_sensors.to_csv(sensors_csv, index=False)
    df_joined.to_csv(joined_csv, index=False)

    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
    print(f"Files saved in: {OUTPUT_DIR}")
    print(f" - Enriched Dataset : {os.path.basename(joined_csv)}")
    print(f" - Raw Counts       : {os.path.basename(counts_csv)}")
    print(f" - Sensor Locations : {os.path.basename(sensors_csv)}")
    print(f"\nTotal API calls used across whole pipeline: {call_count + 1} calls (Well below 49,000!)")

    print("\nPreview of extracted data:")
    preview_cols = [c for c in ["location_id", "sensor_description", "sensing_date", "hourday", "pedestriancount"] if c in df_joined.columns]
    print(df_joined[preview_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    run_pipeline()
