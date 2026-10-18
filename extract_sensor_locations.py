import os
import requests
import pandas as pd

# API Endpoint for Melbourne Sensor Locations
SENSOR_LOCATIONS_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-sensor-locations/records"

# Target location IDs from your list
LOCATION_IDS = [5, 47, 35, 142, 184, 29, 41, 25]

# Build WHERE clause
LOCATION_IDS_STR = ", ".join(map(str, LOCATION_IDS))
WHERE_CLAUSE = f"location_id in ({LOCATION_IDS_STR})"

PARAMS = {
    "where": WHERE_CLAUSE,
    "limit": 100,
    "offset": 0,
    "order_by": "location_id asc"
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def extract_sensor_locations(url: str, params: dict) -> list:
    """Fetches sensor location metadata for the requested location IDs."""
    print("Calling Melbourne Open Data API for sensor locations...")
    print(f"Filter: {params['where']}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    total_count = data.get("total_count", 0)
    results = data.get("results", [])

    print(f"Total matching sensor locations found: {total_count}")
    print(f"Extracted records: {len(results)}")
    return results


def save_sensor_locations(records: list, output_dir: str):
    """Saves sensor locations to CSV and JSON locally."""
    if not records:
        print("No sensor records found to save.")
        return

    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(records)

    csv_path = os.path.join(output_dir, "sensor_locations.csv")
    json_path = os.path.join(output_dir, "sensor_locations.json")

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    print("\nSaved files:")
    print(f" - CSV:  {os.path.abspath(csv_path)}")
    print(f" - JSON: {os.path.abspath(json_path)}")
    
    print("\nExtracted Sensor Locations Overview:")
    columns_to_show = [c for c in ["location_id", "sensor_description", "status", "latitude", "longitude"] if c in df.columns]
    print(df[columns_to_show].to_string(index=False))


if __name__ == "__main__":
    records = extract_sensor_locations(SENSOR_LOCATIONS_URL, PARAMS)
    save_sensor_locations(records, OUTPUT_DIR)
