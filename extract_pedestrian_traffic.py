import os
import requests
import pandas as pd
from datetime import datetime

# API Endpoint
BASE_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-past-hour-counts-per-minute/records"

# Query parameters: offset 0, max 100 records
PARAMS = {
    "where": 'location_id in (142,5,35,41,29,25,184,47) and sensing_time in ("13:00","13:30","14:00","14:30","15:00","15:30")',
    "limit": 100,
    "offset": 0,
    "order_by": "sensing_datetime desc"
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def extract_pedestrian_data(url: str, params: dict) -> list:
    """Fetches up to 100 records starting from offset 0."""
    print("Calling Melbourne Open Data API...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    total_count = data.get("total_count", 0)
    results = data.get("results", [])

    print(f"Total matching records in dataset: {total_count}")
    print(f"Extracted records in this batch: {len(results)}")
    return results


def save_locally(records: list, output_dir: str):
    """Saves records to CSV and JSON locally."""
    if not records:
        print("No records found to save.")
        return

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    df = pd.DataFrame(records)

    if "sensing_datetime" in df.columns:
        df["sensing_datetime"] = pd.to_datetime(df["sensing_datetime"])

    csv_path = os.path.join(output_dir, f"pedestrian_data_{timestamp}.csv")
    json_path = os.path.join(output_dir, f"pedestrian_data_{timestamp}.json")

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", date_format="iso", indent=2)

    print("\nSaved files:")
    print(f" - CSV:  {os.path.abspath(csv_path)}")
    print(f" - JSON: {os.path.abspath(json_path)}")
    print("\nSample Data:")
    print(df.head())


if __name__ == "__main__":
    records = extract_pedestrian_data(BASE_URL, PARAMS)
    save_locally(records, OUTPUT_DIR)
