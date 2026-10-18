import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.param import Param

# Base API Endpoints
COUNT_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-monthly-counts-per-hour/records"
LOCATION_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-sensor-locations/records"

# Default fallback values
DEFAULT_LOCATIONS = "66, 3, 59, 21, 63, 20, 30, 292"
DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")


@dag(
    dag_id="melbourne_pedestrian_traffic_pipeline",
    description="Automated weekly pedestrian traffic ingestion with customizable date ranges and locations via Airflow Params.",
    # Runs automatically every Monday at 02:00 AM UTC (or manually whenever you want)
    schedule="0 2 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "start_date": Param(
            default="",
            type=["string", "null"],
            description="Start date (YYYY-MM-DD). If left empty, automatically defaults to 7 days ago."
        ),
        "end_date": Param(
            default="",
            type=["string", "null"],
            description="End date (YYYY-MM-DD). If left empty, automatically defaults to yesterday."
        ),
        "start_hour": Param(
            default=8,
            type="integer",
            minimum=0,
            maximum=23,
            description="Start hour of the day (e.g. 8 for 8:00 AM)"
        ),
        "end_hour": Param(
            default=20,
            type="integer",
            minimum=0,
            maximum=23,
            description="End hour of the day (e.g. 20 for 8:00 PM)"
        ),
        "location_ids": Param(
            default=DEFAULT_LOCATIONS,
            type="string",
            description="Comma-separated sensor location IDs (e.g., '66, 3, 59, 21, 63, 20, 30, 292')"
        ),
    },
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["melbourne", "pedestrian", "weekly", "parameterized"]
)
def melbourne_pedestrian_traffic_pipeline():

    @task
    def resolve_parameters(**context) -> dict:
        """
        Extracts parameters from DAG run configuration.
        If dates are not specified, dynamically computes the previous 7 days (last week).
        """
        params = context.get("params", {})
        
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        start_hour = params.get("start_hour", 8)
        end_hour = params.get("end_hour", 20)
        loc_str = str(params.get("location_ids", DEFAULT_LOCATIONS)).strip()

        # Clean locations list
        clean_locs = [x.strip() for x in loc_str.split(",") if x.strip().isdigit()]
        final_locs_str = ", ".join(clean_locs) if clean_locs else DEFAULT_LOCATIONS

        # Dynamic fallback for dates if empty
        today = datetime.now().date()
        if not start_date:
            # 7 days ago (e.g. last Monday)
            start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date:
            # 1 day ago (e.g. last Sunday)
            end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

        resolved = {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "start_hour": int(start_hour),
            "end_hour": int(end_hour),
            "locations_str": final_locs_str
        }

        print("=" * 60)
        print("PIPELINE RUN CONFIGURATION:")
        print(f" - Date Range : {resolved['start_date']} to {resolved['end_date']}")
        print(f" - Daily Hours: {resolved['start_hour']}:00 to {resolved['end_hour']}:00")
        print(f" - Sensor IDs : {resolved['locations_str']}")
        print("=" * 60)
        return resolved

    @task
    def extract_sensor_locations(config: dict) -> list:
        """Fetches metadata (name, coordinates) for the targeted sensor locations."""
        locs_str = config["locations_str"]
        print(f"Extracting sensor locations for: {locs_str}")
        params = {
            "where": f"location_id in ({locs_str})",
            "limit": 100,
            "offset": 0,
            "order_by": "location_id asc"
        }
        resp = requests.get(LOCATION_URL, params=params, timeout=30)
        resp.raise_for_status()
        records = resp.json().get("results", [])
        print(f"Retrieved {len(records)} sensor location records.")
        return records

    @task
    def extract_traffic_counts(config: dict) -> list:
        """
        Fetches hourly pedestrian counts for the target sensors and date range.
        Uses limit=100 pagination.
        """
        where_clause = (
            f"location_id in ({config['locations_str']}) and "
            f"sensing_date >= '{config['start_date']}' and sensing_date <= '{config['end_date']}' and "
            f"hourday >= {config['start_hour']} and hourday <= {config['end_hour']}"
        )
        print(f"Querying counts with filter:\n{where_clause}")

        all_records = []
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

            all_records.extend(batch)
            print(f"Call #{call_count}: fetched {len(batch)} records (Total: {len(all_records)} / {total_count})")

            offset += limit
            if offset >= total_count:
                break

        print(f"Finished count extraction: {len(all_records)} total records in {call_count} API calls.")
        return all_records

    @task
    def transform_and_save(sensors: list, counts: list, config: dict) -> str:
        """Joins count data with sensor details and saves clean CSV & Parquet files."""
        os.makedirs(DATA_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_tag = f"{config['start_date']}_to_{config['end_date']}"

        df_sensors = pd.DataFrame(sensors)
        df_counts = pd.DataFrame(counts)

        if df_counts.empty:
            print("No counts data returned for this query range.")
            return "No data processed."

        # Keep relevant sensor location columns
        sensor_cols = [c for c in ["location_id", "sensor_description", "status", "latitude", "longitude"] if c in df_sensors.columns]
        df_sensors_clean = df_sensors[sensor_cols] if not df_sensors.empty else pd.DataFrame(columns=["location_id"])

        # Join counts with sensor details
        df_joined = pd.merge(df_counts, df_sensors_clean, on="location_id", how="left")

        # Save latest and historical run files with 'airflow_' prefix
        counts_csv = os.path.join(DATA_DIR, f"airflow_traffic_counts_{date_tag}_{timestamp}.csv")
        sensors_csv = os.path.join(DATA_DIR, "airflow_sensor_locations_latest.csv")
        joined_csv = os.path.join(DATA_DIR, f"airflow_pedestrian_enriched_{date_tag}_{timestamp}.csv")
        latest_csv = os.path.join(DATA_DIR, "airflow_pedestrian_latest.csv")  # Always points to newest data for dashboard
        joined_parquet = os.path.join(DATA_DIR, f"airflow_pedestrian_enriched_{date_tag}_{timestamp}.parquet")

        df_counts.to_csv(counts_csv, index=False)
        if not df_sensors.empty:
            df_sensors.to_csv(sensors_csv, index=False)

        df_joined.to_csv(joined_csv, index=False)
        df_joined.to_csv(latest_csv, index=False)
        df_joined.to_parquet(joined_parquet, index=False)

        print(f"\n[Success] Processed and saved {len(df_joined)} records:")
        print(f" - Enriched CSV : {joined_csv}")
        print(f" - Latest CSV   : {latest_csv}")
        print(f" - Parquet File : {joined_parquet}")
        return latest_csv

    # DAG Task Dependencies
    run_cfg = resolve_parameters()
    sensors_data = extract_sensor_locations(config=run_cfg)
    counts_data = extract_traffic_counts(config=run_cfg)
    transform_and_save(sensors=sensors_data, counts=counts_data, config=run_cfg)


# Instantiate the DAG
melbourne_pedestrian_dag = melbourne_pedestrian_traffic_pipeline()
