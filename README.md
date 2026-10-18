# Melbourne Pedestrian Traffic Intelligence 🚶‍♂️📊

An automated data pipeline and interactive visual intelligence dashboard designed to monitor and analyze pedestrian foot traffic and city centre vitality across the City of Melbourne.

This project was prototyped and accelerated using **Antigravity (powered by Gemini 3.8 Flash)**, orchestrating data ingestion with **Apache Airflow** in **Docker Compose**, and serving an interactive, zero-overhead analytical dashboard.

---

## 🎯 Project Objectives

1. **Automated Public Data Ingestion via Apache Airflow**  
   Leverage Apache Airflow to programmatically query, extract, paginate, and process publicly available open data from the [City of Melbourne Open Data Portal](https://data.melbourne.vic.gov.au/).
2. **AI-Assisted Rapid Prototyping with Antigravity (Gemini 3.8 Flash)**  
   Utilize Antigravity (Gemini 3.8 Flash) to rapidly design and implement end-to-end architecture: from REST API querying, schema transformations, and parameterized Airflow DAG generation to building a polished, interactive analytics web interface.
3. **Audience-Centric, Intuitive Data Visualization**  
   Translate complex spatio-temporal pedestrian count records into intuitive visualizations that clearly convey city centre crowd vitality, peak footfall hours, and weekday vs. weekend patterns.

---

## 💡 Inspiration & Background

Earlier experiments with other datasets (such as retail shoe pricing) proved challenging for recurring orchestration due to erratic updates, anti-scraping walls, and inconsistent schemas. 

In contrast, the **City of Melbourne Open Data API** provided a robust, structured, and realistic public dataset with continuous hourly pedestrian observations and geographical sensor metadata. 

By containerizing Airflow with **Docker Compose**, the data ingestion pipeline is scheduled to run on a **weekly cadence** (every Monday), automatically capturing the preceding week's activity. The accompanying dashboard was designed to visualize city centre "buzz"—identifying where people gather near Queen Victoria Market and Chinatown, how foot traffic varies with each day, and how Melbourne pulses from morning to evening.

---

## 🏗️ Architecture & Pipeline Flow

```
                      +---------------------------------------+
                      |     City of Melbourne Open Data       |
                      |  - Pedestrian Sensor Locations API    |
                      |  - Hourly Foot Traffic Counts API     |
                      +-------------------+-------------------+
                                          |
                                          | (REST API / JSON)
                                          v
+-----------------------------------------------------------------------------------+
| Apache Airflow DAG (Docker Compose / LocalExecutor)                               |
| Schedule: Weekly ("0 2 * * 1" - Mondays 02:00 AM UTC)                             |
|                                                                                   |
|  [resolve_parameters]                                                             |
|         │                                                                         |
|         ├──► [extract_sensor_locations] ──┐                                       |
|         │                                 │                                       |
|         └──► [extract_traffic_counts]   ──┴──► [transform_and_save]                |
|               (Paginated API batches)                 │                           |
+-------------------------------------------------------┼---------------------------+
                                                        │
                                                        ▼ (CSV & Parquet)
                                         +------------------------------+
                                         |     /data Directory          |
                                         | airflow_pedestrian_latest.csv|
                                         +--------------+---------------+
                                                        │
                                                        ▼
                                         +------------------------------+
                                         |    Interactive Dashboard     |
                                         |     (serve_dashboard.py)     |
                                         |  Leaflet • Plotly • Tailwind |
                                         |     http://localhost:8501    |
                                         +------------------------------+
```

### Key Components:
- **API Extraction**: Fetches metadata for key sensor locations (e.g., Flinders Street Station, Southern Cross Station, Bourke Street Mall, Melbourne Central) and hourly pedestrian counts between 8:00 AM and 8:00 PM.
- **Airflow Orchestration**:
  - **Dynamic Date Windowing**: Automatically defaults to the previous 7 days when triggered on schedule, or accepts user-provided dates via Airflow UI runtime parameters.
  - **Robust Pagination**: Iteratively steps through API results with automatic offset increments.
  - **Transformation & Storage**: Enriches raw counts with sensor geospatial data and outputs both timestamped historical archives and a pinned `airflow_pedestrian_latest.csv`.
- **Interactive Visual Dashboard**: A standalone, zero-build web application rendering high-performance client-side charts without requiring bulky web frameworks.

---

## 📊 Visualizations & Insights

The dashboard translates raw counts into four targeted visual perspectives:

1. **Geographic Sensor Map (Google Maps Light style)**  
   - Interactive Leaflet map displaying active sensor locations across Melbourne's CBD.
   - Dynamic marker radiuses scale proportionally to footfall volume according to the active filters.
   - Clickable popups with total counts and sensor details.
2. **Pedestrian Count in 1-Hour Blocks (Per Location)**  
   - Linear trend chart with 13 discrete hourly points (08:00 to 20:00).
   - Allows comparison of commute peaks (morning rush, lunch hour, and evening commute) across transit hubs and commercial streets.
3. **Footfall Share by Location**  
   - Proportional donut/pie chart revealing the distribution of total traffic across monitored sensors.
4. **Average Weekday vs. Average Weekend (4-Hour Blocks)**  
   - Grouped bar comparison split into intuitive 4-hour time windows:
     - 🟡 **Morning** (08:00 - 12:00)
     - 🌸 **Afternoon** (12:00 - 16:00)
     - 🔵 **Evening** (16:00 - 20:00)
   - Highlights commuter-driven workday footfall vs. leisurely weekend shopping and nightlife patterns.

---

## 📁 Repository Structure

```
melb_pedestrian/
├── dags/
│   └── melbourne_pedestrian_pipeline.py  # Airflow DAG (extract, transform, save)
├── data/                                 # Ingested datasets (CSV and Parquet)
│   ├── airflow_pedestrian_latest.csv     # Pinned latest dataset read by dashboard
│   └── ...                               # Historical extraction files
├── extract_pedestrian_traffic.py         # Standalone count extraction script
├── extract_sensor_locations.py           # Standalone sensor extraction script
├── run_pipeline_local.py                 # Standalone end-to-end pipeline (no Docker)
├── serve_dashboard.py                    # Lightweight Python server & dashboard generator
├── dashboard.html                        # Generated interactive HTML dashboard
├── docker-compose.yaml                   # Apache Airflow 2.9 stack (Postgres + LocalExecutor)
├── .env                                  # Environment configurations for Airflow
└── README.md                             # Project documentation
```

---

## 🚀 Quick Start Guide

### Prerequisites
- [Docker & Docker Compose](https://www.docker.com/) (for Airflow orchestration)
- [Python 3.10+](https://www.python.org/) with `pandas` and `requests`

```bash
pip install pandas requests pyarrow
```

---

### Option A: Running with Apache Airflow (Docker Compose)

1. **Initialize Airflow Environment:**
   ```bash
   docker compose up airflow-init
   ```

2. **Start Airflow Services:**
   ```bash
   docker compose up -d
   ```

3. **Access the Airflow Web UI:**
   - URL: `http://localhost:8080`
   - Default Username: `airflow`
   - Default Password: `airflow`

4. **Trigger the DAG:**
   - Locate `melbourne_pedestrian_traffic_pipeline`.
   - Toggle the DAG to **Active** (or click **Trigger DAG** with custom dates).
   - Ingested files will be stored in `./data/airflow_pedestrian_latest.csv`.

---

### Option B: Running the Pipeline Locally (Without Airflow)

If you prefer to extract data directly without running Docker:

```bash
python run_pipeline_local.py
```

This will fetch the configured date range (8:00 AM - 8:00 PM), merge the sensor metadata, and write the output files into the `data/` directory.

---

### Launching the Dashboard

Once data has been generated (either via Airflow or the local script):

```bash
python serve_dashboard.py
```

- Automatically loads `data/airflow_pedestrian_latest.csv`.
- Spins up a local web server at **`http://localhost:8501`**.
- Opens your default web browser to display the interactive dashboard.

---

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow 2.9 (Docker Compose, PostgreSQL backend)
- **Data Engineering**: Python, Pandas, Requests, PyArrow (Parquet)
- **Prototyping & Code Generation**: Antigravity (Gemini 3.8 Flash)
- **Visualization & UI**: HTML5, Tailwind CSS, Leaflet.js, Plotly.js, Lucide Icons
- **Data Source**: City of Melbourne Open Data Platform API v2.1
