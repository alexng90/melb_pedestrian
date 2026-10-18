import os
import glob
import json
import http.server
import socketserver
import webbrowser
import pandas as pd

PORT = 8501
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def get_latest_data_file():
    # Priority 1: Airflow generated latest pinned file
    airflow_latest = os.path.join(DATA_DIR, "airflow_pedestrian_latest.csv")
    if os.path.exists(airflow_latest):
        return airflow_latest

    # Priority 2: Airflow timestamped enriched files
    airflow_patterns = sorted(glob.glob(os.path.join(DATA_DIR, "airflow_pedestrian_enriched_*.csv")))
    if airflow_patterns:
        return airflow_patterns[-1]

    # Priority 3: Standalone / manual runs fallback
    patterns = [
        os.path.join(DATA_DIR, "pedestrian_oct5_11_enriched_*.csv"),
        os.path.join(DATA_DIR, "pedestrian_enriched_*.csv"),
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    if not files:
        raise FileNotFoundError(f"No enriched pedestrian CSV found in {DATA_DIR}")
    return sorted(files)[-1]


def build_dashboard():
    csv_file = get_latest_data_file()
    print(f"Loading data from: {csv_file}")
    df = pd.read_csv(csv_file)

    # Clean & enrich time dimensions
    df["sensing_date"] = pd.to_datetime(df["sensing_date"])
    df["day_name"] = df["sensing_date"].dt.day_name()
    df["day_of_week"] = df["sensing_date"].dt.dayofweek  # 0=Monday, 6=Sunday
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    df["day_type"] = df["is_weekend"].map({True: "Weekend", False: "Weekday"})

    # Ensure numeric columns
    df["hourday"] = pd.to_numeric(df["hourday"], errors="coerce")
    df["pedestriancount"] = pd.to_numeric(df["pedestriancount"], errors="coerce").fillna(0)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    
    # Fill sensor description fallback
    if "sensor_description" not in df.columns:
        df["sensor_description"] = "Sensor " + df["location_id"].astype(str)
    else:
        df["sensor_description"] = df["sensor_description"].fillna("Sensor " + df["location_id"].astype(str))

    # 1-hour block formatted string: e.g. "08:00 - 09:00", "09:00 - 10:00", etc.
    def assign_1hr_block(hour):
        if pd.isna(hour):
            return "Other"
        h = int(hour)
        return f"{h:02d}:00 - {h+1:02d}:00"

    # 4-hour blocks: Morning (08:00-12:00), Afternoon (12:00-16:00), Evening (16:00-20:00)
    def assign_4hr_block(hour):
        if pd.isna(hour):
            return "Other"
        h = int(hour)
        if 8 <= h < 12:
            return "Morning (08:00 - 12:00)"
        elif 12 <= h < 16:
            return "Afternoon (12:00 - 16:00)"
        elif 16 <= h <= 20:
            return "Evening (16:00 - 20:00)"
        return "Other"

    df["block_1hr"] = df["hourday"].apply(assign_1hr_block)
    df["block_4hr"] = df["hourday"].apply(assign_4hr_block)

    # Convert to JSON for client-side interactive rendering
    records = df[[
        "location_id", "sensor_description", "sensing_date", "day_name",
        "day_of_week", "hourday", "pedestriancount", "latitude", "longitude",
        "block_1hr", "block_4hr", "day_type"
    ]].to_dict(orient="records")

    for r in records:
        r["sensing_date"] = r["sensing_date"].strftime("%Y-%m-%d")

    records_json = json.dumps(records)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Melbourne Pedestrian Traffic Intelligence Dashboard</title>
  
  <!-- Tailwind CSS -->
  <script src="https://cdn.tailwindcss.com"></script>
  
  <!-- Leaflet CSS & JS -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  
  <!-- Plotly JS -->
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

  <!-- Lucide Icons -->
  <script src="https://unpkg.com/lucide@latest"></script>

  <style>
    body {{
      background: #0f172a;
      color: #f8fafc;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }}
    .glass-card {{
      background: rgba(30, 41, 59, 0.7);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 1rem;
    }}
    #map {{
      height: 380px;
      border-radius: 0.75rem;
      z-index: 1;
    }}
    .leaflet-popup-content-wrapper {{
      background: #ffffff;
      color: #0f172a;
      border-radius: 0.5rem;
      box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
    }}
    .leaflet-popup-tip {{
      background: #ffffff;
    }}
    select, input[type="range"] {{
      accent-color: #38bdf8;
    }}
  </style>
</head>
<body class="min-h-screen p-4 md:p-6 lg:p-8">

  <!-- Header -->
  <div class="max-w-7xl mx-auto mb-6">
    <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-800 pb-5">
      <div>
        <div class="flex items-center gap-3">
          <div class="p-2.5 bg-sky-500/10 text-sky-400 rounded-xl border border-sky-500/20">
            <i data-lucide="footprints" class="w-7 h-7"></i>
          </div>
          <div>
            <h1 class="text-2xl md:text-3xl font-extrabold tracking-tight text-white">
              Melbourne Pedestrian Traffic Dashboard
            </h1>
            <p class="text-xs md:text-sm text-slate-400 mt-0.5">
              Target Locations Analysis • Oct 5 - 11, 2026 • 8:00 AM to 8:00 PM
            </p>
          </div>
        </div>
      </div>
      
      <!-- KPI Badges -->
      <div class="flex flex-wrap items-center gap-3">
        <div class="glass-card px-4 py-2 text-center">
          <span class="text-xs uppercase tracking-wider text-slate-400 font-medium">Total Pedestrians</span>
          <p id="kpi-total-count" class="text-xl font-bold text-sky-400">0</p>
        </div>
        <div class="glass-card px-4 py-2 text-center">
          <span class="text-xs uppercase tracking-wider text-slate-400 font-medium">Active Sensors</span>
          <p id="kpi-sensor-count" class="text-xl font-bold text-emerald-400">0</p>
        </div>
        <div class="glass-card px-4 py-2 text-center">
          <span class="text-xs uppercase tracking-wider text-slate-400 font-medium">Peak Location</span>
          <p id="kpi-peak-loc" class="text-sm font-bold text-amber-400 truncate max-w-[150px]">-</p>
        </div>
      </div>
    </div>

    <!-- Filter Control Bar -->
    <div class="glass-card p-4 mt-6">
      <div class="flex items-center gap-2 mb-3 text-sky-400 font-semibold text-sm">
        <i data-lucide="sliders-horizontal" class="w-4 h-4"></i>
        <span>Global Interactive Filters</span>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <!-- Day Filter -->
        <div>
          <label class="block text-xs font-medium text-slate-400 mb-1.5">Day of Week Filter</label>
          <select id="filter-day" class="w-full bg-slate-800/80 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-sky-500">
            <option value="ALL">All Days (Monday - Sunday)</option>
            <option value="Monday">Monday (Oct 5)</option>
            <option value="Tuesday">Tuesday (Oct 6)</option>
            <option value="Wednesday">Wednesday (Oct 7)</option>
            <option value="Thursday">Thursday (Oct 8)</option>
            <option value="Friday">Friday (Oct 9)</option>
            <option value="Saturday">Saturday (Oct 10)</option>
            <option value="Sunday">Sunday (Oct 11)</option>
            <option value="WEEKDAY">All Weekdays Only (Mon-Fri)</option>
            <option value="WEEKEND">Weekend Only (Sat-Sun)</option>
          </select>
        </div>

        <!-- Hour Filter -->
        <div>
          <div class="flex justify-between items-center mb-1.5">
            <label class="text-xs font-medium text-slate-400">Hour Filter</label>
            <span id="hour-label" class="text-xs font-semibold text-sky-400">All Hours (8am - 8pm)</span>
          </div>
          <select id="filter-hour" class="w-full bg-slate-800/80 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-sky-500">
            <option value="ALL">All Hours (8:00 AM - 8:00 PM)</option>
            <option value="8">8:00 AM</option>
            <option value="9">9:00 AM</option>
            <option value="10">10:00 AM</option>
            <option value="11">11:00 AM</option>
            <option value="12">12:00 PM (Noon)</option>
            <option value="13">1:00 PM</option>
            <option value="14">2:00 PM</option>
            <option value="15">3:00 PM</option>
            <option value="16">4:00 PM</option>
            <option value="17">5:00 PM</option>
            <option value="18">6:00 PM</option>
            <option value="19">7:00 PM</option>
            <option value="20">8:00 PM</option>
          </select>
        </div>

        <!-- Location Filter -->
        <div>
          <label class="block text-xs font-medium text-slate-400 mb-1.5">Location Filter</label>
          <select id="filter-loc" class="w-full bg-slate-800/80 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-sky-500">
            <option value="ALL">All Locations</option>
          </select>
        </div>
      </div>
    </div>
  </div>

  <!-- Main Content Grid -->
  <div class="max-w-7xl mx-auto space-y-6">

    <!-- Row 1: Map (1) & Distribution Pie (3) -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
      
      <!-- 1. Interactive Geographic Map (Google Maps Light style) -->
      <div class="lg:col-span-7 glass-card p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center justify-between mb-3">
            <div class="flex items-center gap-2">
              <i data-lucide="map-pin" class="w-5 h-5 text-sky-400"></i>
              <h2 class="font-bold text-lg text-white">1. Geographic Sensor Map (Google Maps Light)</h2>
            </div>
            <span class="text-xs text-slate-300 bg-sky-950 px-2.5 py-1 rounded-full border border-sky-800">
              Interactive Radius by Volume
            </span>
          </div>
          <p class="text-xs text-slate-400 mb-3">
            Google Maps Light basemap. Marker circle sizes dynamically scale with footfall volume based on your selected Day and Hour filter. Click a marker to inspect details.
          </p>
        </div>
        <div id="map"></div>
      </div>

      <!-- 3. Distribution Pie Chart -->
      <div class="lg:col-span-5 glass-card p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center justify-between mb-2">
            <div class="flex items-center gap-2">
              <i data-lucide="pie-chart" class="w-5 h-5 text-indigo-400"></i>
              <h2 class="font-bold text-lg text-white">3. Footfall Share by Location</h2>
            </div>
          </div>
          <p class="text-xs text-slate-400 mb-2">
            Proportion of total pedestrian volume across sensors for the current filter selection.
          </p>
        </div>
        <div id="pie-chart" class="w-full h-[380px]"></div>
      </div>

    </div>

    <!-- Row 2: 1-Hour Blocks Linear Graph (2) -->
    <div class="glass-card p-5">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
        <div class="flex items-center gap-2">
          <i data-lucide="trending-up" class="w-5 h-5 text-emerald-400"></i>
          <h2 class="font-bold text-lg text-white">2. Pedestrian Count in 1-Hour Blocks (Per Location)</h2>
        </div>
        <div class="text-xs text-slate-300 bg-slate-800 px-3 py-1 rounded-lg border border-slate-700">
          Hourly Granularity: 08:00 to 20:00 (13 hourly points)
        </div>
      </div>
      <p class="text-xs text-slate-400 mb-4">
        Detailed 1-hour breakdown showing pedestrian volume throughout the day (8am to 8pm) across each target location.
      </p>
      <div id="line-chart" class="w-full h-[400px]"></div>
    </div>

    <!-- Row 3: Average Weekday vs Weekend 4-Hour Blocks (4) -->
    <div class="glass-card p-5">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
        <div class="flex items-center gap-2">
          <i data-lucide="calendar-range" class="w-5 h-5 text-amber-400"></i>
          <h2 class="font-bold text-lg text-white">4. Average Weekday vs. Average Weekend (4-Hour Blocks)</h2>
        </div>
        <div class="flex flex-wrap items-center gap-2 text-xs">
          <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-yellow-500/20 text-yellow-300 border border-yellow-500/30">
            <span class="w-2 h-2 rounded-full bg-yellow-400"></span> Morning (Yellow)
          </span>
          <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-pink-500/20 text-pink-300 border border-pink-500/30">
            <span class="w-2 h-2 rounded-full bg-pink-400"></span> Afternoon (Pink)
          </span>
          <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 border border-blue-500/30">
            <span class="w-2 h-2 rounded-full bg-blue-400"></span> Evening (Blue)
          </span>
          <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-700 text-slate-300 border border-slate-600">
            Weekend = Darker Tone
          </span>
        </div>
      </div>
      <p class="text-xs text-slate-400 mb-4">
        Compares average daily pedestrian volume on weekdays (Mon-Fri) vs weekend (Sat-Sun) broken down into Morning (Yellow), Afternoon (Pink), and Evening (Blue) with darker tones for weekend.
      </p>
      <div id="grouped-bar-chart" class="w-full h-[450px]"></div>
    </div>

  </div>

  <!-- Raw Data injected from Python -->
  <script>
    const RAW_DATA = {records_json};

    // Dark Plotly Layout Template
    const darkLayout = {{
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {{ family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', color: '#94a3b8' }},
      margin: {{ l: 50, r: 20, t: 30, b: 50 }},
      legend: {{ orientation: 'h', y: -0.2, x: 0.1, font: {{ color: '#cbd5e1' }} }},
      xaxis: {{ gridcolor: 'rgba(255,255,255,0.06)', zerolinecolor: 'rgba(255,255,255,0.1)' }},
      yaxis: {{ gridcolor: 'rgba(255,255,255,0.06)', zerolinecolor: 'rgba(255,255,255,0.1)' }}
    }};

    // Initialize Map with Google Maps Light Style (standard roadmap)
    let map = L.map('map').setView([-37.8136, 144.9631], 14);
    L.tileLayer('https://mt1.google.com/vt/lyrs=m&x={{x}}&y={{y}}&z={{z}}', {{
      attribution: '&copy; Google Maps',
      maxZoom: 20
    }}).addTo(map);

    let markersLayer = L.layerGroup().addTo(map);

    // Populate Location Filter Dropdown
    const locDropdown = document.getElementById('filter-loc');
    const uniqueLocs = [...new Set(RAW_DATA.map(d => d.sensor_description))].sort();
    uniqueLocs.forEach(loc => {{
      const opt = document.createElement('option');
      opt.value = loc;
      opt.textContent = loc;
      locDropdown.appendChild(opt);
    }});

    // Helper: Filter records based on selected dropdowns
    function getFilteredData() {{
      const dayFilter = document.getElementById('filter-day').value;
      const hourFilter = document.getElementById('filter-hour').value;
      const locFilter = document.getElementById('filter-loc').value;

      return RAW_DATA.filter(d => {{
        // Day filter
        if (dayFilter === 'WEEKDAY' && (d.day_of_week === 5 || d.day_of_week === 6)) return false;
        if (dayFilter === 'WEEKEND' && (d.day_of_week < 5)) return false;
        if (dayFilter !== 'ALL' && dayFilter !== 'WEEKDAY' && dayFilter !== 'WEEKEND' && d.day_name !== dayFilter) return false;

        // Hour filter
        if (hourFilter !== 'ALL' && d.hourday !== parseInt(hourFilter)) return false;

        // Loc filter
        if (locFilter !== 'ALL' && d.sensor_description !== locFilter) return false;

        return true;
      }});
    }}

    // Update Dashboard Elements
    function updateDashboard() {{
      const filtered = getFilteredData();

      // 1. Update KPIs
      const totalCount = filtered.reduce((acc, cur) => acc + cur.pedestriancount, 0);
      document.getElementById('kpi-total-count').innerText = totalCount.toLocaleString();

      const activeSensors = new Set(filtered.map(d => d.location_id)).size;
      document.getElementById('kpi-sensor-count').innerText = activeSensors;

      const locTotals = {{}};
      filtered.forEach(d => {{
        locTotals[d.sensor_description] = (locTotals[d.sensor_description] || 0) + d.pedestriancount;
      }});
      let peakLoc = '-';
      let maxPed = 0;
      for (const [loc, count] of Object.entries(locTotals)) {{
        if (count > maxPed) {{
          maxPed = count;
          peakLoc = loc;
        }}
      }}
      document.getElementById('kpi-peak-loc').innerText = peakLoc;

      // 2. Render Map Markers (Vibrant blue/cyan markers visible on light basemap)
      markersLayer.clearLayers();
      const locGeoMap = {{}};
      filtered.forEach(d => {{
        if (!locGeoMap[d.sensor_description]) {{
          locGeoMap[d.sensor_description] = {{
            lat: d.latitude,
            lon: d.longitude,
            total: 0,
            id: d.location_id,
            name: d.sensor_description
          }};
        }}
        locGeoMap[d.sensor_description].total += d.pedestriancount;
      }});

      const totalsArray = Object.values(locGeoMap).map(l => l.total);
      const maxLocTotal = Math.max(...totalsArray, 1);

      Object.values(locGeoMap).forEach(loc => {{
        if (loc.lat && loc.lon) {{
          // Radius between 8 and 30 px
          const radius = 8 + (loc.total / maxLocTotal) * 22;
          const circle = L.circleMarker([loc.lat, loc.lon], {{
            radius: radius,
            fillColor: '#0284c7',
            color: '#0369a1',
            weight: 2.5,
            opacity: 1.0,
            fillOpacity: 0.85
          }});

          circle.bindPopup(`
            <div class="text-sm font-sans p-1">
              <strong class="text-slate-900 text-base font-bold">${{loc.name}}</strong><br>
              <span class="text-xs text-slate-500">Sensor ID: ${{loc.id}}</span><br>
              <div class="mt-1 font-semibold text-sky-700">Volume: ${{loc.total.toLocaleString()}} pedestrians</div>
            </div>
          `);
          markersLayer.addLayer(circle);
        }}
      }});

      // 3. Render Distribution Pie Chart
      const pieLabels = Object.keys(locTotals);
      const pieValues = Object.values(locTotals);

      Plotly.react('pie-chart', [{{
        labels: pieLabels,
        values: pieValues,
        type: 'pie',
        hole: 0.45,
        textinfo: 'percent',
        hoverinfo: 'label+value+percent',
        marker: {{
          colors: ['#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb923c', '#4ade80', '#fbbf24']
        }}
      }}], {{
        ...darkLayout,
        showlegend: true,
        legend: {{ orientation: 'h', y: -0.1, x: 0 }}
      }}, {{ responsive: true, displayModeBar: false }});

      // 4. Render Linear Graph in 1-Hour Blocks (08:00 to 20:00)
      const oneHourBlocks = [
        "08:00 - 09:00", "09:00 - 10:00", "10:00 - 11:00", "11:00 - 12:00",
        "12:00 - 13:00", "13:00 - 14:00", "14:00 - 15:00", "15:00 - 16:00",
        "16:00 - 17:00", "17:00 - 18:00", "18:00 - 19:00", "19:00 - 20:00",
        "20:00 - 21:00"
      ];

      const lineTraces = [];
      const distinctSensors = [...new Set(filtered.map(d => d.sensor_description))];

      distinctSensors.forEach(sensor => {{
        const sensorData = filtered.filter(d => d.sensor_description === sensor);
        const yVals = oneHourBlocks.map(block => {{
          return sensorData
            .filter(d => d.block_1hr === block)
            .reduce((sum, cur) => sum + cur.pedestriancount, 0);
        }});

        lineTraces.push({{
          x: oneHourBlocks,
          y: yVals,
          mode: 'lines+markers',
          name: sensor,
          line: {{ shape: 'spline', width: 2.5 }},
          marker: {{ size: 5 }}
        }});
      }});

      Plotly.react('line-chart', lineTraces, {{
        ...darkLayout,
        xaxis: {{ ...darkLayout.xaxis, title: '1-Hour Blocks', tickangle: -25 }},
        yaxis: {{ ...darkLayout.yaxis, title: 'Pedestrian Count' }},
        hovermode: 'x unified'
      }}, {{ responsive: true, displayModeBar: false }});

      // 5. Average Weekday vs Weekend (4-Hour Blocks)
      // Morning = Yellow, Afternoon = Pink, Evening = Blue (Darker tones for Weekend)
      const fourHourBlocksConfig = [
        {{
          name: "Morning (08:00 - 12:00)",
          label: "Morning",
          weekdayColor: "#facc15", // bright yellow
          weekendColor: "#854d0e"  // dark mustard / dark yellow
        }},
        {{
          name: "Afternoon (12:00 - 16:00)",
          label: "Afternoon",
          weekdayColor: "#f472b6", // bright pink
          weekendColor: "#9d174d"  // dark magenta / dark pink
        }},
        {{
          name: "Evening (16:00 - 20:00)",
          label: "Evening",
          weekdayColor: "#60a5fa", // bright blue
          weekendColor: "#1e3a8a"  // deep dark navy blue
        }}
      ];

      const numWeekdays = 5; // Mon-Fri in Oct 5-11 2026
      const numWeekends = 2; // Sat-Sun in Oct 5-11 2026

      const selectedLoc = document.getElementById('filter-loc').value;
      const targetSensorsForBar = selectedLoc === 'ALL' ? distinctSensors : [selectedLoc];

      const barTraces = [];

      fourHourBlocksConfig.forEach(cfg => {{
        // Weekday Trace (Lighter / Bright Tone)
        barTraces.push({{
          x: targetSensorsForBar,
          y: targetSensorsForBar.map(sensor => {{
            const sum = RAW_DATA
              .filter(d => d.sensor_description === sensor && d.day_type === 'Weekday' && d.block_4hr === cfg.name)
              .reduce((acc, c) => acc + c.pedestriancount, 0);
            return Math.round(sum / numWeekdays);
          }}),
          name: `${{cfg.label}} (Weekday - Light)`,
          type: 'bar',
          marker: {{ color: cfg.weekdayColor }}
        }});

        // Weekend Trace (Darker Tone)
        barTraces.push({{
          x: targetSensorsForBar,
          y: targetSensorsForBar.map(sensor => {{
            const sum = RAW_DATA
              .filter(d => d.sensor_description === sensor && d.day_type === 'Weekend' && d.block_4hr === cfg.name)
              .reduce((acc, c) => acc + c.pedestriancount, 0);
            return Math.round(sum / numWeekends);
          }}),
          name: `${{cfg.label}} (Weekend - Dark)`,
          type: 'bar',
          marker: {{ color: cfg.weekendColor }}
        }});
      }});

      Plotly.react('grouped-bar-chart', barTraces, {{
        ...darkLayout,
        barmode: 'group',
        xaxis: {{ ...darkLayout.xaxis, tickangle: -15 }},
        yaxis: {{ ...darkLayout.yaxis, title: 'Average Daily Pedestrians' }},
        legend: {{ orientation: 'h', y: -0.25, x: 0 }}
      }}, {{ responsive: true, displayModeBar: false }});
    }}

    // Attach Event Listeners
    document.getElementById('filter-day').addEventListener('change', updateDashboard);
    document.getElementById('filter-hour').addEventListener('change', updateDashboard);
    document.getElementById('filter-loc').addEventListener('change', updateDashboard);

    // Initial render
    updateDashboard();
    lucide.createIcons();
  </script>
</body>
</html>
"""

    output_html = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Dashboard HTML written to: {output_html}")
    return output_html


def serve_dashboard():
    html_path = build_dashboard()
    os.chdir(os.path.dirname(html_path))

    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        url = f"http://localhost:{PORT}/dashboard.html"
        print(f"\n==================================================")
        print(f"[LIVE] Pedestrian Dashboard ready at: {url}")
        print(f"Press Ctrl+C in terminal to stop.")
        print(f"==================================================\n")
        webbrowser.open(url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")


if __name__ == "__main__":
    serve_dashboard()
