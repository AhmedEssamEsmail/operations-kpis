# Operations KPIs - Parcel KPI Engine

A comprehensive data pipeline and dashboard for analyzing parcel delivery operations across multiple warehouses.

## Features

- Multi-warehouse support (Kuwait, Riyadh, Dammam, Jeddah, Qatar, UAE, Bahrain)
- Timezone-aware timestamp processing
- SLA calculation with shift/cutoff rules
- Ramadan period support with adjusted schedules
- Waiting Address (WA) detection
- Late reason tagging
- Interactive web dashboard with Plotly charts
- CSV export functionality

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Warehouse Configuration (`config/warehouses.yaml`)
Defines warehouse-specific settings including:
- Timezone (IANA format)
- Shift start/end times
- Cutoff times
- Default SLA hours
- Ramadan schedules

### SLA Configuration (`config/sla_by_area.csv`)
Maps locations to zones and custom SLA hours:
```csv
warehouse,city,area,zone,sla_hours
Kuwait,Kuwait City,Yarmouk,Zone A,24
```

### Schema Mapping (`config/schema_map.yaml`)
Flexible column name mapping for input files.

### Location Mapping (`config/location_map.csv`)
Normalizes city/area names across datasets.

## CLI Usage

```bash
python -m src.run \
  --input ./Rawdata\ Examples \
  --config ./config \
  --out ./output \
  --log-level INFO
```

### CLI Arguments

- `--input`: Path to input data folder (required)
- `--config`: Path to config folder (default: ./config)
- `--out`: Path to output folder (default: ./out)
- `--from-date`: Start date filter (YYYY-MM-DD)
- `--to-date`: End date filter (YYYY-MM-DD)
- `--warehouses`: Comma-separated warehouse names
- `--timezone-default`: Default timezone (default: Asia/Kuwait)
- `--log-level`: Logging level (INFO|DEBUG)

## Dashboard Usage

Start the Flask dashboard:

```bash
python app.py
```

Then open http://localhost:5000 in your browser.

### Upload Files

The dashboard requires 6 CSV files:
1. Delivery Details
2. Parcel Logs
3. Collectors Report
4. Prepare Report
5. Items Per Order
6. FreshDesk Data

### Available Views

- **Daily DOD Trend**: Delivered parcels over time
- **Warehouse Comparison**: Parcel volume by warehouse
- **Waiting Address Analysis**: WA vs normal addresses
- **Area Breakdown**: Top 20 areas by volume
- **Staff Productivity**: Collector and preparer metrics

## Output Files

### Reports
- `dod_daily.csv`: Daily delivered on date metrics
- `sla_breakdown.csv`: SLA performance by location
- `staff_summary.csv`: Staff productivity metrics

### Quality Report
- `quality_report.json`: Validation results and data quality metrics

### Fact Table (optional)
- `parcel_fact.parquet`: Complete enriched dataset

## Data Pipeline

1. **Ingestion**: Load and parse CSV/Excel files
2. **Normalization**: Clean data, detect WA, standardize locations
3. **Timestamp Extraction**: Extract key timestamps from parcel logs
4. **Timezone Localization**: Convert to warehouse local time and UTC
5. **Zone Mapping**: Map locations to zones and SLA hours
6. **SLA Calculation**: Calculate expected delivery times and status
7. **Late Reason Tagging**: Tag reasons for late deliveries
8. **Metrics Generation**: Aggregate into reports
9. **Quality Validation**: Check data quality
10. **Export**: Save reports and fact table

## Waiting Address (WA) Detection

Parcels with "WA" or "wa" after "Extra info:" in the delivery address are flagged as waiting addresses:

```
Extra info: WA  → has_waiting_address = True
Extra info: wa  → has_waiting_address = True
```

## SLA Rules

### Adjusted Start Time
- Order before shift start → Start at shift start same day
- Order after cutoff → Start at shift start next day
- Otherwise → Start at order time

### SLA Status
- **On Time**: Delivered ≤ expected delivery time
- **Late**: Delivered > expected delivery time
- **Open**: Not yet delivered

### Late Reasons
- `after_cutoff`: Order created after cutoff
- `before_shift`: Order created before shift start
- `unknown_zone`: Zone mapping failed
- `missing_events`: Required timestamps missing
- `delivery_delay`: FreshDesk issue
- `exceeded_sla`: General late delivery

## Project Structure

```
.
├── app.py                  # Flask dashboard
├── src/
│   ├── run.py             # CLI entry point
│   ├── types.py           # Data types
│   ├── ingest.py          # Data ingestion
│   ├── normalize.py       # Data normalization
│   ├── time_rules.py      # Timestamp & timezone handling
│   ├── mapping.py         # Location & SLA mapping
│   ├── sla_engine.py      # SLA calculation
│   ├── fact_table.py      # Fact table builder
│   ├── metrics.py         # Metrics generation
│   ├── quality.py         # Quality validation
│   └── export.py          # Data export
├── config/
│   ├── warehouses.yaml
│   ├── sla_by_area.csv
│   ├── schema_map.yaml
│   └── location_map.csv
├── templates/
│   └── index.html         # Dashboard UI
└── tests/
```

## Example Run

```bash
# Run CLI pipeline
python -m src.run \
  --input "./Rawdata Examples" \
  --config ./config \
  --out ./output

# Start dashboard
python app.py
```

## Requirements

- Python 3.8+
- pandas
- pyyaml
- pytz
- openpyxl
- python-dateutil
- flask
- plotly
- pyarrow/fastparquet

## License

MIT
