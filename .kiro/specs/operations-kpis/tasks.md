# Operations KPIs - Implementation Tasks

## 1. Project Setup & Configuration
- [x] 1.1 Initialize Python project structure with pyproject.toml or requirements.txt
  - [x] 1.1.1 Create src/ directory with __init__.py
  - [x] 1.1.2 Add dependencies: pandas, pyyaml, pytz, openpyxl, python-dateutil
  - [x] 1.1.3 Create tests/ directory structure
- [x] 1.2 Create configuration files with 7 warehouse definitions
  - [x] 1.2.1 Create config/warehouses.yaml with Kuwait, Riyadh, Dammam, Jeddah, Qatar, UAE, Bahrain
  - [x] 1.2.2 Create config/sla_by_area.csv template
  - [x] 1.2.3 Create config/schema_map.yaml for flexible column mapping
  - [x] 1.2.4 Create config/location_map.csv for city/area normalization (optional)
- [x] 1.3 Create data types module (src/types.py)
  - [x] 1.3.1 Define WarehouseConfig dataclass
  - [x] 1.3.2 Define SLAConfig dataclass
  - [x] 1.3.3 Define SchemaMapping dataclass

## 2. Data Ingestion Module (src/ingest.py)
- [x] 2.1 Implement multi-format file reader
  - [x] 2.1.1 Create CSV reader with encoding detection
  - [x] 2.1.2 Create Excel reader (.xlsx support)
  - [x] 2.1.3 Implement auto-detection of file format
- [x] 2.2 Implement flexible schema mapper
  - [x] 2.2.1 Load schema_map.yaml configuration
  - [x] 2.2.2 Map column aliases to standard names (e.g., "Parcel ID" → "parcel_id")
  - [x] 2.2.3 Handle case-insensitive column matching
- [x] 2.3 Implement multi-format date parser
  - [x] 2.3.1 Auto-detect date formats: "DD/MM/YYYY HH:MM:SS", "MMM D, YYYY, H:MM:SS AM/PM", "D MMM, YYYY H:MM:SS"
  - [x] 2.3.2 Parse dates with python-dateutil fallback
  - [x] 2.3.3 Handle timezone-naive dates (assume warehouse local time)
- [x] 2.4 Create dataset-specific loaders
  - [x] 2.4.1 Load Delivery Details (orders + delivery dates)
  - [x] 2.4.2 Load Parcel Logs (status transitions with timestamps)
  - [x] 2.4.3 Load Collectors Report (staff productivity)
  - [x] 2.4.4 Load Prepare Report (staff productivity)
  - [x] 2.4.5 Load Items Per Order Report (item counts)
  - [x] 2.4.6 Load FreshDesk Data (issues/tickets for late reason tagging)

## 3. Data Normalization Module (src/normalize.py)
- [x] 3.1 Implement null marker cleaning
  - [x] 3.1.1 Convert "#N/A", "#REF!", empty strings to None
  - [x] 3.1.2 Handle "na", "N/A" variations
- [x] 3.2 Implement text standardization
  - [x] 3.2.1 Trim whitespace from all string fields
  - [x] 3.2.2 Normalize city/area names (casefold, strip)
  - [x] 3.2.3 Apply location_map.csv replacements if configured
- [x] 3.3 Implement deduplication logic
  - [x] 3.3.1 Group by parcel_id
  - [x] 3.3.2 Keep latest record by updated_at if available, else last row
  - [x] 3.3.3 Log duplicate counts per warehouse

## 4. Timestamp Extraction & Multi-Timezone Handling (src/time_rules.py)
- [x] 4.1 Extract timestamps from Parcel Logs
  - [x] 4.1.1 Extract picked_at = first "Collecting" timestamp
  - [x] 4.1.2 Extract packed_at = first "Prepare" timestamp
  - [x] 4.1.3 Extract out_for_delivery_at = first "On The Way" timestamp
  - [x] 4.1.4 Extract delivered_at from Delivery Details parcel_delivery_date
  - [x] 4.1.5 Implement forward-fill logic: if timestamp missing, use previous row's timestamp (up to 2 rows back)
- [x] 4.2 Implement timezone localization
  - [x] 4.2.1 Load warehouse timezone from config (IANA format)
  - [x] 4.2.2 Convert all timestamps to warehouse local time (*_at_local fields)
  - [x] 4.2.3 Convert all timestamps to UTC (*_at_utc fields)
  - [x] 4.2.4 Store both local and UTC versions for all timestamp fields
- [x] 4.3 Implement shift/cutoff rules
  - [x] 4.3.1 Parse shift_start and shift_end from warehouse config
  - [x] 4.3.2 Parse cutoff_time from warehouse config
  - [x] 4.3.3 Implement Ramadan date range detection
  - [x] 4.3.4 Apply Ramadan shift overrides when applicable
- [x] 4.4 Calculate phase durations
  - [x] 4.4.1 Calculate collect_duration_min = (picked_at - order_created_at) in minutes
  - [x] 4.4.2 Calculate pack_duration_min = (packed_at - picked_at) in minutes
  - [x] 4.4.3 Calculate dispatch_duration_min = (out_for_delivery_at - packed_at) in minutes
  - [x] 4.4.4 Calculate delivery_duration_min = (delivered_at - out_for_delivery_at) in minutes
  - [x] 4.4.5 Calculate end_to_end_min = (delivered_at - order_created_at) in minutes
  - [x] 4.4.6 Handle null timestamps gracefully (return null duration)

## 5. Location Mapping Module (src/mapping.py)
- [x] 5.1 Implement zone mapping logic
  - [x] 5.1.1 Load sla_by_area.csv configuration
  - [x] 5.1.2 Lookup zone by (warehouse, city, area) - primary key
  - [x] 5.1.3 Fallback to (warehouse, city) if area not found
  - [x] 5.1.4 Set zone = null if no match found
  - [x] 5.1.5 Tag parcels with unknown_zone flag
- [x] 5.2 Implement SLA hours lookup
  - [x] 5.2.1 Lookup sla_hours by (warehouse, city, area) override
  - [x] 5.2.2 Fallback to warehouse default_delivery_sla_hours
  - [x] 5.2.3 Store resolved sla_hours in fact table

## 6. SLA Engine Module (src/sla_engine.py)
- [x] 6.1 Implement adjusted SLA start time calculation
  - [x] 6.1.1 If order_created_at_local.time() < shift_start → start = same_day shift_start
  - [x] 6.1.2 If order_created_at_local.time() > cutoff_time → start = next_day shift_start
  - [x] 6.1.3 Else start = order_created_at_local
  - [x] 6.1.4 Handle service day transitions (skip non-service days if configured)
- [x] 6.2 Calculate expected delivery time
  - [x] 6.2.1 expected_delivery_at_local = adjusted_start + sla_hours
  - [x] 6.2.2 Store expected_delivery_at_local and expected_delivery_at_utc
- [x] 6.3 Determine SLA status
  - [x] 6.3.1 For delivered parcels: "On Time" if delivered_at_local <= expected_delivery_at_local, else "Late"
  - [x] 6.3.2 For non-delivered parcels: "Open"
  - [x] 6.3.3 Store sla_status in fact table
- [x] 6.4 Implement late reason tagging
  - [x] 6.4.1 Tag "after_cutoff" if order created after cutoff_time
  - [x] 6.4.2 Tag "before_shift" if order created before shift_start
  - [x] 6.4.3 Tag "unknown_zone" if zone mapping failed
  - [x] 6.4.4 Tag "missing_events" if required timestamps are null
  - [x] 6.4.5 Tag FreshDesk issues (e.g., "delivery_delay", "damaged_item") by matching parcel_id
  - [x] 6.4.6 Tag "exceeded_sla" as fallback
  - [x] 6.4.7 Store late_tags (pipe-delimited) and late_primary_reason (highest priority)

## 7. Fact Table Builder
- [x] 7.1 Merge all datasets by parcel_id
  - [x] 7.1.1 Merge Delivery Details (orders)
  - [x] 7.1.2 Merge Parcel Logs (timestamps)
  - [x] 7.1.3 Merge Collectors Report (collector, collect times)
  - [x] 7.1.4 Merge Prepare Report (preparer, prepare times)
  - [x] 7.1.5 Merge Items Per Order (item_count)
  - [x] 7.1.6 Merge FreshDesk Data (issue tags)
- [x] 7.2 Apply all enrichment steps
  - [x] 7.2.1 Normalize and clean data
  - [x] 7.2.2 Extract and localize timestamps
  - [x] 7.2.3 Calculate phase durations
  - [x] 7.2.4 Map zones and SLA hours
  - [x] 7.2.5 Calculate expected delivery times
  - [x] 7.2.6 Determine SLA status and late reasons
- [x] 7.3 Validate fact table
  - [x] 7.3.1 Check parcel_id uniqueness
  - [x] 7.3.2 Check for delivered parcels missing delivered_at
  - [x] 7.3.3 Calculate mapping health (% unknown zones per warehouse)

## 8. Metrics & Aggregation Module (src/metrics.py)
- [x] 8.1 Generate Daily DOD report (dod_daily.csv)
  - [x] 8.1.1 Group by warehouse and date (local)
  - [x] 8.1.2 Count delivered, on_time, late
  - [x] 8.1.3 Calculate on_time_pct (guard against zero division)
  - [x] 8.1.4 Export to CSV
- [x] 8.2 Generate SLA breakdown report (sla_breakdown.csv)
  - [x] 8.2.1 Group by warehouse, zone, city, area
  - [x] 8.2.2 Count delivered, on_time, late
  - [x] 8.2.3 Calculate on_time_pct
  - [x] 8.2.4 Export to CSV
- [x] 8.3 Generate staff productivity report (staff_summary.csv)
  - [x] 8.3.1 Group by warehouse, collector (if data exists)
  - [x] 8.3.2 Group by warehouse, preparer (if data exists)
  - [x] 8.3.3 Group by warehouse, driver (if data exists)
  - [x] 8.3.4 Calculate volumes and on_time_pct per staff member
  - [x] 8.3.5 Export to CSV

## 9. Quality Validation Module (src/quality.py)
- [x] 9.1 Implement validation checks
  - [x] 9.1.1 Check parcel_id uniqueness
  - [x] 9.1.2 Check for missing required columns
  - [x] 9.1.3 Check for delivered parcels missing delivered_at
  - [x] 9.1.4 Calculate % unknown city/area/zone per warehouse
  - [x] 9.1.5 Check config completeness (all warehouses present, SLA defaults exist)
- [x] 9.2 Generate quality report
  - [x] 9.2.1 Create quality_report.json with counts and percentages
  - [x] 9.2.2 Include validation errors and warnings
  - [x] 9.2.3 Export to out/quality_report.json

## 10. Export Module (src/export.py)
- [x] 10.1 Implement CSV export
  - [x] 10.1.1 Export dod_daily.csv
  - [x] 10.1.2 Export sla_breakdown.csv
  - [x] 10.1.3 Export staff_summary.csv
  - [x] 10.1.4 Ensure no Excel error tokens in output
- [x] 10.2 Implement Parquet export (optional)
  - [x] 10.2.1 Export parcel_fact.parquet for fast re-runs

## 11. CLI Entry Point (src/run.py)
- [x] 11.1 Implement argument parser
  - [x] 11.1.1 Add --input flag (path to data folder)
  - [x] 11.1.2 Add --config flag (path to config folder)
  - [x] 11.1.3 Add --out flag (path to output folder)
  - [x] 11.1.4 Add --from-date and --to-date flags (YYYY-MM-DD format)
  - [x] 11.1.5 Add --warehouses flag (comma-separated subset)
  - [x] 11.1.6 Add --timezone-default flag (fallback timezone)
  - [x] 11.1.7 Add --log-level flag (INFO|DEBUG)
- [x] 11.2 Implement pipeline orchestration
  - [x] 11.2.1 Load configurations
  - [x] 11.2.2 Ingest all datasets
  - [x] 11.2.3 Build fact table
  - [x] 11.2.4 Generate metrics
  - [x] 11.2.5 Export outputs
  - [x] 11.2.6 Generate quality report
- [x] 11.3 Implement error handling
  - [x] 11.3.1 Exit code 0 for success
  - [x] 11.3.2 Exit code 2 for validation errors
  - [x] 11.3.3 Exit code 3 for runtime errors
  - [x] 11.3.4 Log errors with context

## 12. Web Dashboard (Flask/FastAPI + HTML/JS)
- [x] 12.1 Setup web framework
  - [x] 12.1.1 Create app.py with Flask or FastAPI
  - [x] 12.1.2 Add dependencies: flask/fastapi, plotly, pandas
  - [x] 12.1.3 Create templates/ and static/ directories
- [x] 12.2 Implement file upload interface
  - [x] 12.2.1 Create upload page with 6 file inputs (Delivery Details, Parcel Logs, Collectors Report, Prepare Report, Items Per Order, FreshDesk Data)
  - [x] 12.2.2 Validate uploaded files (CSV format, required columns)
  - [x] 12.2.3 Save uploaded files to temp directory
  - [x] 12.2.4 Trigger pipeline execution on upload
- [x] 12.3 Implement dashboard visualizations
  - [x] 12.3.1 Create Daily DOD trend chart (combo chart: delivered bars + on-time % line)
  - [x] 12.3.2 Create On-time % by warehouse comparison (bar chart)
  - [x] 12.3.3 Create SLA breakdown by zone/city/area (table + heatmap)
  - [x] 12.3.4 Create Staff productivity metrics (bar chart by collector/preparer/driver)
  - [x] 12.3.5 Use Plotly for interactive charts
- [x] 12.4 Implement CSV export from dashboard
  - [x] 12.4.1 Add "Export to CSV" buttons for each report
  - [x] 12.4.2 Generate downloadable CSV files
- [x] 12.5 Add error handling and user feedback
  - [x] 12.5.1 Display validation errors on upload
  - [x] 12.5.2 Show processing status/progress
  - [x] 12.5.3 Display quality report warnings

## 13. Documentation & README
- [x] 13.1 Create README.md
  - [x] 13.1.1 Add project overview and goals
  - [x] 13.1.2 Add installation instructions
  - [x] 13.1.3 Add configuration guide (warehouses.yaml, sla_by_area.csv)
  - [x] 13.1.4 Add CLI usage examples
  - [x] 13.1.5 Add dashboard usage guide
  - [x] 13.1.6 Add example run with sample data
- [x] 13.2 Add inline code documentation
  - [x] 13.2.1 Add docstrings to all modules and functions
  - [x] 13.2.2 Add type hints where applicable

## 14. Integration & End-to-End Testing
- [x] 14.1 Test with provided sample data
  - [x] 14.1.1 Run CLI with Rawdata Examples folder
  - [x] 14.1.2 Verify all 6 CSV files are ingested correctly
  - [x] 14.1.3 Verify fact table is built with all fields
  - [x] 14.1.4 Verify all 3 output reports are generated
  - [x] 14.1.5 Verify quality report is generated
- [x] 14.2 Test dashboard with sample data
  - [x] 14.2.1 Upload all 6 CSV files via web interface
  - [x] 14.2.2 Verify all charts render correctly
  - [x] 14.2.3 Verify CSV export works
- [x] 14.3 Test edge cases
  - [x] 14.3.1 Test with missing timestamps (forward-fill logic)
  - [x] 14.3.2 Test with unknown zones
  - [x] 14.3.3 Test with Ramadan date ranges
  - [x] 14.3.4 Test with multiple date formats
  - [x] 14.3.5 Test with duplicate parcel_ids
