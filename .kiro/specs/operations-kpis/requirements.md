# Parcel KPI Engine (Option 1: Python + Pandas) — requirements.md

## Scope (Warehouses / Countries)
The system must support importing and calculating KPIs across **7 warehouses**:
- Kuwait
- Riyadh (Saudi Arabia)
- Dammam (Saudi Arabia)
- Jeddah (Saudi Arabia)
- Qatar
- UAE
- Bahrain

All warehouse-specific behavior (timezone, shifts, cutoffs, SLAs, mapping rules) must be **config-driven**.

---

## Goals
Reproduce (and improve) the spreadsheet KPIs for parcel operations:
- Daily DOD (delivered, on-time, late, on-time %)
- SLA compliance by warehouse / zone / city / area
- Lateness reason tagging (before shift, after cutoff, missing mapping, etc.)
- Staff productivity metrics (collector, wrapper, driver) when data exists

---

## Inputs

### Supported input formats
- Excel (.xlsx) exports
- CSV exports
- Optional later: Google Sheets pull (not required for v1)

### Required source datasets (v1)
1) **Orders** (equivalent to `Raw Data`)
   - Minimum fields (via mapping):
     - `parcel_id` (unique key)
     - `order_created_at` (timestamp)
     - `order_status` (Delivered / Cancelled / Returned / etc.)
     - `warehouse` (one of the 7 above OR inferable)
     - `country` (optional if warehouse implies it)
     - `city`, `area` (or address fields that can map to them)
     - optional: `collector`, `wrapper`
2) **Delivery stages / events** (equivalent to `Raw Data - Delivery Stages`)
   - Minimum fields:
     - `parcel_id`
     - event timestamps used to compute phases, e.g.:
       - `picked_at`, `packed_at`, `out_for_delivery_at`, `delivered_at`
     - `delivery_status` / final status
     - `driver` (optional)

### Optional datasets (v1.1)
- Collector report (productivity)
- Preparer report (productivity)
- Exceptions / backlog lists (if tracked elsewhere)

---

## Configuration (must exist before calculations)

### Config files
- `config/warehouses.yaml` (required)
- `config/sla_by_area.csv` (required; can be minimal in v1)
- `config/location_map.csv` (optional; for standardization)
- `config/holidays.csv` (optional; future use)

### Warehouse configuration (per warehouse)
Each of the 7 warehouses must define:
- `timezone` (IANA, e.g., `Asia/Kuwait`, `Asia/Riyadh`, `Asia/Dubai`, `Asia/Qatar`, `Asia/Bahrain`)
- `shift_start`, `shift_end` (local time)
- `cutoff_time` (local time)
- `default_delivery_sla_hours`
- Optional Ramadan rules:
  - `ramadan_date_ranges` (start/end dates)
  - `ramadan_shift_start`, `ramadan_shift_end` (optional overrides)
  - `iftar_break_start`, `iftar_break_end` (optional; v1.1)
- `country_code` (for reporting/filters)

### SLA by area
`sla_by_area.csv` must support overrides by:
- warehouse
- city
- area
- optional: zone

---

## Core Calculations (Functional Requirements)

### FR-1 — Ingestion & schema mapping
- Read all inputs from a local folder (`--input`).
- Support column alias mapping (`config/schema_map.yaml` optional but recommended).
- Enforce types:
  - IDs -> string
  - timestamps -> parsed and localized using the **warehouse timezone**
- Produce a single **fact table** keyed by `parcel_id`.

### FR-2 — Multi-timezone handling
- All timestamps must be stored in:
  - `*_at_local` (localized to warehouse tz) and
  - `*_at_utc` (UTC)
- All SLA and shift/cutoff logic must operate in **warehouse local time**.

### FR-3 — Cleaning
- Convert invalid markers (`#N/A`, `#REF!`, empty strings) to nulls.
- Standardize city/area (trim, normalize, apply mapping tables).
- Deduplicate by `parcel_id` with deterministic rules:
  - keep latest record by `updated_at` if available; else last row.

### FR-4 — Phase durations
Compute durations (minutes) where possible:
- `collect_duration = picked_at - order_created_at`
- `pack_duration = packed_at - picked_at`
- `dispatch_duration = out_for_delivery_at - packed_at`
- `delivery_duration = delivered_at - out_for_delivery_at`
- `end_to_end = delivered_at - order_created_at`

Missing timestamps must yield null for that duration (no hard failures).

### FR-5 — Zone mapping
- Determine `zone = f(warehouse, city, area)` using config tables.
- If zone cannot be mapped, set `zone = null` and tag `unknown_zone`.

### FR-6 — Expected delivery time
For each parcel:
1) Determine `warehouse`, `timezone`, and `service_day_rules`
2) Determine SLA hours (`sla_hours`) via:
   - area override (warehouse+city+area) else warehouse default
3) Adjust start time:
   - If order created **before shift** → start = shift_start (same day)
   - If order created **after cutoff** → start = next service day shift_start
   - (v1.1) pause clock during iftar break window if configured
4) `expected_delivery_at = adjusted_start + sla_hours`

### FR-7 — SLA status
- For delivered parcels:
  - `sla_status = "On Time"` if `delivered_at_local <= expected_delivery_at_local` else `"Late"`
- For non-delivered:
  - `sla_status = "Open"` (or a configured status mapping)

### FR-8 — Lateness reason tagging (minimum viable set)
For late parcels, assign tags (multi-tag allowed):
- `after_cutoff`
- `before_shift`
- `unknown_zone`
- `missing_events`
- `exceeded_sla` (fallback)
Expose:
- `late_tags` (list/pipe-delimited)
- `late_primary_reason`

### FR-9 — Aggregations / outputs
Generate:
1) `dod_daily.csv`
   - `date` (warehouse local date)
   - `warehouse`
   - `delivered`, `on_time`, `late`, `on_time_pct`
2) `sla_breakdown.csv`
   - by `warehouse`, `zone`, `city`, `area`
   - counts + on-time %
3) `staff_summary.csv` (if data exists)
   - by `warehouse`, `collector`/`wrapper`/`driver`
   - volumes + on-time %

### FR-10 — Output quality
- Output must never contain Excel error tokens.
- Any division must guard against zero denominators.

---

## CLI / Execution

### Command
`python -m src.run --input ./data --config ./config --out ./out`

Options:
- `--from-date YYYY-MM-DD` `--to-date YYYY-MM-DD` (interpreted per warehouse local date)
- `--warehouses Kuwait,Riyadh,...` (subset run)
- `--timezone-default Africa/Cairo` (only used if warehouse tz missing; should not happen)
- `--log-level INFO|DEBUG`

### Exit codes
- `0` success
- `2` validation error (missing required columns/config)
- `3` runtime error

---

## Data Validation & Quality Checks

### Required checks
- `parcel_id` uniqueness after merge
- Missing timestamps:
  - delivered parcels missing `delivered_at` must be counted and reported
- Mapping health:
  - % unknown city/area/zone per warehouse
- Config completeness:
  - all warehouses present in config
  - SLA exists (default + overrides are optional)

### Quality report
- `out/quality_report.json` with counts and percentages per warehouse

---

## Non-Functional Requirements
- Reproducible: same input + config → identical output
- Performance: 50k rows in < 60 seconds on a typical laptop
- Test coverage: unit tests for SLA/shift/cutoff and timezone conversions
