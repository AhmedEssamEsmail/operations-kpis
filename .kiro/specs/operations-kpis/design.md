# Parcel KPI Engine (Option 1: Python + Pandas) — design.md

## Architecture overview
A config-driven pipeline that builds a single fact table and then produces reporting tables.

### Pipeline stages
1) **Ingest**
2) **Normalize & Map**
3) **Enrich (SLA/Expected times)**
4) **Aggregate**
5) **Export + Quality report**

---

## Repo layout
```
parcel-kpi/
  config/
    warehouses.yaml
    sla_by_area.csv
    location_map.csv            # optional
    schema_map.yaml             # optional (recommended)
  data/                         # gitignored
  out/                          # generated outputs, gitignored
  src/
    run.py                      # CLI entry
    ingest.py                   # read xlsx/csv + schema mapping
    normalize.py                # clean text, fix null markers
    mapping.py                  # city/area normalization + zone mapping
    time_rules.py               # shift/cutoff/ramadan rules
    sla_engine.py               # expected_delivery_at + SLA status + late reasons
    metrics.py                  # dod + breakdowns
    export.py                   # csv/xlsx/parquet writers
    quality.py                  # validation + quality_report.json
    types.py                    # dataclasses for configs
  tests/
    test_time_rules.py
    test_sla_engine.py
    test_mapping.py
  pyproject.toml (or requirements.txt)
  README.md
```

---

## Key design decisions

### 1) Warehouse is the top-level partition
Most rules depend on warehouse (timezone, shift, cutoff, SLA defaults).  
Design: compute all derived fields **per warehouse group**.

### 2) Multi-timezone is explicit
Store both local and UTC fields:
- `order_created_at_local`, `order_created_at_utc`
- `delivered_at_local`, `delivered_at_utc`
Local is the “business truth” for SLA/shift/cutoff.

### 3) Config-driven rules
No warehouse names in code except as keys in configs.

#### `warehouses.yaml` (example)
```yaml
warehouses:
  Kuwait:
    timezone: Asia/Kuwait
    country_code: KW
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
    ramadan:
      enabled: true
      date_ranges:
        - { start: "2026-02-18", end: "2026-03-19" }
      shift_start: "12:00"
      cutoff_time: "19:00"
  Riyadh:
    timezone: Asia/Riyadh
    country_code: SA
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
  Dammam:
    timezone: Asia/Riyadh
    country_code: SA
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
  Jeddah:
    timezone: Asia/Riyadh
    country_code: SA
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
  Qatar:
    timezone: Asia/Qatar
    country_code: QA
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
  UAE:
    timezone: Asia/Dubai
    country_code: AE
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
  Bahrain:
    timezone: Asia/Bahrain
    country_code: BH
    shift_start: "10:00"
    shift_end: "23:00"
    cutoff_time: "21:00"
    default_delivery_sla_hours: 4
```

#### `sla_by_area.csv` (example)
Columns:
- `warehouse,city,area,zone,sla_hours`
`zone` can be empty if not used.

---

## Data model

### Fact table (core)
One row per `parcel_id` after merging.
Minimum fields:
- Keys: `parcel_id`, `warehouse`, `country_code`
- Location: `city`, `area`, `zone`
- Status: `order_status`, `delivery_status`
- Timestamps (local + utc): `order_created_at_*`, `picked_at_*`, `packed_at_*`, `out_for_delivery_at_*`, `delivered_at_*`
- SLA: `sla_hours`, `expected_delivery_at_local`, `sla_status`
- Tags: `late_tags`, `late_primary_reason`
- Durations: `collect_duration_min`, `pack_duration_min`, `dispatch_duration_min`, `delivery_duration_min`, `end_to_end_min`

---

## Algorithms

### A) Normalize & map location
1) Apply `location_map.csv` replacements (optional).
2) Standardize formatting (trim/casefold).
3) Compute `zone` via lookup:
   - primary key: (warehouse, city, area)
   - fallback: (warehouse, city)
   - else null

### B) Adjusted SLA start time (shift/cutoff)
Given `order_created_at_local`:
- If `time < shift_start`: `start = same_day shift_start`
- Else if `time > cutoff_time`: `start = next_day shift_start`
- Else: `start = order_created_at_local`

(v1.1) if iftar break window exists, “pause” time accumulation.

### C) Expected delivery time
`expected_delivery_at_local = adjusted_start + sla_hours`

### D) SLA status
- Delivered:
  - On Time if `delivered_at_local <= expected_delivery_at_local`
  - Late otherwise
- Not delivered:
  - Open (or configured mapping)

### E) Late reason tagging (priority)
- If created after cutoff → `after_cutoff`
- Else if created before shift → `before_shift`
- Else if zone missing → `unknown_zone`
- Else if required events missing → `missing_events`
- Else → `exceeded_sla`

Allow multi-tags, plus a primary reason based on the above priority.

---

## Outputs

### CSV outputs
- `dod_daily.csv`: by date (local), warehouse
- `sla_breakdown.csv`: by warehouse/zone/city/area
- `staff_summary.csv`: if columns exist

### Optional outputs
- `parcel_fact.parquet` for fast re-runs
- `dashboard_ready.xlsx` with clean tables for BI

---

## Testing strategy
- Unit tests for:
  - timezone localization and conversion per warehouse
  - shift/cutoff transitions (before shift, after cutoff, normal)
  - SLA override selection (area override vs default)
  - zone mapping fallbacks
  - late reason priority

---

## Operational notes
- Keep `data/` and `out/` gitignored.
- Provide sample config files committed to repo.
- Provide a `README.md` with a quickstart and an example run.
