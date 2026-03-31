## Pipeline Table Structure
 
The full pipeline produces **13 tables** across Bronze, Silver, and Gold layers inside Unity Catalog.
 
### Bronze Layer — `electrical_grid.01_bronze`
 
3 tables. Raw data ingested as-is from the landing zone volumes — schema enforced, no transformations applied.
 
| Table | Source Volume | Description |
|---|---|---|
| `bronze_meters` | `/meters/` | Raw meter readings — voltage, current, power, energy per batch |
| `bronze_transformers` | `/transformers/` | Raw transformer snapshots — load, capacity, status per batch |
| `bronze_grid_events` | `/grid_events/` | Raw breaker trip and reclose events |
 
### Silver Layer — `electrical_grid.02_silver`
 
5 tables. Cleaned, typed, enriched, and joined. Outage flags derived, meter-to-transformer relationships resolved, reference dimensions added.
 
| Table | Description |
|---|---|
| `silver_fact_meters` | Cleaned meter readings with `is_offline` flag, validated voltage/current/power values |
| `silver_fact_transformers` | Cleaned transformer snapshots with `is_offline` flag and derived `load_pct` |
| `silver_fact_grid_events` | Typed event log with validated `event_type` and `transformer_id` |
| `silver_dim_locations` | Transformer location dimension — `area_name`, `service_band`, `latitude`, `longitude` |
| `silver_dim_tariffs` | NERC SRT tariff rates by `service_band` — `tariff_price_per_kwh`, `effective_from` |
 
### Gold Layer — `electrical_grid.03_gold`
 
5 production-ready tables powering all dashboards, DBSQL queries, and the AI Genie Bot.
 
| Table | Description |
|---|---|
| `gold_area_demand` | 15-min geo-tagged demand per transformer — load kW, apparent load kVA, load %, headroom, avg voltage and power factor |
| `gold_billing_summary` | Daily energy consumption and estimated cost per meter using NERC SRT tariff band rates (₦) |
| `gold_billing_by_area` | Daily billing aggregated by area and service band — total kWh, total cost (₦), active meter count |
| `gold_outage_log` | One row per outage — trip/reclose event pair with duration in minutes and full location context |
| `gold_transformer_health` | 15-min windowed transformer health — avg/peak load %, avg/peak load kW, offline minutes, uptime % |
 
#### Gold Tables — Column Detail
 
**`gold_area_demand`**
 
| Column | Description |
|---|---|
| `transformer_id` | Transformer identifier |
| `area_name` | Geographic area (e.g. Lekki, Victoria Island) |
| `service_band` | NERC service band (A–E) |
| `latitude`, `longitude` | Transformer coordinates |
| `window_start`, `window_end` | 15-min aggregation window boundaries |
| `total_load_kw` | Sum of active power across all connected meters |
| `apparent_load_kva` | `total_load_kw / avg_power_factor` |
| `total_energy_kwh` | Total energy consumed in the window |
| `load_pct` | `apparent_load_kva / capacity_kva × 100` |
| `headroom_kva` | `capacity_kva − apparent_load_kva` |
| `headroom_kw` | Remaining active power headroom |
| `capacity_kva` | Transformer rated capacity |
| `active_meter_count` | Distinct meters reporting in the window |
| `avg_voltage_v` | Average voltage across connected meters |
| `avg_power_factor` | Average power factor across connected meters |
 
**`gold_billing_summary`**
 
| Column | Description |
|---|---|
| `meter_id` | Meter identifier |
| `transformer_id` | Upstream transformer |
| `area_name` | Geographic area |
| `service_band` | NERC SRT tariff band |
| `date` | Billing date |
| `total_kwh` | Total energy consumed that day |
| `avg_power_kw` | Average active power |
| `avg_power_factor` | Average power factor |
| `tariff_price_per_kwh` | Applicable NERC tariff rate (₦/kWh) |
| `estimated_cost_naira` | `total_kwh × tariff_price_per_kwh` |
| `reading_count` | Number of meter readings that day |
 
**`gold_billing_by_area`**
 
| Column | Description |
|---|---|
| `date` | Billing date |
| `area_name` | Geographic area |
| `service_band` | NERC SRT tariff band |
| `total_kwh` | Total energy across all meters in the area |
| `total_cost_naira` | Total estimated cost (₦) |
| `active_meters` | Distinct meters active that day |
 
**`gold_outage_log`**
 
| Column | Description |
|---|---|
| `trip_event_id` | Event ID of the `breaker_trip` event |
| `reclose_event_id` | Event ID of the `breaker_reclose` event |
| `transformer_id` | Transformer that tripped |
| `area_name` | Geographic area |
| `service_band` | NERC service band |
| `outage_start` | Timestamp of the trip |
| `outage_end` | Timestamp of the reclose |
| `duration_minutes` | Outage duration in minutes |
| `latitude`, `longitude` | Transformer coordinates |
 
**`gold_transformer_health`**
 
| Column | Description |
|---|---|
| `transformer_id` | Transformer identifier |
| `area_name` | Geographic area |
| `service_band` | NERC service band |
| `window_start`, `window_end` | 15-min aggregation window boundaries |
| `avg_load_pct` | Average load percentage in the window |
| `peak_load_pct` | Peak load percentage in the window |
| `avg_load_kw` | Average active power load |
| `peak_load_kw` | Peak active power load |
| `offline_minutes` | Total minutes the transformer was offline |
| `uptime_pct` | `(1 − offline_minutes / 15) × 100` |
| `snapshot_count` | Number of transformer readings in the window |
 
---
