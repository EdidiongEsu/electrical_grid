# 000 — Electrical Grid Data Generator

> Streaming data simulator for a synthetic electrical grid. Runs continuously on Databricks, writing realistic meter, transformer, and event data to a Delta Lake landing zone every 30 seconds.

---

## What This Notebook Does

This notebook is a **streaming data generator** for a simulated electrical grid. It acts as a fake IoT source — endlessly creating realistic readings from smart meters and transformers, then writing them to a data lake in Delta format. Think of it as a synthetic sensor stream you can run on Databricks to build and test downstream pipelines without needing real hardware.

The simulation has three moving parts: **50 smart meters**, **5 transformers** (each serving 10 meters), and a **grid events log**. Every 30 seconds, a new batch fires. Meters report voltage, current, power draw, and energy consumed. Transformers report their aggregate load and online/offline status. And when something goes wrong — a breaker trips — that event gets recorded separately.

The most interesting part is the **outage logic**. Each batch, every transformer has a small random chance (3%) of going offline. When it does, all 10 meters behind it immediately report zero voltage and current, and a `breaker_trip` event is logged. The transformer stays offline for up to 3 batches, then automatically recovers with a `breaker_reclose` event — mimicking a real auto-recloser on a distribution grid.

Each batch also simulates **event-time latency**: meter and transformer readings are timestamped slightly behind the actual batch time (up to 20 seconds), reflecting real-world sensor delays. This makes the data suitable for testing late-arriving event handling in streaming pipelines.

---

## How It Works — Step by Step

### 1. Batch fires every 30 seconds

The notebook runs an infinite `while True` loop. At the top of each iteration, it captures the current UTC time as `batch_time`. This timestamp is used as the `ingestion_time` for all records in the batch.

### 2. Random outage check

Before generating any readings, the generator checks each of the 5 transformers. Each online transformer has a **3% chance** of tripping this batch. If it trips:
- Its outage counter is set to `1`
- A `breaker_trip` event is immediately appended to the events list

### 3. Meter data generation

For each of the 50 meters:
- Its parent transformer is determined by `((meter_id - 1) % TRANSFORMER_COUNT) + 1`
- If the transformer is **offline**, the meter reports `voltage = 0.0`, `current = 0.0`, and all derived values as zero
- If the transformer is **online**, voltage (220–240 V) and current (2–30 A) are randomised, and power and energy are calculated from those
- A small random latency (0–20 seconds) is subtracted from `batch_time` to simulate the `event_time`

As meter records are built, each meter's `power_kw` is accumulated into a per-transformer load map used in the next step.

### 4. Transformer data generation + outage progression

For each of the 5 transformers:
- A record is written with its aggregated `load_kw` (summed from connected meters), rated capacity, and current status (`online`/`offline`)
- If the transformer is currently in an outage, its counter is incremented
- Once the counter exceeds `MAX_OUTAGE_BATCHES` (default: 3), the transformer recovers: its counter resets to `0` and a `breaker_reclose` event is appended

### 5. Write to Delta Lake

Three separate writes happen at the end of each batch:
- Meter records → `/meters/`
- Transformer records → `/transformers/`
- Event records → `/grid_events/` (only if events occurred this batch)

All writes use `mode("append")` so data accumulates across batches.

### 6. Sleep and repeat

The loop sleeps for `BATCH_INTERVAL_SECONDS` (default: 30) and starts again from step 1.

---

## Grid Topology

```
5 Transformers
└── 10 Meters each → 50 Meters total

Meter-to-transformer assignment: ((meter_id - 1) % TRANSFORMER_COUNT) + 1
```

| Component | Count |
|---|---|
| Smart meters | 50 |
| Transformers | 5 |
| Meters per transformer | 10 |

---

## Output — Delta Lake Landing Zone

All data is written in **append mode** to the following Delta Lake paths:

```
/Volumes/electrical_grid/00_landing_zone/electrical_stream/
├── meters/
├── transformers/
└── grid_events/
```

---

### `/meters/`

One row per meter per batch — **50 rows every 30 seconds**.

| Column | Type | Description |
|---|---|---|
| `meter_id` | int | Unique meter identifier (1–50) |
| `transformer_id` | int | Upstream transformer serving this meter (1–5) |
| `event_time` | timestamp | Simulated reading time — up to 20s behind batch time |
| `voltage_v` | float | Voltage in volts — `0.0` if transformer is offline |
| `current_a` | float | Current in amperes — `0.0` if transformer is offline |
| `power_kw` | float | Active power in kilowatts (`voltage × current / 1000`) |
| `power_factor` | float | Power factor (0.92–0.99 range, randomised) |
| `energy_kwh_interval` | float | Energy consumed this batch interval in kWh |
| `ingestion_time` | timestamp | UTC time when the batch was written |

> **Note:** When a transformer trips, all 10 meters behind it report `voltage_v = 0.0` and `current_a = 0.0`, resulting in `power_kw = 0.0` and `energy_kwh_interval = 0.0` for the duration of the outage.

---

### `/transformers/`

One row per transformer per batch — **5 rows every 30 seconds**.

| Column | Type | Description |
|---|---|---|
| `transformer_id` | int | Unique transformer identifier (1–5) |
| `event_time` | timestamp | Simulated reading time — up to 20s behind batch time |
| `load_kw` | float | Total load — sum of `power_kw` across all connected meters |
| `capacity_kva` | int | Rated capacity in kVA (fixed at 500 kVA) |
| `status` | string | `"online"` or `"offline"` |
| `ingestion_time` | timestamp | UTC time when the batch was written |

> **Note:** `load_kw` drops to `0.0` when `status = "offline"` because all connected meters report zero power during an outage.

---

### `/grid_events/`

Written only when an outage starts or ends — **zero to a few rows per batch**.

| Column | Type | Description |
|---|---|---|
| `event_id` | string | Unique event identifier — format: `EVT_{transformer_id}_{epoch_ms}_{rand}` |
| `asset_type` | string | Always `"transformer"` in this simulation |
| `asset_id` | int | ID of the transformer that tripped or recovered |
| `event_type` | string | `"breaker_trip"` (outage starts) or `"breaker_reclose"` (recovery) |
| `event_time` | timestamp | UTC time the event occurred (equal to batch time) |
| `ingestion_time` | timestamp | UTC time when the batch was written |

---

## Outage Simulation Logic

Each batch, every **online** transformer independently has a 3% chance of going offline.

```
Batch N+0  →  Transformer trips  →  status = "offline", breaker_trip event logged
Batch N+1  →  Still offline      →  all 10 connected meters report 0.0
Batch N+2  →  Still offline      →  all 10 connected meters report 0.0
Batch N+3  →  Auto-recovers      →  status = "online", breaker_reclose event logged
```

- Outage duration is capped at `MAX_OUTAGE_BATCHES` (default: 3 batches ≈ 90 seconds)
- Multiple transformers can be offline at the same time
- Recovery is deterministic — no random roll needed to come back online
- Outage state is tracked in-memory in `outage_tracker`, a dictionary keyed by `transformer_id`

---

## Configuration

All key parameters are defined at the top of the notebook under `# CONFIG`:

| Variable | Default | Description |
|---|---|---|
| `METER_COUNT` | `50` | Number of smart meters to simulate |
| `TRANSFORMER_COUNT` | `5` | Number of transformers |
| `BATCH_INTERVAL_SECONDS` | `30` | Sleep time between batches (seconds) |
| `MAX_LATENCY_SECONDS` | `20` | Max simulated event-time lag behind batch time |
| `MAX_OUTAGE_BATCHES` | `3` | Number of batches a transformer stays offline before recovering |
| `TRANSFORMER_OFFLINE_PROB` | `0.03` | Per-batch probability that an online transformer trips |
| `LANDING_ZONE_METER_PATH` | `/Volumes/electrical_grid/00_landing_zone/electrical_stream/meters/` | Delta path for meter data |
| `LANDING_ZONE_TRANSFORMER_PATH` | `/Volumes/electrical_grid/00_landing_zone/electrical_stream/transformers/` | Delta path for transformer data |
| `LANDING_ZONE_EVENTS_PATH` | `/Volumes/electrical_grid/00_landing_zone/electrical_stream/grid_events/` | Delta path for grid events |

---

## How to Run

1. Attach the notebook to a running Databricks cluster with Delta Lake enabled.
2. Ensure the target Volume paths exist under `electrical_grid` in Unity Catalog.
3. Run all cells — the generator enters an infinite loop and prints a confirmation after each batch:
   ```
   Batch written at 2024-11-01T10:30:00+00:00
   ```
4. Interrupt the kernel to stop generation.

> This notebook is intended to run **before or alongside** any downstream streaming notebooks that read from the landing zone paths above.

---

## Dependencies

- Databricks Runtime with PySpark
- Delta Lake (included in Databricks Runtime)
- Unity Catalog Volume at `/Volumes/electrical_grid/00_landing_zone/`
