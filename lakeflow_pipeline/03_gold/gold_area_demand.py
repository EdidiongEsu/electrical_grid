from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="electrical_grid.03_gold.gold_area_demand",
    comment="15-min geo-tagged demand per transformer/area. Load % correctly calculated using apparent power (kVA)."
)
def gold_area_demand():
    meters = spark.read.table("electrical_grid.02_silver.silver_fact_meters")
    tf     = spark.read.table("electrical_grid.02_silver.silver_fact_transformers")
    locs   = spark.read.table("electrical_grid.02_silver.silver_dim_locations")

    # Watermark + 15-min window aggregation
    meter_windowed = (
        meters
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            "transformer_id",
            F.window(F.col("event_time"), "15 minutes")
        )
        .agg(
            F.round(F.sum("power_kw"), 2).alias("total_load_kw"),
            F.round(F.sum("energy_kwh_interval"), 4).alias("total_energy_kwh"),
            F.countDistinct("meter_id").alias("active_meter_count"),
            F.round(F.avg("voltage_v"), 2).alias("avg_voltage_v"),
            F.round(F.avg("power_factor"), 3).alias("avg_power_factor")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # Transformer capacity
    capacity = (
        tf
        .groupBy("transformer_id")
        .agg(F.max("capacity_kva").alias("capacity_kva"))
    )

    return (
        meter_windowed
        .join(capacity, on="transformer_id", how="left")
        .join(locs,     on="transformer_id", how="left")
        .withColumn(
            "apparent_load_kva",
            F.round(
                F.col("total_load_kw") / 
                F.when(F.col("avg_power_factor") > 0, F.col("avg_power_factor")).otherwise(1.0), 
                2
            )
        )
        .withColumn(
            "load_pct",
            F.round(F.col("apparent_load_kva") / F.col("capacity_kva") * 100, 2)
        )
        .withColumn(
            "headroom_kva",
            F.round(F.col("capacity_kva") - F.col("apparent_load_kva"), 2)
        )
        .withColumn(
            "headroom_kw",
            F.round(F.col("capacity_kva") * F.col("avg_power_factor") - F.col("total_load_kw"), 2)
        )
        .select(
            "transformer_id",
            "area_name",
            "service_band",
            "latitude",
            "longitude",
            "window_start",
            "window_end",
            "total_load_kw",
            "apparent_load_kva",
            "total_energy_kwh",
            "load_pct",
            "headroom_kva",
            "headroom_kw",
            "capacity_kva",
            "active_meter_count",
            "avg_voltage_v",
            "avg_power_factor"
        )
    )