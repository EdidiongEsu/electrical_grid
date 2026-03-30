from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="electrical_grid.03_gold.gold_transformer_health",
    comment="15-min windowed health metrics per transformer: load, uptime, and peak values."
)
def gold_transformer_health():
    tf   = spark.read.table("electrical_grid.02_silver.silver_fact_transformers")
    locs = spark.read.table("electrical_grid.02_silver.silver_dim_locations")

    windowed = (
        tf
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            "transformer_id",
            F.window(F.col("event_time"), "15 minutes")
        )
        .agg(
            F.round(F.avg("load_pct"),  2).alias("avg_load_pct"),
            F.round(F.max("load_pct"),  2).alias("peak_load_pct"),
            F.round(F.avg("load_kw"),   2).alias("avg_load_kw"),
            F.round(F.max("load_kw"),   2).alias("peak_load_kw"),
            F.round(
                F.sum(F.when(F.col("is_offline"), 0.5).otherwise(0)),
                1
            ).alias("offline_minutes"),
            F.count("*").alias("snapshot_count")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .withColumn(
            "uptime_pct",
            F.round(
                (1 - F.col("offline_minutes") / 15) * 100,
                2
            )
        )
        .drop("window")
    )

    return (
        windowed
        .join(locs, on="transformer_id", how="left")
        .select(
            "transformer_id",
            "area_name",
            "service_band",
            "window_start",
            "window_end",
            "avg_load_pct",
            "peak_load_pct",
            "avg_load_kw",
            "peak_load_kw",
            "offline_minutes",
            "uptime_pct",
            "snapshot_count"
        )
    )