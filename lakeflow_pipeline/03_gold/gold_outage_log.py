from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.table(
    name="electrical_grid.03_gold.gold_outage_log",
    comment="One row per outage: trip→reclose pair with duration and location context."
)
def gold_outage_log():
    events = spark.read.table("electrical_grid.02_silver.silver_fact_grid_events")
    locs   = spark.read.table("electrical_grid.02_silver.silver_dim_locations")

    trips = (
        events
        .filter(F.col("event_type") == "breaker_trip")
        .select(
            F.col("transformer_id"),
            F.col("event_time").alias("outage_start"),
            F.col("event_id").alias("trip_event_id")
        )
    )

    recloses = (
        events
        .filter(F.col("event_type") == "breaker_reclose")
        .select(
            F.col("transformer_id"),
            F.col("event_time").alias("outage_end"),
            F.col("event_id").alias("reclose_event_id")
        )
    )

    w = Window.partitionBy("transformer_id").orderBy("outage_start")

    trips_ranked    = trips.withColumn("rn", F.row_number().over(w))
    recloses_ranked = recloses.withColumn("rn", F.row_number().over(
        Window.partitionBy("transformer_id").orderBy("outage_end")
    ))

    paired = (
        trips_ranked
        .join(recloses_ranked, on=["transformer_id", "rn"], how="left")
        .withColumn(
            "duration_minutes",
            F.round(
                (F.unix_timestamp("outage_end") - F.unix_timestamp("outage_start")) / 60,
                2
            )
        )
        .drop("rn")
    )

    return (
        paired
        .join(locs, on="transformer_id", how="left")
        .select(
            "trip_event_id",
            "reclose_event_id",
            "transformer_id",
            "area_name",
            "service_band",
            "outage_start",
            "outage_end",
            "duration_minutes",
            "latitude",
            "longitude"
        )
    )