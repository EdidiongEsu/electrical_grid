from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import IntegerType, StringType

@dp.table(
    name="electrical_grid.02_silver.silver_fact_grid_events",
    comment="Trip and reclose events affecting the small grid. One trip + one reclose = one outage."
)
@dp.expect_all_or_fail({
    "valid_event_id":       "event_id IS NOT NULL",
    "valid_transformer_id": "transformer_id IS NOT NULL",
    "valid_event_time":     "event_time IS NOT NULL",
})
def fact_grid_events():
    df = spark.readStream.table("electrical_grid.01_bronze.bronze_grid_events")

    return (
        df
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
        .withColumn(
            "is_trip",
            when(col("event_type") == "breaker_trip", True).otherwise(False)
        )
        .withColumn(
            "event_direction",
            when(col("event_type") == "breaker_trip", "outage_start")
            .when(col("event_type") == "breaker_reclose", "outage_end")
            .otherwise("unknown")
        )
        .select(
            col("event_id").cast(StringType()),
            col("asset_id").cast(IntegerType()).alias("transformer_id"),
            col("asset_type").cast(StringType()),
            col("event_type").cast(StringType()),
            col("event_direction"),
            col("is_trip"),
            col("event_time"),
            col("ingestion_time")
        )
    )