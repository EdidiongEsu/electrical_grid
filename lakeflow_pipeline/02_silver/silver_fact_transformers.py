from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, when, round as spark_round
from pyspark.sql.types import IntegerType, DoubleType, StringType

@dp.table(
    name="electrical_grid.02_silver.silver_fact_transformers",
    comment="Transformer snapshots every 30s. load_pct = load_kw/500kva, is_offline = outage_tracker > 0 in generator."
)
@dp.expect_all_or_fail({
    "valid_transformer_id": "transformer_id IS NOT NULL",
    "valid_event_time":     "event_time IS NOT NULL",
})
def fact_transformers():
    df = spark.readStream.table("electrical_grid.01_bronze.bronze_transformers")

    return (
        df
        .withColumn("event_time",     to_timestamp(col("event_time")))
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
        .withColumn(
            "load_pct",
            spark_round(col("load_kw") / col("capacity_kva") * 100, 2)
        )
        .withColumn(
            "is_offline",
            when(col("status") == "offline", True).otherwise(False)
        )
        .select(
            col("transformer_id").cast(IntegerType()),
            col("capacity_kva").cast(IntegerType()),
            col("load_kw").cast(DoubleType()),
            col("load_pct"),
            col("status").cast(StringType()),
            col("is_offline"),
            col("event_time"),
            col("ingestion_time")
        )
    )