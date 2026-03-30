from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType

@dp.table(
    name="electrical_grid.02_silver.silver_fact_meters",
    comment="Meter readings every 30s, 10 per transformer. energy_kwh_interval = power_kw * (30/3600). This is used for billing."
)
@dp.expect_all_or_fail({
    "valid_meter_id":       "meter_id IS NOT NULL",
    "valid_transformer_id": "transformer_id IS NOT NULL",
    "valid_event_time":     "event_time IS NOT NULL",
})
def fact_meters():
    df = spark.readStream.table("electrical_grid.01_bronze.bronze_meters")

    return (
        df
        .withColumn("event_time",     to_timestamp(col("event_time")))
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
        .select(
            col("meter_id").cast(IntegerType()),
            col("transformer_id").cast(IntegerType()),
            col("voltage_v").cast(DoubleType()),
            col("current_a").cast(DoubleType()),
            col("power_kw").cast(DoubleType()),
            col("power_factor").cast(DoubleType()),
            col("energy_kwh_interval").cast(DoubleType()),
            col("event_time"),
            col("ingestion_time")
        )
    )