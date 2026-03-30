from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="electrical_grid.03_gold.gold_billing_summary",
    comment="Daily energy consumption and cost estimate per meter using NERC SRT band rates."
)
def gold_billing_summary():
    meters  = spark.read.table("electrical_grid.02_silver.silver_fact_meters")
    locs    = spark.read.table("electrical_grid.02_silver.silver_dim_locations")
    tariffs = spark.read.table("electrical_grid.02_silver.silver_dim_tariffs")

    daily = (
        meters
        .withColumn("date", F.to_date(F.col("event_time")))
        .groupBy("meter_id", "transformer_id", "date")
        .agg(
            F.round(F.sum("energy_kwh_interval"), 4).alias("total_kwh"),
            F.round(F.avg("power_kw"), 2).alias("avg_power_kw"),
            F.round(F.avg("power_factor"), 3).alias("avg_power_factor"),
            F.count("*").alias("reading_count")
        )
    )

    with_band = daily.join(locs, on="transformer_id", how="left")

    latest_tariffs = (
        tariffs
        .groupBy("service_band")
        .agg(F.max_by("tariff_price_per_kwh", "effective_from").alias("tariff_price_per_kwh"))
    )

    return (
        with_band
        .join(latest_tariffs, on="service_band", how="left")
        .withColumn(
            "estimated_cost_naira",
            F.round(
                F.col("total_kwh") * 
                F.when(F.col("tariff_price_per_kwh").isNotNull(), F.col("tariff_price_per_kwh")).otherwise(0), 
                2
            )
        )
        .select(
            "meter_id",
            "transformer_id",
            "area_name",
            "service_band",
            "date",
            "total_kwh",
            "avg_power_kw",
            "avg_power_factor",
            "tariff_price_per_kwh",
            "estimated_cost_naira",
            "reading_count"
        )
    )