from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="electrical_grid.03_gold.gold_billing_by_area",
    comment="Daily billing summary aggregated by area (perfect for Lekki, Victoria Island, etc.)"
)
def gold_billing_by_area():
    billing = spark.read.table("electrical_grid.03_gold.gold_billing_summary")

    return (
        billing
        .groupBy("date", "area_name", "service_band")
        .agg(
            F.round(F.sum("total_kwh"),4).alias("total_kwh"),
            F.sum("estimated_cost_naira").alias("total_cost_naira"),
            F.countDistinct("meter_id").alias("active_meters")
        )
        .withColumn("total_cost_naira", F.round(F.col("total_cost_naira"), 2))
        .select(
            "date",
            "area_name",
            "service_band",
            "total_kwh",
            "total_cost_naira",
            "active_meters"
        )
    )