from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, col

@dp.table(
    name="electrical_grid.02_silver.silver_dim_tariffs",
    comment="Service Reflective Tariff bands. Value is ₦/kWh (NERC SRT rates)"
)
def dim_tariffs():

    data = [
        ("A", 209.50, "2025-07-01", "High-supply premium areas"),
        ("B",  64.07, "2025-01-01", "Medium reliability"),
        ("C",  48.53, "2025-01-01", "Average residential"),
        ("D",  43.27, "2025-01-01", "Low supply areas"),
        ("E",  31.50, "2025-01-01", "Heavily subsidized / very low supply")
    ]

    schema = StructType([
        StructField("service_band", StringType(), True),
        StructField("tariff_price_per_kwh", DoubleType(), True),
        StructField("effective_from", StringType(), True),  # cast after
        StructField("notes", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    return df.withColumn(
        "effective_from",
        to_date(col("effective_from"))
    )