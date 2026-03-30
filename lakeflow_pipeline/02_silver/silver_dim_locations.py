from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

@dp.table(
    name="electrical_grid.02_silver.silver_dim_locations",
    comment="Static mapping: transformer and Lagos area, geo coordinates, service band"
)
def dim_locations():
    # this contains the locations of the different transformers and their service bands
    data = [
        (1, "Ikeja",           6.5964, 3.3400, "A"),
        (2, "Lekki Phase 1",   6.4485, 3.4727, "B"),
        (3, "Surulere",        6.4998, 3.3500, "C"),
        (4, "Victoria Island", 6.4281, 3.4215, "A"),
        (5, "Agege",           6.6250, 3.3167, "D")
    ]
    
    schema = StructType([
        StructField("transformer_id", IntegerType(), True),
        StructField("area_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("service_band", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)