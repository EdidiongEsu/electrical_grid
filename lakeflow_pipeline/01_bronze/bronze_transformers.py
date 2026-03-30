from pyspark import pipelines as dp

@dp.table(name="electrical_grid.01_bronze.bronze_transformers", comment="Raw Transformer readings")
def bronze_transformers():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/electrical_grid/00_landing_zone/electrical_stream/transformers/")
    )


