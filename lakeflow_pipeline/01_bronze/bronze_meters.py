from pyspark import pipelines as dp

# Bronze: raw ingestion of meters streams

@dp.table(name="electrical_grid.01_bronze.bronze_meters", comment="Raw meter readings")
def bronze_meters():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/electrical_grid/00_landing_zone/electrical_stream/meters/")
    )


