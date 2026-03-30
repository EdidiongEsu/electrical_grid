from pyspark import pipelines as dp

@dp.table(name="electrical_grid.01_bronze.bronze_grid_events", comment="Raw grid_events")
def bronze_grid_events():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/electrical_grid/00_landing_zone/electrical_stream/grid_events/")
    )
