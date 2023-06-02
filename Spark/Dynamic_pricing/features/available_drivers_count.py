from Spark.Dynamic_pricing.data_sources import driver_locations_stream
from Spark.Dynamic_pricing.entities import zipcode
from tecton import stream_feature_view, Aggregation 
from tecton.aggregation_functions import approx_count_distinct
from datetime import datetime, timedelta

@stream_feature_view(
    description='''Number of available drivers in the given zipcode over the last 30 minutes, updated every 5 minutes.''',
    source=driver_locations_stream,
    entities=[zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=5),
    aggregations=[
        Aggregation(column='driver_id', function=approx_count_distinct(), time_window=timedelta(minutes=30))
    ],
    batch_schedule=timedelta(days=1),
)
def available_drivers_count(driver_locations_stream):
    return f"""
        SELECT zipcode, timestamp, driver_id
        FROM {driver_locations_stream}
        """