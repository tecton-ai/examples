from Dynamic_pricing.data_sources.driver_locations import driver_locations_stream
from Dynamic_pricing.entities import zipcode
from tecton import stream_feature_view, Aggregate 
from tecton.aggregation_functions import approx_count_distinct
from datetime import datetime, timedelta
from tecton.types import Field, String

@stream_feature_view(
    description='''Number of available drivers in the given zipcode over the last 30 minutes, updated every 5 minutes.''',
    source=driver_locations_stream,
    entities=[zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=5),
    features=[
        Aggregate(input_column=Field('driver_id', String), function=approx_count_distinct(), time_window=timedelta(minutes=30))
    ],
    batch_schedule=timedelta(days=1),
    timestamp_field="timestamp"
)
def available_drivers_count(driver_locations_stream):
    return f"""
        SELECT zipcode, timestamp, driver_id
        FROM {driver_locations_stream}
        """