from Dynamic_pricing.data_sources.ride_requests import ride_requests_stream
from Dynamic_pricing.entities import origin_zipcode
from tecton import stream_feature_view, Aggregate 
from datetime import datetime, timedelta
from tecton.types import Field, String

@stream_feature_view(
    description='''Number of ride requests from the given origin zipcode over the last 30 minutes, updated every 5 minutes.''',
    source=ride_requests_stream,
    entities=[origin_zipcode],
    mode='pyspark',
    aggregation_interval=timedelta(minutes=5),
    features=[
        Aggregate(input_column=Field('request_id', String), function='count', time_window=timedelta(minutes=30))
    ],
    batch_schedule=timedelta(days=1),
    timestamp_field='timestamp'
)
def ride_request_count(ride_requests_stream):
    from pyspark.sql import functions as f
    return ride_requests_stream.select('origin_zipcode', 'timestamp', 'request_id')
