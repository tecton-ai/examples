from Dynamic_pricing.data_sources.completed_rides import completed_rides_batch
from Dynamic_pricing.entities import origin_zipcode
from tecton import batch_feature_view, Aggregate
from datetime import timedelta
from tecton.types import Field, Int64

@batch_feature_view(
    description='''Standard deviation of ride durations from the given zipcode over a series of time windows, updated daily.''',
    sources=[completed_rides_batch],
    entities=[origin_zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    features=[
        Aggregate(input_column=Field('duration', Int64), function='stddev_samp', time_window=timedelta(days=10)),
        Aggregate(input_column=Field('duration', Int64), function='stddev_samp', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('duration', Int64), function='stddev_samp', time_window=timedelta(days=60)),
        Aggregate(input_column=Field('duration', Int64), function='mean', time_window=timedelta(days=60))
    ],
    timestamp_field='timestamp'
)
def ride_durations(completed_rides_batch):
    return f'''
        SELECT
            origin_zipcode,
            duration,
            timestamp
        FROM
            {completed_rides_batch}
        '''