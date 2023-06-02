from Spark.Dynamic_pricing.data_sources import completed_rides_batch
from Spark.Dynamic_pricing.entities import origin_zipcode
from tecton import batch_feature_view, FilteredSource, Aggregation
from datetime import datetime, timedelta

@batch_feature_view(
    description='''Standard deviation of ride durations from the given zipcode over a series of time windows, updated daily.''',
    sources=[FilteredSource(completed_rides_batch)],
    entities=[origin_zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    aggregations=[
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=10)),
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=30)),
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=60)),
        Aggregation(column='duration', function='mean', time_window=timedelta(days=60))
    ]
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