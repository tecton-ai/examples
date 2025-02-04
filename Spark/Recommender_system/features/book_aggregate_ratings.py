from Recommender_system.entities import book
from datetime import datetime, timedelta
from Recommender_system.data_sources import ratings_batch
from tecton import batch_feature_view, Aggregate
from tecton.types import Float64, Field

@batch_feature_view(
    description='''Book aggregate rating features over the past year and past 30 days.''',
    sources=[ratings_batch],
    entities=[book],
    mode='spark_sql',
    timestamp_field='rating_timestamp',
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('rating', Float64), function='mean', time_window=timedelta(days=365)),
        Aggregate(input_column=Field('rating', Float64), function='mean', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('rating', Float64), function='stddev', time_window=timedelta(days=365)),
        Aggregate(input_column=Field('rating', Float64), function='stddev', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('rating', Float64), function='count', time_window=timedelta(days=365)),
        Aggregate(input_column=Field('rating', Float64), function='count', time_window=timedelta(days=30)),
    ]
)
def book_aggregate_ratings(ratings):
    return f'''
        SELECT
            isbn,
            rating_timestamp,
            rating
        FROM
            {ratings}
        '''