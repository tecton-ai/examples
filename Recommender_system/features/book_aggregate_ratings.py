from Recommender_system.entities import book
from datetime import datetime, timedelta
from Recommender_system.data_sources import ratings_batch
from tecton import batch_feature_view, FilteredSource, Aggregation

@batch_feature_view(
    description='''Book aggregate rating features over the past year and past 30 days.''',
    sources=[FilteredSource(ratings_batch)],
    entities=[book],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='rating', function='mean', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='mean', time_window=timedelta(days=30)),
        Aggregation(column='rating', function='stddev', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='stddev', time_window=timedelta(days=30)),
        Aggregation(column='rating', function='count', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='count', time_window=timedelta(days=30)),
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