from Spark.Recommender_system.data_sources import ratings_with_book_metadata_stream
from Spark.Recommender_system.entities import user
from datetime import datetime, timedelta
from tecton import stream_feature_view, FilteredSource, Aggregation
from tecton.aggregation_functions import last_distinct

@stream_feature_view(
    description='''Ratings summaries of the user\'s most recent 200 book ratings.''',
    source=FilteredSource(ratings_with_book_metadata_stream),
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='rating_summary', function=last_distinct(200), time_window=timedelta(days=365), name='last_200_ratings'),
    ]
)
def user_recent_ratings(ratings_with_book_metadata):
    from pyspark.sql.functions import struct, to_json, col

    df = ratings_with_book_metadata.select(
        col("user_id"),
        col("rating_timestamp"),
        to_json(struct('rating', 'book_author', 'category')).alias("rating_summary")
    )

    return df