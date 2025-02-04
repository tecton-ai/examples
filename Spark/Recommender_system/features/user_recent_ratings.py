from Recommender_system.data_sources import ratings_with_book_metadata_stream
from Recommender_system.entities import user
from datetime import datetime, timedelta
from tecton import stream_feature_view, Aggregate
from tecton.aggregation_functions import last_distinct
from tecton.types import Field, String, Timestamp

@stream_feature_view(
    description='''Ratings summaries of the user\'s most recent 200 book ratings.''',
    source=ratings_with_book_metadata_stream,
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    timestamp_field='rating_timestamp',
    features=[
        Aggregate(input_column=Field('rating_summary', String), function=last_distinct(200), time_window=timedelta(days=365), name='last_200_ratings'),
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