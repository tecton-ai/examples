from Recommender_system.data_sources import books_batch
from Recommender_system.entities import book
from datetime import datetime, timedelta
from tecton import batch_feature_view
from tecton.types import String, Timestamp
from tecton import Attribute

@batch_feature_view(
    description='Book metadata features.',
    sources=[books_batch],
    entities=[book],
    mode='spark_sql',
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1),
    timestamp_field='created_at',
    features=[
        Attribute('book_title', String),
        Attribute('book_author', String),
        Attribute('year_of_publication', String),
        Attribute('category', String)
    ]
)
def book_metadata_features(books):
    return f'''
        SELECT
            isbn,
            created_at,
            book_title,
            book_author,
            year_of_publication,
            category
        FROM
            {books}
        '''