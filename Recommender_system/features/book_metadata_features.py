from Recommender_system.data_sources import books_batch
from Recommender_system.entities import book
from datetime import datetime, timedelta
from tecton import batch_feature_view


@batch_feature_view(
    description='Book metadata features.',
    sources=[books_batch],
    entities=[book],
    mode='spark_sql',
    feature_start_time=datetime(2018, 1, 1) ,
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1)
 
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