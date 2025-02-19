from Fraud.entities import user
from Fraud.data_sources import transactions_stream
from tecton import stream_feature_view, Attribute
from tecton.types import Float64
from datetime import datetime, timedelta

@stream_feature_view(
    description='''Latitude and longitude of the last transaction made by a user''',
    source=transactions_stream,
    entities=[user],
    mode='spark_sql',
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1),
    online=True,
    timestamp_field='timestamp',
    features=[
        Attribute('user_last_transaction_lat', Float64),
        Attribute('user_last_transaction_long', Float64)
    ]
)
def user_last_transaction_location(transactions_stream):
    return f'''select 
                user_id, 
                timestamp,
                merch_lat as user_last_transaction_lat,
                merch_long as user_last_transaction_long
            from
              {transactions_stream}
    '''