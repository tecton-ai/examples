from Fraud.entities import user
from Fraud.data_sources import transactions_stream
from tecton import stream_feature_view
from datetime import datetime, timedelta

@stream_feature_view(
    description='''Latitude and longitude of the last transaction made by a user''',
    source=transactions_stream,
    entities=[user],
    mode='spark_sql',
    feature_start_time=datetime(2022,5, 1),
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1)
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