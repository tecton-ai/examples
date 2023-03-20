from Fraud.data_sources import transactions_stream
from Fraud.entities import user, merchant
from tecton import stream_feature_view, Aggregation 
from datetime import datetime, timedelta

@stream_feature_view(
    description='''Count number of transactions for this user at this same merchant in the last 30 minutes, updated every 5 minutes''',
    source=transactions_stream,
    entities=[user, merchant],
    mode='pyspark',
    aggregation_interval=timedelta(minutes=5),
    aggregations=[
        Aggregation(column='transaction_id', function='count', time_window=timedelta(minutes=30))
    ],
    feature_start_time=datetime(2022,5, 1),
    batch_schedule=timedelta(days=1),
    online=True

)
def user_merchant_transactions_count(transactions_stream):
  from pyspark.sql import functions as f
  return transactions_stream.select('user_id','merchant','transaction_id','timestamp')