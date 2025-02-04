from Fraud.entities import user
from Fraud.data_sources import transactions_batch
from tecton import batch_feature_view, Aggregate
from datetime import datetime, timedelta
from tecton.types import Field, Float64

@batch_feature_view(
    description='''Standard deviation of a user's transaction amounts 
                    over a series of time windows, updated daily.''',
    sources=[transactions_batch],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021,1,1),
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    features=[
        Aggregate(input_column=Field('amt', Float64), function='stddev_samp', time_window=timedelta(days=10)),
        Aggregate(input_column=Field('amt', Float64), function='stddev_samp', time_window=timedelta(days=30)),
        Aggregate(input_column=Field('amt', Float64), function='stddev_samp', time_window=timedelta(days=60)),
        Aggregate(input_column=Field('amt', Float64), function='mean', time_window=timedelta(days=60))
    ],
    timestamp_field="timestamp"
)
def user_dollar_spend(transactions):
    return f'''
        SELECT
            user_id,
            amt,
            timestamp
        FROM
            {transactions}
        '''