from Fraud.entities import user
from Fraud.data_sources import transactions_batch
from tecton import batch_feature_view, FilteredSource, Aggregation
from datetime import datetime, timedelta

@batch_feature_view(
    description='''Standard deviation of a user's transaction amounts 
                    over a series of time windows, updated daily.''',
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    aggregations=[
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=10)),
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=30)),
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=60))
    ],
    feature_start_time=datetime(2022, 5, 1)
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