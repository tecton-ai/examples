from tecton.types import String, Timestamp, Float64, Field, Int64
from tecton import on_demand_feature_view, RequestSource
from Spark.Fraud.features.user_dollar_spend_aggregates import user_dollar_spend

request_schema = [Field('user_id', String),Field('amt', Float64)]
transaction_request = RequestSource(schema=request_schema)
output_schema=[Field('zscore_transaction_amount', Float64)]

@on_demand_feature_view(
    description='''Z-score of the current transaction amount for a user based on 60 days mean and standard deviation''',
    sources=[transaction_request, user_dollar_spend],
    mode='pandas',
    schema=output_schema
)
def zscore_current_transaction(transaction_request, user_dollar_spend):
    import pandas
    
    user_dollar_spend['amount'] = transaction_request['amt']
    user_dollar_spend['zscore_transaction_amount'] = (user_dollar_spend['amount'] - user_dollar_spend['amt_mean_60d_1d']) / user_dollar_spend['amt_stddev_samp_60d_1d']

    return user_dollar_spend[['zscore_transaction_amount']]

