from tecton.types import String, Float64, Field
from tecton import realtime_feature_view, RequestSource, Attribute
from Fraud.features.user_dollar_spend_aggregates import user_dollar_spend

request_schema = [Field('user_id', String), Field('amt', Float64)]
transaction_request = RequestSource(schema=request_schema)

@realtime_feature_view(
    description='''Z-score of the current transaction amount for a user based on 60 days mean and standard deviation''',
    sources=[transaction_request, user_dollar_spend],
    mode='pandas',
    features=[
        Attribute('zscore_transaction_amount', Float64)
    ]
)
def zscore_current_transaction(transaction_request, user_dollar_spend):
    import pandas as pd
    
    user_dollar_spend['amount'] = transaction_request['amt']
    user_dollar_spend['zscore_transaction_amount'] = (user_dollar_spend['amount'] - user_dollar_spend['amt_mean_60d_1d']) / user_dollar_spend['amt_stddev_samp_60d_1d']

    return user_dollar_spend[['zscore_transaction_amount']]

