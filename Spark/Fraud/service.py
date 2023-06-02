from tecton import FeatureService
from Fraud.features.user_distance_previous_location import distance_previous_transaction
from Fraud.features.user_dollar_spend_aggregates import user_dollar_spend
from Fraud.features.user_merchant_transaction_counts import user_merchant_transactions_count
from Fraud.features.user_transaction_zscore import zscore_current_transaction

fs = FeatureService(
    name="fraud_feature_service",
    features =[
        distance_previous_transaction,
        user_dollar_spend,
        user_merchant_transactions_count,
        zscore_current_transaction
    ]
)