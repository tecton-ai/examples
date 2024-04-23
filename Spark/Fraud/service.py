from tecton import FeatureService
from Fraud.features.user_distance_previous_location import distance_previous_transaction
from Fraud.features.user_dollar_spend_aggregates import user_dollar_spend
from Fraud.features.user_merchant_transaction_counts import user_merchant_transactions_count
from Fraud.features.user_transaction_zscore import zscore_current_transaction
from Fraud.features.transaction_location import geocoded_address

fs = FeatureService(
    name="fraud_feature_service",
    features =[
        distance_previous_transaction,
        user_dollar_spend,
        user_merchant_transactions_count,
        zscore_current_transaction,
        geocoded_address
    ],
    on_demand_environment="tecton-python-extended:0.5"
)