from tecton import FeatureService

from features.batch_features.closing_prices_metrics import closing_price_metrics
from features.batch_features.todays_closing_price import todays_closing_price
from features.batch_features.yesterday_closing_price import yesterday_closing_price
from features.on_demand_features.daily_return_on_demand import percentage_daily_returns

stock_daily_stats_feature_service = FeatureService(
    name="stock_daily_stats_feature_service",
    online_serving_enabled=True,
    features=[
        todays_closing_price,
        yesterday_closing_price,
        percentage_daily_returns,
        closing_price_metrics,
    ],
)