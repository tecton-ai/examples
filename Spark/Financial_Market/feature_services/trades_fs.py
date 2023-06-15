from tecton import FeatureService

from features.batch_features.closing_prices_metrics import closing_price_metrics
from features.on_demand_features.daily_return_on_demand import percentage_daily_returns
from features.streaming_features.live_trading_stats import live_trading_stats

live_trading_stats_feature_service = FeatureService(
    name="live_trading_stats",
    online_serving_enabled=True,
    features=[
        percentage_daily_returns,
        closing_price_metrics,
        live_trading_stats
    ],
)