from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Timestamp, Float64, Field
from Financial_Market.features.batch_features.todays_closing_price import todays_closing_price
from Financial_Market.features.batch_features.yesterday_closing_price import yesterday_closing_price

# Get FV for yesterday's closing price and today's closing price, and then do a calculation
@realtime_feature_view(
    sources=[yesterday_closing_price, todays_closing_price],
    mode="python",
    description="What is the percent of daily returns after the close today?",
    features=[Attribute("percent_daily_return", Float64)]
)

def percentage_daily_returns(yesterday_closing_price, todays_closing_price):
    percent_return = float(todays_closing_price["CLOSE"] - yesterday_closing_price["PREVIOUS_CLOSE"]) / yesterday_closing_price["PREVIOUS_CLOSE"]
    return {'percent_daily_return': percent_return}
