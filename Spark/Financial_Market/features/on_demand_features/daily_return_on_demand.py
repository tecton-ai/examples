from tecton import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Float64, Field
from features.batch_features.todays_closing_price import todays_closing_price
from features.batch_features.yesterday_closing_price import yesterday_closing_price

#This ODFV does not require a request input schema; it merely grabs two feature views and makes a real-time calculation.
output_schema = [Field("percent_daily_return", Float64)]

# Get FV for yesterday's closing price and today's closing price, and then do a calculation
@on_demand_feature_view(
    sources=[yesterday_closing_price, todays_closing_price],
    mode="python",
    schema=output_schema,
    description="What is the percent of daily returns after the close today?",
)

def percentage_daily_returns(yesterday_closing_price, todays_closing_price):
    percent_return = float(todays_closing_price["CLOSE"] - yesterday_closing_price["PREVIOUS_CLOSE"]) / yesterday_closing_price["PREVIOUS_CLOSE"]
    return {'percent_daily_return': percent_return}