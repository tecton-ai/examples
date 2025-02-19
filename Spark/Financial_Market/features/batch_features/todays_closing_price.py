from tecton import batch_feature_view, Attribute
from Financial_Market.entities import stock
from Financial_Market.data_sources.stock_daily_stats import stock_daily_stats
from datetime import datetime, timedelta
from tecton.types import Float64

@batch_feature_view(
    sources=[stock_daily_stats],
    entities=[stock],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(hours=1),
    timestamp_field="TIMESTAMP",
    description="The closing price of a stock",
    ttl=timedelta(days=3650),
    features=[
        Attribute("CLOSE", Float64)
    ]
)
def todays_closing_price(stock_trades_batch):
    return f"""
        SELECT
            SYMBOL,
            CLOSE, 
            TIMESTAMP
        FROM
            {stock_trades_batch}
        """

