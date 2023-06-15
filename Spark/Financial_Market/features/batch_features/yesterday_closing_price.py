from tecton import batch_feature_view, FilteredSource, Aggregation
from tecton.aggregation_functions import last
from entities import stock
from data_sources.stock_daily_stats import stock_daily_stats
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(stock_daily_stats)],
    entities=[stock],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(hours=1),
    timestamp_field="TIMESTAMP",
    description="The previous day's closing price (so you can do a day-to-day comparison)",
    name = "yesterday_closing_price",
    ttl=timedelta(days=3650)
)
def yesterday_closing_price(stock_trades_batch):
    return f"""
        SELECT
            SYMBOL,
            LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY TIMESTAMP) as PREVIOUS_CLOSE,
            TIMESTAMP
        FROM
            {stock_trades_batch}
        """
