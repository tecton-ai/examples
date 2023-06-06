from tecton import batch_feature_view, FilteredSource, Aggregation
from entities import stock
from data_sources.stock_trades import stock_trades
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(stock_trades)],
    entities=[stock],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(minutes=60),
    timestamp_field="TIMESTAMP",
    description="Analysis on stock trades made each day.",
    aggregations = [
        Aggregation(column="PRICE", function="min", time_window=timedelta(minutes=60)),
        Aggregation(column="PRICE", function="max", time_window=timedelta(minutes=60)),
        Aggregation(column="TOTAL_SHARES", function="sum", time_window=timedelta(minutes=60)),
        Aggregation(column="DOLLAR_VOLUME", function="sum", time_window=timedelta(minutes=60))
    ],
    aggregation_interval=timedelta(minutes=60),
    name = "daily_transactions_stats"
)
def max_prices_seen(stock_trades_batch):
    return f"""
        SELECT
            SYMBOL,
            QUANTITY as TOTAL_SHARES,
            PRICE, 
            QUANTITY*PRICE as DOLLAR_VOLUME,
            TIMESTAMP
        FROM
            {stock_trades_batch}
        """
