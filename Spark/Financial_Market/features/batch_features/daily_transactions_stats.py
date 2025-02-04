from tecton import batch_feature_view, Aggregate
from Financial_Market.entities import stock
from Financial_Market.data_sources.stock_trades import stock_trades
from datetime import datetime, timedelta
from tecton.types import Field, Float64, Int64

@batch_feature_view(
    sources=[stock_trades],
    entities=[stock],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(minutes=60),
    timestamp_field="TIMESTAMP",
    description="Analysis on stock trades made each day.",
    features = [
        Aggregate(input_column=Field("PRICE", Float64), function="min", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("PRICE", Float64), function="max", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("TOTAL_SHARES", Int64), function="sum", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("DOLLAR_VOLUME", Float64), function="sum", time_window=timedelta(minutes=60))
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
