from tecton import batch_feature_view, Aggregate
from Financial_Market.entities import stock
from Financial_Market.data_sources.stock_daily_stats import stock_daily_stats
from datetime import datetime, timedelta
from tecton.types import Field, Float64

@batch_feature_view(
    sources=[stock_daily_stats],
    entities=[stock],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2016, 1, 1),
    batch_schedule=timedelta(hours = 1),
    timestamp_field="TIMESTAMP",
    description="Analysis on closing prices seen over different timespans.",
    features = [
        Aggregate(input_column=Field("CLOSE", Float64), function="max", time_window=timedelta(days=3)),
        Aggregate(input_column=Field("CLOSE", Float64), function="max", time_window=timedelta(days=7)),
        Aggregate(input_column=Field("CLOSE", Float64), function="max", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("CLOSE", Float64), function="min", time_window=timedelta(days=3)),
        Aggregate(input_column=Field("CLOSE", Float64), function="min", time_window=timedelta(days=7)),
        Aggregate(input_column=Field("CLOSE", Float64), function="min", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("CLOSE", Float64), function="mean", time_window=timedelta(days=3)),
        Aggregate(input_column=Field("CLOSE", Float64), function="mean", time_window=timedelta(days=7)),
        Aggregate(input_column=Field("CLOSE", Float64), function="mean", time_window=timedelta(days=30)),
    ],
    aggregation_interval=timedelta(hours=1),
    name = "closing_prices_metrics",
)
def closing_price_metrics(stock_trades_batch):
    return f"""
        SELECT
            SYMBOL,
            CLOSE,
            TIMESTAMP
        FROM
            {stock_trades_batch}
        """

