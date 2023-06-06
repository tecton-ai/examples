from tecton import batch_feature_view, FilteredSource, Aggregation
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
    batch_schedule=timedelta(hours = 1),
    timestamp_field="TIMESTAMP",
    description="Analysis on closing prices seen over different timespans.",
    aggregations = [
        Aggregation(column="CLOSE", function="max", time_window=timedelta(days=3)),
        Aggregation(column="CLOSE", function="max", time_window=timedelta(days=7)),
        Aggregation(column="CLOSE", function="max", time_window=timedelta(days=30)),
        Aggregation(column="CLOSE", function="min", time_window=timedelta(days=3)),
        Aggregation(column="CLOSE", function="min", time_window=timedelta(days=7)),
        Aggregation(column="CLOSE", function="min", time_window=timedelta(days=30)),
        Aggregation(column="CLOSE", function="mean", time_window=timedelta(days=3)),
        Aggregation(column="CLOSE", function="mean", time_window=timedelta(days=7)),
        Aggregation(column="CLOSE", function="mean", time_window=timedelta(days=30)),
    ],
    aggregation_interval=timedelta(hours=1),
    name = "closing_prices_metrics"
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

