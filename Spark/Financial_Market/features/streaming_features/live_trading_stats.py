from datetime import datetime, timedelta
from tecton import stream_feature_view, Aggregate, BatchTriggerType
from Financial_Market.entities import stock
from Financial_Market.data_sources.stock_transactions_event_source import stock_transactions_event_source
from tecton.types import Field, Timestamp, Int64, String

@stream_feature_view(
    name="live_trading_stats",
    source=stock_transactions_event_source,
    entities=[stock],
    online=True,
    offline=True,
    feature_start_time=datetime(2019, 1, 1),
    batch_schedule = timedelta(days=1),
    features=[
        Aggregate(input_column=Field("PRICE", Int64), function="min", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("PRICE", Int64), function="max", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("QUANTITY", Int64), function="sum", time_window=timedelta(minutes=60)),
        Aggregate(input_column=Field("DOLLAR_VOLUME", Int64), function="sum", time_window=timedelta(minutes=60)),
    ],
    tags={"release": "production"},
    description="analysis of realtime stock transactions",
    mode="python",
    timestamp_field="TIMESTAMP"
)
def live_trading_stats(stock_transactions_event_source):
    stock_transactions_event_source["DOLLAR_VOLUME"] = stock_transactions_event_source["QUANTITY"] * stock_transactions_event_source["PRICE"]
    return stock_transactions_event_source