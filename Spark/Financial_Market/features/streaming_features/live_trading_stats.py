from datetime import datetime, timedelta
from tecton import stream_feature_view, Aggregation, FilteredSource, BatchTriggerType
from entities import stock
from data_sources.stock_transactions_event_source import stock_transactions_event_source
from tecton.types import Field, Timestamp, Int64, String

output_schema = [
    Field(name="SYMBOL", dtype=String),
    Field(name="PRICE", dtype=Int64),
    Field(name="QUANTITY", dtype=Int64),
    Field(name="DOLLAR_VOLUME", dtype=Int64),
    Field(name="TIMESTAMP", dtype=Timestamp)
]

@stream_feature_view(
    name="live_trading_stats",
    source=FilteredSource(stock_transactions_event_source),
    entities=[stock],
    online=True,
    offline=True,
    feature_start_time=datetime(2019, 1, 1),
    batch_schedule = timedelta(days=1),
    aggregations=[
        Aggregation(column="PRICE", function="min", time_window=timedelta(minutes=60)),
        Aggregation(column="PRICE", function="max", time_window=timedelta(minutes=60)),
        Aggregation(column="QUANTITY", function="sum", time_window=timedelta(minutes=60)),
        Aggregation(column="DOLLAR_VOLUME", function="sum", time_window=timedelta(minutes=60)),
    ],
    tags={"release": "production"},
    owner="nlee@tecton.ai",
    description="analysis of realtime stock transactions",
    mode="python",
    schema=output_schema,
)
def live_trading_stats(stock_transactions_event_source):
    stock_transactions_event_source["DOLLAR_VOLUME"] = stock_transactions_event_source["QUANTITY"] * stock_transactions_event_source["PRICE"]
    return stock_transactions_event_source