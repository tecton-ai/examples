from tecton import PushSource, HiveConfig
from tecton.types import String, Int64, Timestamp, Field

input_schema = [
    Field(name="trade_identification", dtype=String),
    Field(name="SYMBOL", dtype=String),
    Field(name="QUANTITY", dtype=Int64),
    Field(name="PRICE", dtype=Int64),
    Field(name="TIMESTAMP", dtype=Timestamp),
]

#This is sample code that can be used to re-cast variable types if necessary.
# This code block is left here for instructional purposes.
# def post_processor_batch(df):
#     from pyspark.sql.functions import col
#
#     df = df.select(
#         col("trade_identification").cast("string").alias("trade_identification"),
#         col("SYMBOL").cast("string").alias("SYMBOL"),
#         col("QUANTITY").cast("long").alias("QUANTITY"),
#         col("PRICE").cast("long").alias("PRICE"),
#         col("TIMESTAMP").cast("timestamp").alias("TIMESTAMP")
#     )
#     return df

stock_transactions_event_source = PushSource(
    name="stock_transactions_event_source",
    schema=input_schema,
    batch_config=HiveConfig(
        database="stock_demo_data",
        table="stock_trades",
        #post_processor=post_processor_batch,
        timestamp_field="TIMESTAMP",
    ),
    description="Push Source for second-by-second trade information",
    owner="nlee@tecton.ai",
    tags={"release": "production"},
)