from tecton import FileConfig, BatchSource, StreamSource, KinesisConfig

def translate_stream(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        TimestampType,
        BooleanType,
        IntegerType,
        LongType
    )

    stream_schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amt", DoubleType(), False),
        StructField("is_fraud", LongType(), False),
        StructField("merchant", StringType(), False),
        StructField("merch_lat", DoubleType(), False),
        StructField("merch_long", DoubleType(), False),
        StructField("timestamp", StringType(), False),
    ]
)

    return (
        df.selectExpr("cast (data as STRING) jsonData")
        .select(from_json("jsonData", stream_schema).alias("payload"))
        .select(
            col("payload.user_id").alias("user_id"),
            col("payload.transaction_id").alias("transaction_id"),
            col("payload.category").alias("category"),
            col("payload.amt").cast("double").alias("amt"),
            col("payload.is_fraud").cast("long").alias("is_fraud"),
            col("payload.merchant").alias("merchant"),
            col("payload.merch_lat").cast("double").alias("merch_lat"),
            col("payload.merch_long").cast("double").alias("merch_long"),
            from_utc_timestamp("payload.timestamp", "UTC").alias("timestamp"),
        )
    )

transactions_batch_config = FileConfig(
        uri='s3://tecton.ai.public/tutorials/fraud_demo/transactions/',
        file_format='parquet',
        timestamp_field='timestamp'
    )

transactions_batch = BatchSource(
    name='transactions_batch',
    batch_config=transactions_batch_config,
    description='historical transaction data from payment provider',
    tags={'project':'fraud'},
    owner='vince@tecton.ai'
)

transactions_stream = StreamSource(
    name='transactions_stream',
    batch_config=transactions_batch_config,
    stream_config=KinesisConfig(
        stream_name='tecton-demo-fraud-data-stream',
        region='us-west-2',
        post_processor=translate_stream,
        initial_stream_position='latest',
        timestamp_field='timestamp',
        options={'roleArn': 'arn:aws:iam::706752053316:role/tecton-demo-fraud-data-cross-account-kinesis-ro'}
    ),

    tags={'project':'fraud'},
    owner='vince@tecton.ai'
)