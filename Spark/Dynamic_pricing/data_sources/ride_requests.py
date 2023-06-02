from tecton import HiveConfig, BatchSource, StreamSource, KinesisConfig

def translate_stream(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import StructType, StructField, StringType

    stream_schema = StructType(
        [
            StructField("origin_zipcode", StringType(), False),
            StructField("destination_zipcode", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("request_id", StringType(), False),
            StructField("timestamp", StringType(), False),
        ]
    )

    return (
        df.selectExpr("cast (data as STRING) jsonData")
        .select(from_json("jsonData", stream_schema).alias("payload"))
        .select(
            col("payload.origin_zipcode").alias("origin_zipcode"),
            col("payload.destination_zipcode").alias("destination_zipcode"),
            col("payload.user_id").alias("user_id"),
            col("payload.request_id").alias("request_id"),
            from_utc_timestamp("payload.timestamp", "UTC").alias("timestamp"),
        )
    )

ride_requests_batch_config = HiveConfig(
    database='rides',
    table='ride_requests',
    timestamp_field='TIMESTAMP'
)

ride_requests_stream = StreamSource(
    name='ride_requests_stream',
    batch_config=ride_requests_batch_config,
    stream_config=KinesisConfig(
        stream_name='ride-requests-data-stream',
        region='us-west-2',
        post_processor=translate_stream,
        initial_stream_position='latest',
        timestamp_field='timestamp',
        options={'roleArn': 'arn:aws:iam::706752053316:role/ride-requests-data-cross-account-kinesis-ro'}
    ),

    tags={'project':'pricing'},
    owner='felix@tecton.ai'
)