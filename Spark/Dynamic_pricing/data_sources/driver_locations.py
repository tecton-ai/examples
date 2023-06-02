from tecton import HiveConfig, BatchSource, StreamSource, KinesisConfig

def translate_stream(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import StructType, StructField, StringType

    stream_schema = StructType(
        [
            StructField("zipcode", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("timestamp", StringType(), False),
        ]
    )

    return (
        df.selectExpr("cast (data as STRING) jsonData")
        .select(from_json("jsonData", stream_schema).alias("payload"))
        .select(
            col("payload.zipcode").alias("zipcode"),
            col("payload.driver_id").alias("driver_id"),
            from_utc_timestamp("payload.timestamp", "UTC").alias("timestamp"),
        )
    )

driver_locations_batch_config = HiveConfig(
    database='drivers',
    table='driver_locations',
    timestamp_field='TIMESTAMP'
)

driver_locations_stream = StreamSource(
    name='driver_locations_stream',
    batch_config=driver_locations_batch_config,
    stream_config=KinesisConfig(
        stream_name='driver-locations-data-stream',
        region='us-west-2',
        post_processor=translate_stream,
        initial_stream_position='latest',
        timestamp_field='timestamp',
        options={'roleArn': 'arn:aws:iam::706752053316:role/driver-locations-data-cross-account-kinesis-ro'}
    ),

    tags={'project':'pricing'},
    owner='felix@tecton.ai'
)