from tecton import BatchSource, HiveConfig, HiveConfig, StreamSource, KinesisConfig



product_attributes_src = BatchSource(
    name= "product_attributes",
    batch_config=HiveConfig(
      database="my_db", 
      table="attributes"
      )
)

product_title_source = BatchSource(
    name= "product_tile",
    batch_config=HiveConfig(
      database="my_db", 
      table="search_queries"
      )
)

search_batch_config = HiveConfig(
        database='my_db',
        table='search_user_interactions',
        timestamp_field='timestamp'
    )

search_user_interactions = BatchSource(
    name= "search_user_interactions",
    batch_config=search_batch_config
)

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
        StructField("product_uid", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event", StringType(), False),
      
    ]
)

    return (
        df.selectExpr("cast (data as STRING) jsonData")
        .select(from_json("jsonData", stream_schema).alias("s"))
        .select("s.*")
    )


search_interaction_stream = StreamSource(
    name='search_user_interactions_stream',
    batch_config=search_batch_config,
    stream_config=KinesisConfig(
        stream_name='tecton-demo-orders',
        region='us-west-2',
        post_processor=translate_stream,
        initial_stream_position='latest',
        timestamp_field='timestamp',
        options={'roleArn': 'TECTON_KINESIS_ROLE'}
    ),

    tags={'project':'search'},
    owner='vince@tecton.ai'
)