from tecton import FileConfig, BatchSource, spark_stream_config, spark_batch_config, StreamSource

#Books metadata batch data source
books_batch = BatchSource(
    name='books_batch',
    batch_config=FileConfig(
        uri="s3://tecton-demo-data/apply-book-recsys/books_v3.parquet",
        file_format="parquet",
    ),
    owner='jake@tecton.ai',
    tags={'release': 'production'},
    description='Book metadata, e.g. the book title, author, category, language, etc.'
)

#User ratings batch data source
ratings_batch = BatchSource(
    name='ratings_batch',
    batch_config=FileConfig(
        uri="s3://tecton-demo-data/apply-book-recsys/ratings.parquet",
        file_format="parquet",
        timestamp_field='rating_timestamp',
    ),
    owner='jake@tecton.ai',
    tags={'release': 'production'},
    description='User book ratings.'
)

#User metadata batch data source
users_batch = BatchSource(
    name='users_batch',
    batch_config=FileConfig(
        uri="s3://tecton-demo-data/apply-book-recsys/users.parquet",
        file_format="parquet",
    ),
    owner='jake@tecton.ai',
    tags={'release': 'production'},
    description='User metadata, e.g. their sign-up date, age, and location.'
)

#Rating streaming events
def raw_data_deserialization(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
    )

    payload_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("isbn", StringType(), False),
            StructField("rating_timestamp", StringType(), False),
            StructField("rating", StringType(), False),
        ]
    )

    return (
        df.selectExpr("cast (data as STRING) jsonData")
        .select(from_json("jsonData", payload_schema).alias("payload"))
        .select(
            col("payload.user_id").alias("user_id"),
            col("payload.isbn").alias("isbn"),
            from_utc_timestamp("payload.rating_timestamp", "UTC").alias(
                "rating_timestamp"
            ),
            col("payload.rating").cast("long").alias("rating"),
        )
    )


@spark_stream_config()
def ratings_stream_config(spark):
    import datetime

    options = {
        "streamName": "book-ratings-stream",
        "region": "us-west-2",
        "shardFetchInterval": "30s",
        "initialPosition": "earliest",
        "roleArn": "arn:aws:iam::706752053316:role/tecton-demo-fraud-data-cross-account-kinesis-ro",
    }
    reader = spark.readStream.format("kinesis").options(**options)
    ratings_stream_df = reader.load()
    ratings_stream_df = raw_data_deserialization(ratings_stream_df)
    watermark = "{} seconds".format(datetime.timedelta(hours=25).seconds)
    ratings_stream_df = ratings_stream_df.withWatermark("rating_timestamp", watermark)

    book_metadata_df = spark.read.parquet(
        "s3://tecton-demo-data/apply-book-recsys/books_v3.parquet"
    ).select("isbn", "category", "book_author")

    joined_df = ratings_stream_df.join(book_metadata_df, ["isbn"], "left")

    return joined_df


@spark_batch_config(supports_time_filtering=True)
def ratings_batch_config(spark, filter_context):
    from pyspark.sql.functions import col

    ratings_df = spark.read.parquet(
        "s3://tecton-demo-data/apply-book-recsys/ratings.parquet"
    )
    if filter_context:
        if filter_context.start_time:
            ratings_df = ratings_df.where(
                col("rating_timestamp") >= filter_context.start_time
            )
        if filter_context.end_time:
            ratings_df = ratings_df.where(
                col("rating_timestamp") < filter_context.end_time
            )
    book_metadata_df = spark.read.parquet(
        "s3://tecton-demo-data/apply-book-recsys/books_v3.parquet"
    ).select("isbn", "category", "book_author")
    joined_df = ratings_df.join(book_metadata_df, ["isbn"], "left")
    return joined_df


ratings_with_book_metadata_stream = StreamSource(
    name="ratings_with_book_metadata_stream",
    stream_config=ratings_stream_config,
    batch_config=ratings_batch_config,
    description="A stream data source for user-book ratings. Book metadata (e.g. the book author and category) is joined onto the raw stream events from a file source.",
    owner="jake@tecton.ai",
    tags={"release": "production"},
)