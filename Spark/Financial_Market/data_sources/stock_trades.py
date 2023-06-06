from tecton import HiveConfig, BatchSource, DatetimePartitionColumn
# import pyspark


my_batch_config = HiveConfig(
        database='stock_demo_data',
        table='stock_trades',
        timestamp_field='TIMESTAMP',
    )

stock_trades = BatchSource(
    name='stock_trades_batch',
    batch_config= my_batch_config,
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)