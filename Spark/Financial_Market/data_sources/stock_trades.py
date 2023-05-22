from tecton import HiveConfig, BatchSource, DatetimePartitionColumn

stock_trades = BatchSource(
    name='stock_trades_batch',
    batch_config=HiveConfig(
        database='stock_demo_data',
        table='stock_trades',
        timestamp_field='TIMESTAMP'
    ),
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)