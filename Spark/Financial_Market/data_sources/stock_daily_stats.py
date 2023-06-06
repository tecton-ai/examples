from tecton import HiveConfig, BatchSource, DatetimePartitionColumn

stock_daily_stats = BatchSource(
    name='stock_daily_stats',
    batch_config=HiveConfig(
        database='stock_demo_data',
        table='stock_daily_stats',
        timestamp_field='TIMESTAMP'
    ),
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)