from tecton import HiveConfig, BatchSource

completed_rides_batch_config = HiveConfig(
    database='rides',
    table='completed_rides',
    timestamp_field='TIMESTAMP'
)

completed_rides_batch = BatchSource(
    name='completed_rides_batch',
    batch_config=completed_rides_batch_config,
    tags={'release': 'production'}
)