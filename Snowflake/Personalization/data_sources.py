from tecton import BatchSource, SnowflakeConfig, PushSource
from tecton.types import Field, Int64, String, Timestamp, Array

gaming_user_batch = BatchSource(
    name="gaming_users",
    batch_config=SnowflakeConfig(
      database="DEMO_DB",
      schema="PUBLIC",
      table="ONLINE_GAMING_USERS",
    ),
)

gaming_transactions_batch = BatchSource(
    name="gaming_transactions",
    batch_config=SnowflakeConfig(
      database="DEMO_DB",
      schema="PUBLIC",
      table="ONLINE_GAMING_TRANSACTIONS"
    ),
)