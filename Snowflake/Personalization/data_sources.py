from tecton import BatchSource, SnowflakeConfig, PushSource
from tecton.types import Field, Int64, String, Timestamp, Array

gaming_user_batch = BatchSource(
    name="gaming_users",
    batch_config=SnowflakeConfig(
      database="VINCE_DEMO_DB",
      schema="PUBLIC",
      table="ONLINE_GAMING_USERS",
    ),
)

gaming_transactions_batch = BatchSource(
    name="gaming_transactions",
    batch_config=SnowflakeConfig(
      database="VINCE_DEMO_DB",
      schema="PUBLIC",
      table="ONLINE_GAMING_TRANSACTIONS"
    ),
)

# Declare a schema for the Push Source
input_schema = [
    Field(name='USER_ID', dtype=String),
    Field(name='EVENT_TS', dtype=Timestamp),
    Field(name='TIME_GAME_PLAYED', dtype=String),
    Field(name='GAME_ID', dtype=Int64),
]

# Declare a PushSource with a name, schema and a batch_config parameters
# See the API documentation for BatchConfig
gaming_event_source = PushSource(
                        name="gaming_event_source",
                        schema=input_schema,
                        batch_config=SnowflakeConfig(
                        database="VINCE_DEMO_DB",
                        schema="PUBLIC",
                        table="ONLINE_GAMING_EVENTS"
                        ),
                        description="Push source for game played events"
                        )