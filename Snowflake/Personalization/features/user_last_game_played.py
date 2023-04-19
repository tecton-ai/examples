from datetime import datetime, timedelta
from tecton import StreamFeatureView, FilteredSource, Aggregation
from Snowflake.Personalization.entities import gaming_user
from Snowflake.Personalization.data_sources import gaming_event_source

user_last_game_played = StreamFeatureView(
    description="Game ID and Timestamp of last game played for a user"
    source=FilteredSource(gaming_user_embeddings),
    entities=[gaming_user],
    online=True,
    feature_start_time=datetime(2023,1, 1),
    batch_schedule=timedelta(days=1),
    tags={"release": "production"},
    ttl=timedelta(days=30)
)