from datetime import datetime, timedelta
from tecton import StreamFeatureView, Attribute
from tecton.types import Int64, String
from Personalization.entities import gaming_user
from Personalization.data_sources import gaming_event_source

user_last_game_played = StreamFeatureView(
    name="user_last_game_played",
    description="Game ID and Timestamp of last game played for a user",
    source=gaming_event_source,
    entities=[gaming_user],
    features=[
        Attribute('GAME_ID', Int64),
        Attribute('TIME_GAME_PLAYED', String)
    ],
    timestamp_field='EVENT_TS',
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    tags={"release": "production"},
    ttl=timedelta(days=30)
)
