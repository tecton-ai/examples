from tecton import realtime_feature_view, RequestSource, Attribute
from tecton.types import String, Int64, Field
from Personalization.features.user_last_game_played import user_last_game_played

request = RequestSource(schema=[Field('TIMESTAMP', String)])

@realtime_feature_view(
    description='''Number of minutes elapsed between current time (coming from the request payload) 
    and the time of the user's last game (fetched from a Streaming Feature View).''',
    sources=[request, user_last_game_played],
    mode='python',
    features=[
        Attribute('user_time_since_last_game', Int64)
    ]
)
def user_time_since_install(request, user_last_game_played):
    from datetime import datetime, date
    import pandas
    
    request_datetime = pandas.to_datetime(request['TIMESTAMP']).replace(tzinfo=None)
    last_game_datetime = pandas.to_datetime(user_last_game_played['TIME_GAME_PLAYED'])
    td = request_datetime - last_game_datetime 
    
    return {'user_time_since_last_game': td.minute}
