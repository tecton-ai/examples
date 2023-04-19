from tecton import on_demand_feature_view, RequestSource
from tecton.types import String, Int64, Field
from Snowflake.Personalization.features import user_last_game_played

request = RequestSource(schema=[Field('TIMESTAMP', String)])

@on_demand_feature_view(
    description='''Number of minutes elapsed between current time (coming from the request payload) 
    and the time of the user's last game (fetched from a Streaming Feature View).'''
    sources=[request, user_last_game_played],
    mode='python',
    schema=[Field('user_time_since_last_game', Int64)]
)
def user_time_since_install(request, user_last_game_played):
    from datetime import datetime, date
    import pandas
    
    request_datetime = pandas.to_datetime(request['TIMESTAMP']).replace(tzinfo=None)
    last_game_datetime = pandas.to_datetime(user_last_game_played['TIME_GAME_PLAYED'])
    td = request_datetime - dob_datetime 
    
    return {'user_time_since_last_game': td.minute}