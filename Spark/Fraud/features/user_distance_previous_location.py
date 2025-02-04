from Fraud.features.user_last_transaction_location import user_last_transaction_location
from tecton.types import String, Float64, Field
from tecton import realtime_feature_view, RequestSource, Attribute

request_schema = [Field('user_id', String),
                Field('merch_lat', Float64),
                Field('merch_long', Float64)
]

@realtime_feature_view(
    description='''Distance between current and previous transaction location for a user in km,
                using Haversine formula''',
    sources=[RequestSource(schema=request_schema), user_last_transaction_location],
    mode='python',
    features=[Attribute('distance_previous_transaction', Float64)]
)
def distance_previous_transaction(transaction_request, user_last_transaction_location):
    from math import sin, cos, sqrt, atan2, radians

    # Approximate radius of earth in km
    R = 6373.0

    lat1 = radians(transaction_request['merch_lat'])
    lon1 = radians(transaction_request['merch_long'])
    lat2 = radians(user_last_transaction_location['user_last_transaction_lat'])
    lon2 = radians(user_last_transaction_location['user_last_transaction_long'])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    
    return {
        "distance_previous_transaction": distance
    }