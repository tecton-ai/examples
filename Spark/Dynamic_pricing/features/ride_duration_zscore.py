from Dynamic_pricing.features.ride_durations import ride_durations
from tecton.types import String, Timestamp, Float64, Field, Int64
from tecton import realtime_feature_view, RequestSource, Attribute

request_schema = [Field('origin_zipcode', String), Field('duration', Float64)]
ride_request = RequestSource(schema=request_schema)

@realtime_feature_view(
    description='''Z-score of the requested ride duration based on 60 days mean and standard deviation''',
    sources=[ride_request, ride_durations],
    mode='pandas',
    features=[
        Attribute('zscore_ride_duration', Float64)
    ]
)
def ride_duration_zscore(transaction_request, ride_durations):
    import pandas
    
    ride_durations['duration'] = transaction_request['duration']
    ride_durations['zscore_duration'] = (ride_durations['duration'] - ride_durations['duration_mean_60d_1d']) / ride_durations['duration_stddev_samp_60d_1d']

    return ride_durations[['zscore_duration']]
