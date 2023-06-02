from Spark.Dynamic_pricing.features.ride_durations import ride_durations
from tecton.types import String, Timestamp, Float64, Field, Int64
from tecton import on_demand_feature_view, RequestSource

request_schema = [Field('origin_zipcode', String), Field('duration', Float64)]
ride_request = RequestSource(schema=request_schema)
output_schema = [Field('zscore_ride_duration', Float64)]

@on_demand_feature_view(
    description='''Z-score of the requested ride duration based on 60 days mean and standard deviation''',
    sources=[ride_request, ride_durations],
    mode='pandas',
    schema=output_schema
)
def ride_duration_zscore(transaction_request, ride_durations):
    import pandas
    
    ride_durations['duration'] = transaction_request['duration']
    ride_durations['zscore_duration'] = (ride_durations['duration'] - ride_durations['duration_mean_60d_1d']) / ride_durations['duration_stddev_samp_60d_1d']

    return ride_durations[['zscore_duration']]
