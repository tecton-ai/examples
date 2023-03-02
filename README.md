# Tecton Feature Examples

This repository contains curated examples of features using Tecton. Use this in your own projects by switching the sample data sources with your own data. 


## [1. Fraud Detection](Fraud)

Build powerful batch, streaming and real-time features to use in fraud detection models. https://www.tecton.ai/solutions/

### [Data sources](Fraud/data_sources.py)

The data used as input for these features is transactional data. The features are built from historical transactions data (e.g files in s3, refreshed daily), transaction events streamed through Kinesis as well as real-time data coming directly from the current transaction being processed.

A sample of the batch data is publicly available in an s3 bucket `s3://tecton.ai.public/tutorials/fraud_demo/transactions/`

**Data preview**

| user_id           | transaction_id                   | category      |   amt |   is_fraud | merchant                           |   merch_lat |   merch_long | timestamp           |
|------------------|---------------------------------|--------------|------|-----------|-----------------------------------|------------|-------------|--------------------|
| user_884240387242 | 3eb88afb219c9a10f5130d0b89a13451 | gas_transport | 68.23 |          0 | fraud_Kutch, Hermiston and Farrell |       42.71 |     -78.3386 | 2023-06-20 102641 |


### [Features](Fraud/features/)

#### [Standard deviation of user spend (Batch)](Fraud/features/user_dollar_spend_aggregates.py)

Measure how much a user's recent transactions deviate from their usual behavior to detect any suspicious activity on a credit card. This feature computes the standard deviation of a user's transaction amounts in the last 10, 30 and 60 days prior to the current transaction day, it is refreshed daily. 

```python
@batch_feature_view(
    description='''Standard deviation of a user's transaction amounts 
                    over a series of time windows, updated daily.''',
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    aggregations=[
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=10)),
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=30)),
        Aggregation(column='amt', function='stddev_samp', time_window=timedelta(days=60))
    ],
    feature_start_time=datetime(2022, 5, 1)
)
def user_dollar_spend(transactions):
    return f'''
        SELECT
            user_id,
            amt,
            timestamp
        FROM
            {transactions}
        '''
```

#### [User-Merchant transaction count (Streaming)](Fraud/features/user_merchant_transaction_counts.py)

A common pattern for fraudsters is to put many small charges on a credit card in a short amount of time after stealing its information, the only way to capture this pattern is to have near real-time feature freshness. This feature computes the number of transactions on a user's card at the same merchant in the last 30 minutes prior to the current transaction time. It is computed from streaming events data.

```python
@stream_feature_view(
    description='''Count number of transactions for this user at this same merchant in the last 30 minutes, updated every 5 minutes''',
    source=transactions_stream,
    entities=[user, merchant],
    mode='pyspark',
    aggregation_interval=timedelta(minutes=5),
    aggregations=[
        Aggregation(column='transaction_id', function='count', time_window=timedelta(minutes=30))
    ],
    feature_start_time=datetime(2022,5, 1),
    batch_schedule=timedelta(days=1)

)
def user_merchant_transactions_count(transactions_stream):
  from pyspark.sql import functions as f
  return transactions_stream.select('user_id','merchant','transaction_id','timestamp')
```

#### [Distance between current and last transaction (On-demand + Streaming)](Fraud/features/user_distance_previous_location.py)

Leverage geolocation features to detect if a user's card is used in an unusual, abnormally far location compared to the last transaction on the card. Combine and compare real-time and pre-computed features to capture new fraud patterns! 
This feature computes the distance between the current transaction location and the user's previous transaction location using Python. 

```python
@on_demand_feature_view(
    description='''Distance between current and previous transaction location for a user in km,
                using Haversine formula''',
    sources=[RequestSource(schema=request_schema), user_last_transaction_location],
    mode='python',
    schema=[Field('distance_previous_transaction', Float64)],
    
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

```