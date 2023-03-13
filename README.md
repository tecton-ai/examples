# Tecton Feature Examples

This repository contains curated examples of Tecton-based feature definitions. Use these examples in your own projects by replacing the sample data sources with your own data.


## [1. Fraud Detection](Fraud)

Build powerful batch, streaming, and real-time features to capture fraudulent patterns and detect fraud in real-time. https://www.tecton.ai/solutions/

### [Data sources](Fraud/data_sources.py)

The data used as input for these features is transactional data. The features are built from historical transactions data (e.g files in s3, refreshed daily), transaction events streamed through Kinesis, as well as real-time data coming directly from the current transaction being processed.

A sample of the batch data is publicly available in an s3 bucket 
 `s3://tecton.ai.public/tutorials/fraud_demo/transactions/`


**Data preview**

Below is a preview of the data, these are the required attributes that you will need to map your input data to in order to reuse these features:

| user_id           | transaction_id                   | category      |   amt |   is_fraud | merchant                           |   merch_lat |   merch_long | timestamp           |
|------------------|---------------------------------|--------------|------|-----------|-----------------------------------|------------|-------------|--------------------|
| user_884240387242 | 3eb88afb219c9a10f5130d0b89a13451 | gas_transport | 68.23 |          0 | fraud_Kutch, Hermiston and Farrell |       42.71 |     -78.3386 | 2023-06-20 102641 |


### [Features](Fraud/features/)

#### [Standard deviation of user spend (Batch)](Fraud/features/user_dollar_spend_aggregates.py)

Measure how much a user's recent transactions deviate from their usual behavior to detect any suspicious activity on a credit card. This feature computes the standard deviation of a user's transaction amounts in the last 10, 30, and 60 days prior to the current transaction day. This feature is refreshed daily.

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

A common pattern for fraudsters is to put many small charges on a stolen credit card in a short amount of time. The only way to capture this pattern is to have near real-time feature freshness. This feature computes the number of transactions on a user's card at the same merchant in the last 30 minutes prior to the current transaction time. It is computed from streaming events data.

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
    feature_start_time=datetime(2022,5, 1)

)
def user_merchant_transactions_count(transactions_stream):
  from pyspark.sql import functions as f
  return transactions_stream.select('user_id','merchant','transaction_id','timestamp')
```

#### [Distance between current and last transaction (On-demand + Streaming)](Fraud/features/user_distance_previous_location.py)

Leverage geolocation features to detect if a user's card is used in an unusual, abnormally far location compared to the last transaction on the card. Combine and compare real-time and pre-computed features to capture new fraud patterns. This feature computes the distance between the current transaction location and the user's previous transaction location using Python.

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

## [2. Recommender System](Recommender_system)

Build a state of the art online book recommender system and improve customer engagement by combining historical product, user and user-product interaction data with session-based, near real-time interaction data coming from streaming events. 

### [Data sources](Recommender_system/data_sources.py)

The data used for these examples is adapted from publicly available data (https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset). While this particular examples aims at recommending relevant books to users, it is applicable and can very easily be adapted to other use cases/product types.

A sample of the batch data is publicly available in an s3 bucket
`s3://tecton-demo-data/apply-book-recsys/users.parquet`
`s3://tecton-demo-data/apply-book-recsys/books_v3.parquet`
`s3://tecton-demo-data/apply-book-recsys/ratings.parquet`


**Data Preview**

**Book metadata**

Column isbn is a book identifier column.

|       isbn | book_title                                                     | book_author                   |   year_of_publication | publisher        | summary                                                            | language   | category                 | created_at          |
|-----------|---------------------------------------------------------------|------------------------------|----------------------|-----------------|-------------------------------------------------------------------|-----------|-------------------------|--------------------|
| 0000913154 | The Way Things Work An Illustrated Encyclopedia of Technology | C. van Amerongen (translator) |                  1967 | Simon & Schuster | Scientific principles, inventions, and chemical, mechanical, and industrial ...  | en         | Technology & Engineering | 2022-03-13 000000 |

**Users metadata**

Column user_id is a the user identifier column.


|   user_id | location                  |   age | city     | state      | country   | signup_date                |
|----------|--------------------------|------|---------|-----------|----------|---------------------------|
|         2 | stockton, california, usa |    18 | stockton | california | usa       | 2021-09-12 061427.197000 |

**User-Book ratings**

Ratings assigned to books by users. user_id is the user identifier, isbn is the book identifier and rating is a rating ranging from 0-10 assigned by the user. This data source is both batch and streaming based in order to capture the latest rating events.

|   user_id |       isbn |   rating | rating_timestamp           |
|----------|-----------|---------|---------------------------|
|         2 | 0195153448 |        0 | 2022-08-04 160316.862000 |


### [Features](Recommender_system/features/)

#### [Book aggregate ratings features (Batch)](Recommender_system/features/book_aggregate_ratings.py)

Capture the popularity of a product using several statistical measures (mean, standard deviation, count). Easily compute batch window aggregate features using Tecton's aggregation framework.

```python
@batch_feature_view(
    description='''Book aggregate rating features over the past year and past 30 days.''',
    sources=[FilteredSource(ratings_batch)],
    entities=[book],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='rating', function='mean', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='mean', time_window=timedelta(days=30)),
        Aggregation(column='rating', function='stddev', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='stddev', time_window=timedelta(days=30)),
        Aggregation(column='rating', function='count', time_window=timedelta(days=365)),
        Aggregation(column='rating', function='count', time_window=timedelta(days=30)),
    ],
    feature_start_time=datetime(2022, 1, 1),  # Only plan on generating training data from the past year.

)
def book_aggregate_ratings(ratings):
    return f'''
        SELECT
            isbn,
            rating_timestamp,
            rating
        FROM
            {ratings}
        '''
```

#### [User recent ratings (Streaming)](Recommender_system/features/user_recent_ratings.py)

Retrieve a user's most recent distinct book ratings within a 365 days window, computed from streaming data.

```python
@stream_feature_view(
    description='''Ratings summaries of the user\'s most recent 200 book ratings.''',
    source=FilteredSource(ratings_with_book_metadata_stream),
    entities=[user],
    mode='pyspark',
    feature_start_time=datetime(2022, 1, 1),  # Only plan on generating training data from the past year.
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='rating_summary', function=last_distinct(200), time_window=timedelta(days=365), name='last_200_ratings'),
    ]
)
def user_recent_ratings(ratings_with_book_metadata):
    from pyspark.sql.functions import struct, to_json, col

    df = ratings_with_book_metadata.select(
        col("user_id"),
        col("rating_timestamp"),
        to_json(struct('rating', 'book_author', 'category')).alias("rating_summary")
    )

    return df

```

#### [User ratings for similar books (On-demand + Batch + Streaming)](Recommender_system/features/user_ratings_similar_to_candidate_book.py)

Blend real-time data and pre-computed batch and streaming features in order to capture the current users' past interactions and ratings with books from the same author and category as the current candidate book. This feature leverages Python to combine batch and streaming features and apply additional logic.


```python
output_schema = [
    Field('avg_rating_for_candidate_book_category', Float64),
    Field('num_rating_for_candidate_book_category', Int64),
    Field('avg_rating_for_candidate_book_author', Float64),
    Field('num_rating_for_candidate_book_author', Int64),
]

@on_demand_feature_view(
    sources=[book_metadata_features, user_recent_ratings],
    mode='python',
    schema=output_schema,
    description="Aggregate rating metrics for the current user for the candidate book's category and author."
)
def user_ratings_similar_to_candidate_book(book_metadata_features, user_recent_ratings):
    import json

    user_ratings_json = user_recent_ratings["last_200_ratings"]
    user_ratings = [json.loads(user_rating) for user_rating in user_ratings_json]

    user_ratings_same_category = []
    user_ratings_same_author = []
    candidate_category = book_metadata_features["category"]
    candidate_author = book_metadata_features["book_author"]
    for rating in user_ratings:
        if candidate_category and "category" in rating and rating["category"] == candidate_category:
            user_ratings_same_category.append(rating["rating"])
        if candidate_author and "book_author" in rating and rating["book_author"] == candidate_author:
            user_ratings_same_author.append(rating["rating"])

    output = {
        "avg_rating_for_candidate_book_category": None,
        "num_rating_for_candidate_book_category": len(user_ratings_same_category),
        "avg_rating_for_candidate_book_author": None,
        "num_rating_for_candidate_book_author": len(user_ratings_same_author),
    }

    if output["num_rating_for_candidate_book_category"] > 0:
        output["avg_rating_for_candidate_book_category"] = (
                sum(user_ratings_same_category) / output["num_rating_for_candidate_book_category"])

    if output["num_rating_for_candidate_book_author"] > 0:
        output["avg_rating_for_candidate_book_author"] = (
                sum(user_ratings_same_author) / output["num_rating_for_candidate_book_author"])

    return output
```