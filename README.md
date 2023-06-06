# Tecton Feature Examples

This repository contains curated examples of Tecton-based feature definitions. Use these examples in your own projects by replacing the sample data sources with your own data.


## [1. Fraud Detection](Spark/Fraud)

Build powerful batch, streaming, and real-time features to capture fraudulent patterns and detect fraud in real-time. 

### [Data sources](Spark/Fraud/data_sources.py)

The data used as input for these features is transactional data. The features are built from historical transactions data (e.g files in s3, refreshed daily), transaction events streamed through Kinesis, as well as real-time data coming directly from the current transaction being processed.

A sample of the batch data is publicly available in an s3 bucket 
 `s3://tecton.ai.public/tutorials/fraud_demo/transactions/`


**Data preview**

Below is a preview of the data, these are the required attributes that you will need to map your input data to in order to reuse these features:

| user_id           | transaction_id                   | category      |   amt |   is_fraud | merchant                           |   merch_lat |   merch_long | timestamp           |
|------------------|---------------------------------|--------------|------|-----------|-----------------------------------|------------|-------------|--------------------|
| user_884240387242 | 3eb88afb219c9a10f5130d0b89a13451 | gas_transport | 68.23 |          0 | fraud_Kutch, Hermiston and Farrell |       42.71 |     -78.3386 | 2023-06-20 102641 |


### [Features](Spark/features/)

#### [Standard deviation of user spend (Batch)](Spark/Fraud/features/user_dollar_spend_aggregates.py)

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

#### [User-Merchant transaction count (Streaming)](Spark/Fraud/features/user_merchant_transaction_counts.py)

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

#### [Distance between current and last transaction (On-demand + Streaming)](Spark/Fraud/features/user_distance_previous_location.py)

Leverage geolocation features to detect if a user's card is used in an unusual, abnormally far location compared to the last transaction on the card. Combine and compare real-time and pre-computed features to capture new fraud patterns. This feature computes the distance between the current transaction location and the user's previous transaction location using Python.

```python
@on_demand_feature_view(
    description='''Distance between current and previous transaction location for a user in km,
                using Haversine formula''',
    sources=[RequestSource(schema=request_schema), user_last_transaction_location],
    mode='python',
    schema=[Field('distance_previous_transaction', Float64)]
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

#### [Z-score of current transaction amount (On-demand + Batch)](Spark/Fraud/features/user_transaction_zscore.py)

Z-score is a statistical measure commonly used in Fraud detection because it is a simple yet very effective way to identify outliers in a timeseries. In the context of Fraud, z-score lets us identify by how many standard deviations the current transaction amount deviates from the mean transaction amount for a user. This feature is an example of combining real-time data (current amount) with batch pre-computed features (mean, stddev) and applying pandas-based logic.

```python
@on_demand_feature_view(
    description='''Z-score of the current transaction amount for a user based on 60 days mean and standard deviation''',
    sources=[transaction_request, user_dollar_spend],
    mode='pandas',
    schema=output_schema
)
def zscore_current_transaction(transaction_request, user_dollar_spend):
    import pandas
    
    user_dollar_spend['amount'] = transaction_request['amt']
    user_dollar_spend['zscore_transaction_amount'] = (user_dollar_spend['amount'] - user_dollar_spend['amt_mean_60d_1d']) / user_dollar_spend['amt_stddev_samp_60d_1d']

    return user_dollar_spend[['zscore_transaction_amount']]
```

## [2. Recommender System](Spark/Recommender_system)

Build a state of the art online book recommender system and improve customer engagement by combining historical product, user and user-product interaction data with session-based, near real-time interaction data coming from streaming events. 

### [Data sources](Spark/Recommender_system/data_sources.py)

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


### [Features](Spark/Recommender_system/features/)

#### [Book aggregate ratings features (Batch)](Spark/Recommender_system/features/book_aggregate_ratings.py)

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

#### [User recent ratings (Streaming)](Spark/Recommender_system/features/user_recent_ratings.py)

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

#### [User ratings for similar books (On-demand + Batch + Streaming)](Spark/Recommender_system/features/user_ratings_similar_to_candidate_book.py)

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

## [3. Credit Risk Scoring](Snowflake/Credit_risk/)

Make real-time decisions on new credit card or loan applications by predicting an applicant's probability of being past due on their credit/loan payment. 

### [Data sources](https://plaid.com/docs/api/products/transactions/#transactionsget)

In most real-time credit decisioning use cases, there is little to no prior information about the applicant. Most features will be generated from application form data or 3rd party APIs like Plaid, Socure or Experian. 

For this example we will leverage information about the current application as well as the transaction history of the applicant's bank accounts, retrieved from the [Plaid API transactions endpoint](https://plaid.com/docs/api/). A sample API response payload is available in [Plaid's documentation](https://plaid.com/docs/api/products/transactions/#transactionsget)

### [Features](Snowflake/Credit_risk/features/)

#### [Applicant total spend accross accounts in 30,60,90 days from Plaid API (On-demand)](Snowflake/Credit_risk/features/plaid_features.py)

Compute features in real-time from a JSON API Payload passed at inference time using Pandas in Python. Tecton guarantees the same code will be executed offline when generating training data to limit training/serving skew. 

```python
@on_demand_feature_view(
    description='''Total expenses accross a user's bank accounts, computed in real-time from the Plaid Transactions API payload''',
    sources=[RequestSource(schema=[Field('TIMESTAMP', String), Field('PLAID_PAYLOAD', String)])],
    mode='python',
    schema=[Field('user_total_spend_last_%s_days'%i, Int64)  for i in range(30,120,30)]
)
def user_plaid_features(request):
    from datetime import datetime, timedelta
    import json
    import pandas
    
    df = pandas.DataFrame(json.loads(request['PLAID_PAYLOAD']).get('transactions'))
    df['date'] = pandas.to_datetime(df['date'])
    output_dict = {}

    for i in range(30,120,30):
        df_sub = df[df['date']>= pandas.to_datetime(request['TIMESTAMP'])-timedelta(days=i)]
        user_total_spend = int(df_sub['amount'].sum())
        output_dict['user_total_spend_last_%s_days'%i] = user_total_spend
    
    return output_dict
```

## [4. Personalization](Snowflake/Personalization/)

Personalize in-product user experience using your users' historical and in-session behavior to increase engagement and conversions! For a given context, user and set of offers/content to display, our model will rank the offers/content based on the probability of a user interacting with the offer. In this example, we are a mobile gaming company that is trying to personalize in-app offers in real-time based on a user's behavior. 

### [Data sources](Snowflake/Personalization/data_sources.py)

The input datasets were synthesized from an openly available Kaggle dataset containing in-app purchase product information for a battle royale mobile game.

**Data Preview**

**User purchases**

Snowflake table containing in-app purchase history for all users. Below is a preview of the data, these are the required attributes that you will need to map your input data to in order to reuse these features:

| USER_ID      | PRODUCT_NAME   | PRODUCT_CATEGORY    |   QUANTITY | TIMESTAMP           |
|-------------|---------------|--------------------|-----------|--------------------|
| user_1009748 | Bandit         | Troops and Defenses |          3 | 2023-03-27 081758 |

**In-game events stream**

Our application emits streaming events based on users interacting with the application (logging-in, game played, purchase etc.). For this example, we are specifically interested in the game played events which we are pushing to Tecton using Tecton's [Stream Ingest API](https://docs.tecton.ai/using-the-ingestion-api).

The structure of the event payload is the following:
{
    'USER_ID': 'user_1009748',
    'EVENT_TS': '2023-03-27T08:17:58+00:00',
    'TIME_GAME_PLAYED': '2023-03-27 08:17:57',
    'GAME_ID': 'multiplayer_988398',
}

### [Features](Snowflake/Personalization/features)

#### [Categorical aggregates of user purchases (Batch)](Snowflake/Personalization/features/user_categorical_aggregations.py)

Aggregate a user's historical interactions with all product categories within a single Feature View to power your ranking model. This Feature View leverages custom aggregations and incremental backfills to return a single dict-like object with the aggregation metric per category within a 30 day window. It is computed in batch and refreshed everyday.

```python
@batch_feature_view(
    description='''Agrregate metrics for each product category in a user's 30 day purchase history. 
    This feature outputs a Snowflake object with the following structure: 
    {'category_1':'user total purchases in category_1', 'category_2': ...}'''
    entities=[gaming_user],
    sources=[gaming_transactions],
    mode='snowflake_sql',
    incremental_backfills=True,
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1)
    )
def user_categorical_aggregations(gaming_transactions, context=materialization_context()):
    return f'''
    SELECT
        USER_ID,
        TO_TIMESTAMP('{context.end_time}') - INTERVAL '1 MICROSECOND' AS TIMESTAMP,
        TO_CHAR(OBJECT_AGG(PRODUCT_CATEGORY, SUM(QUANTITY)::variant) OVER (PARTITION BY USER_ID)) AS USER_PURCHASES
    FROM {gaming_transactions}
        WHERE EVENT_TS <TO_TIMESTAMP('{context.end_time}') AND EVENT_TS >= TO_TIMESTAMP('{context.start_time}') - INTERVAL '30 DAYS'
    GROUP_BY USER_ID, PRODUCT_CATEGORY
    '''
```

#### [Time since last game played (On-demand + Stream Ingest API)](Snowflake/Personalization/features/user_time_since_last_game.py)

Measure the recency of your users' engagements with date difference features in On-demand feature views with Tecton. In this example, we are computing the difference between the current timestamp and a timestamp retrieved from a Streaming Feature view to account for the time elapsed since the user last played a game! 

```python
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

```

## [5. Dynamic Pricing](Spark/Dynamic_pricing/)

In this example, we are a ride-hailing company that is trying to dynamically price rides based on factors such as the duration of a ride, the number of available drivers, and the number of riders currently using the app.

### [Data sources](Spark/Dynamic_pricing/data_sources/)

There were several data sources based loosely on a public Kaggle [dataset](https://www.kaggle.com/datasets/ravi72munde/uber-lyft-cab-prices) containing historical data on Uber and Lyft rides.

**Data Preview**

**Completed Rides**

Hive table containing completed rides for all users. Below is a preview of the data:

| origin_zipcode | duration | TIMESTAMP           |
|----------------|----------|---------------------|
| 94102          | 1273     | 2023-03-27 081758   |

**Driver locations stream**

The driver application emits streaming events that indicate where drivers are located.

The structure of the event payload is the following:
{
    'driver_id': 'driver_826192',
    'timestamp': '2023-03-27T08:17:58+00:00',
    'zipcode': '94102',
}

**Ride requests stream**

The rider app emits streaming events that indicate when a new ride is requested.

The structure of the event payload is the following:
{
    'user_id': 'user_718092',
    'origin_zipcode': '94102',
    'destination_zipcode': '94103',
    'request_id': 'request_917109292',
    'timestamp': '2023-03-27T08:17:58+00:00',
}

### [Features](Spark/Dynamic_pricing/features)

#### [Ride Requests (Streaming)](Spark/Dynamic_pricing/features/ride_request_count.py)

Counts the number of ride requests over the past 30 minutes, grouped by the origin zipcode. It is updated every 5 minutes.

Aggregate a user's historical interactions with all product categories within a single Feature View to power your ranking model. This Feature View leverages custom aggregations and incremental backfills to return a single dict-like object with the aggregation metric per category within a 30 day window. It is computed in batch and refreshed everyday.

```python
@stream_feature_view(
    description='''Number of ride requests from the given origin zipcode over the last 30 minutes, updated every 5 minutes.''',
    source=ride_requests_stream,
    entities=[origin_zipcode],
    mode='pyspark',
    aggregation_interval=timedelta(minutes=5),
    aggregations=[
        Aggregation(column='request_id', function='count', time_window=timedelta(minutes=30))
    ],
    batch_schedule=timedelta(days=1),
)
def ride_request_count(ride_requests_stream):
    from pyspark.sql import functions as f
    return ride_requests_stream.select('origin_zipcode', 'timestamp', 'request_id')
```

#### [Driver Availability (Streaming)](Spark/Dynamic_pricing/features/available_drivers_count.py)

Counts the number of drivers available over the past 30 minutes, grouped by zipcode. It is updated every 5 minutes.

```python
@stream_feature_view(
    description='''Number of available drivers in the given zipcode over the last 30 minutes, updated every 5 minutes.''',
    source=driver_locations_stream,
    entities=[zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=5),
    aggregations=[
        Aggregation(column='driver_id', function=approx_count_distinct(), time_window=timedelta(minutes=30))
    ],
    batch_schedule=timedelta(days=1),
)
def available_drivers_count(driver_locations_stream):
    return f"""
        SELECT zipcode, timestamp, driver_id
        FROM {driver_locations_stream}
        """
```

#### [Ride Duration Statistics (Batch)](Spark/Dynamic_pricing/features/ride_durations.py)

Computes the mean and standard deviation of ride durations from the given zipcode. It is updated every day.

```python
@batch_feature_view(
    description='''Standard deviation of ride durations from the given zipcode over a series of time windows, updated daily.''',
    sources=[FilteredSource(completed_rides_batch)],
    entities=[origin_zipcode],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1), # This feature will be updated daily
    aggregations=[
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=10)),
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=30)),
        Aggregation(column='duration', function='stddev_samp', time_window=timedelta(days=60)),
        Aggregation(column='duration', function='mean', time_window=timedelta(days=60))
    ]
)
def ride_durations(completed_rides_batch):
    return f'''
        SELECT
            origin_zipcode,
            duration,
            timestamp
        FROM
            {completed_rides_batch}
        '''
```

#### [Ride Duration Z-Score (Batch)](Spark/Dynamic_pricing/features/ride_duration_zscore.py)

Computes the z-score of the requested ride duration based on the mean and standard deviation over the past 60 days.

```python
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
```

## [6. Search and Ranking](Spark/Search/)

In this example, we are focusing on improving shopper's experience on an e-commerce website by delivering highly relevant search results based on a user's input query, candidate product attributes and the user's past behavior and interactions with products. This example specifically focuses on the ranking part of the search engine, it assumes the candidate generation process has already happened and aims at ranking the candidates based on the user's likeliness to purchase. 

### [Data sources](Spark/Search/data_sources.py)

There were several data sources inspired from a public Kaggle [dataset](https://www.kaggle.com/c/home-depot-product-search-relevance) containing search relevance data for The Home Depot. Some of the datasets were synthesized. 

**Data Preview**

**Product title**

Hive table containing a mapping of the product_uid and product title:

| origin_zipcode | product_title                     |
|----------------|-----------------------------------|
| 100001         | Simpson Strong-Tie 12-Gauge Angle | 

**Product attributes**

Hive table containing a variety of product attributes (e.g Brand, Color, Material etc) for candidate products


| product_uid    | name     | value               |
|----------------|----------|---------------------|
| 100001	     | Color    | White               |


**Search interaction stream**

The website emits streaming events when a user visits, adds to cart or purchases a product.

The structure of the event payload is the following:
{
    'user_id': 'user_12378',
    'timestamp': '2023-03-27T08:17:58+00:00',
    'product_uid': '100001',
    'event': 'add_to_cart'
}

This streaming data source as a batch equivalent that contains a historical log of these events in a Hive table.



### [Features](Spark/Search/features)

#### [Search query and candidate product title similarity (On-demand + Batch)](Spark/Search/features/query_product_similarity.py)

Compute the [Jaccard similarity](https://en.wikipedia.org/wiki/Jaccard_index) between the user's input query and the candidate product's title/description in real-time. 

```python
@on_demand_feature_view(
  description='''Jaccard similarity between the tokenized input query and the product title, computed in real-time''',  
  sources=[search_query, product_title],
  schema=output_schema,
  mode='python'
)
def search_query_product_similarity(search_query, product_attributes, product_title):
  def jaccard(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection) / union
    
  #Normalizing and tokenizing search query
  search_term = search_query.get('search_term')
  search_term = search_term.lower()
  tokenized_query = search_term.split(' ')

  #Normalizing and tokenizing product title
  product_title = product_title.get('product_title')
  product_title = product_title.lower()
  product_title_tokenized = product_title.split(' ')
  
  #Compute Jaccard similarity
  jaccard_similarity = jaccard(tokenized_query, product_title_tokenized)
  
  return {
    'jaccard_similarity_query_token_title_token': jaccard_similarity
    }
```

#### [Product popularity metrics (Batch)](Spark/Search/features/product_popularity.py)

Capture how popular a candidate product is by computing performance metrics over a rolling window using Tecton's Aggregation Framework. This feature is computed in batch and refreshed daily.

```python
@batch_feature_view(
    description='''Product performance metrics to capture how popular a candidate product is 
    based on last year visit, add to cart, purchase totals''',
    sources=[search_user_interactions],
    entities=[product],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1)
    aggregations=[Aggregation(column='purchase', function='sum', time_window=timedelta(days=365)),
                Aggregation(column='visit', function='sum', time_window=timedelta(days=365)),
                Aggregation(column='add_to_cart', function='sum', time_window=timedelta(days=365))],
    batch_schedule=timedelta(days=1)
)
def product_yearly_totals(search_user_interactions):
  return f"""
    select 
        product_uid,
        timestamp,
        case when event='add_to_cart' then 1 else 0 end as add_to_cart,
        case when event='visit' then 1 else 0 end as visit,
        case when event='purchase' then 1 else 0 end as purchase
    from {search_user_interactions}
  """
```

#### [Candidate product was viewed by user (On-demand + Streaming)](Spark/Search/features/user_viewed_product.py)

Determine whether the user making a search has seen the candidate product in the last hour. This feature is computed from a Streaming feature view that capture the user's last 10 distinct products viewed in the last hour and from an On-demand feature view that determines whether the candidate product is part of the last 10 products viewed.

```python
@stream_feature_view(
    description='''Last 10 products a user has viewed in the last hour, refreshed continuously from streaming events 
    to capture in-session user behavior''',
    source=search_interaction_stream,
    entities=[search_user],
    mode='spark_sql',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS,
    aggregations=[
        Aggregation(column='product_uid', function=last_distinct(10), time_window=timedelta(hours=1))
    ],
    batch_schedule=timedelta(days=1)
)
def user_products_viewed(search_interaction_stream):
  return f"""
  select 
    user_id,
    timestamp,
    product_uid
  from {search_interaction_stream}
  where event='visit'
  """


request_schema = [
                  Field('user_id', String),
                  Field('product_uid', String)
                  ]
search_query = RequestSource(schema=request_schema)

output_schema = [
  Field('user_viewed_product_in_last_10_pages', Bool)
]

@on_demand_feature_view(
  description='''This features verifies whether the current candidate product has been visited
   by the user in the last hour, it is computed in real-time and depends on a streaming feature view''',
  sources=[search_query, user_products_viewed],
  schema=output_schema,
  mode='python'
)
def user_viewed_product(search_query, user_products_viewed):
    product_id = search_query['product_uid']
    last_products_viewed = user_products_viewed['product_uid_last_distinct_10_1h_continuous']
    return {'user_viewed_product_in_last_10_pages': product_id in last_products_viewed}
```