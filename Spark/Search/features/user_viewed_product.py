from tecton import stream_feature_view, StreamProcessingMode, Aggregate
from tecton.aggregation_functions import last_distinct
from datetime import datetime, timedelta
from Search.entities import search_user
from Search.data_sources import search_interaction_stream
from tecton.types import Field, String

@stream_feature_view(
    description='''Last 10 products a user has viewed in the last hour, refreshed continuously from streaming events 
    to capture in-session user behavior''',
    source=search_interaction_stream,
    entities=[search_user],
    mode='spark_sql',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS,
    features=[
        Aggregate(input_column=Field('product_uid', String), function=last_distinct(10), time_window=timedelta(hours=1))
    ],
    batch_schedule=timedelta(days=1),
    timestamp_field='timestamp'
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
from tecton import RequestSource, Attribute, realtime_feature_view
from tecton.types import Field, String, Bool

request_schema = [
                  Field('user_id', String),
                  Field('product_uid', String)
                  ]
search_query = RequestSource(schema=request_schema)

@realtime_feature_view(
  description='''This features verifies whether the current candidate product has been visited
   by the user in the last hour, it is computed in real-time and depends on a streaming feature view''',
  sources=[search_query, user_products_viewed],
  features=[
    Attribute('user_viewed_product_in_last_10_pages', Bool)
  ],
  mode='python'
)
def user_viewed_product(search_query, user_products_viewed):
    product_id = search_query['product_uid']
    last_products_viewed = user_products_viewed['product_uid_last_distinct_10_1h_continuous']
    return {'user_viewed_product_in_last_10_pages': product_id in last_products_viewed}