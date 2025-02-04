from tecton import batch_feature_view, Aggregate
from datetime import datetime, timedelta
from tecton.types import Field, Int64
from Search.data_sources import search_user_interactions
from Search.entities import search_product

@batch_feature_view(
    description='''Product performance metrics to capture how popular a candidate product is 
    based on last year visit, add to cart, purchase totals''',
    sources=[search_user_interactions],
    entities=[search_product],
    mode='spark_sql',
    aggregation_interval=timedelta(days=1),
    timestamp_field='timestamp',
    features=[Aggregate(input_column=Field('purchase', Int64), function='sum', time_window=timedelta(days=365)),
                Aggregate(input_column=Field('visit', Int64), function='sum', time_window=timedelta(days=365)),
                Aggregate(input_column=Field('add_to_cart', Int64), function='sum', time_window=timedelta(days=365))],
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

