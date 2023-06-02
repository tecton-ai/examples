from tecton import batch_feature_view, Aggregation
from datetime import datetime, timedelta

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

