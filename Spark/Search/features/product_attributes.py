from tecton import batch_feature_view, materialization_context
from datetime import datetime, timedelta
from Search.entities import search_product
from Search.data_sources import product_attributes_src, product_title_source

@batch_feature_view(
    description='''product attributes from the product attributes table, updated daily''',
    entities=[search_product],
    sources=[product_attributes_src],
    batch_schedule=timedelta(days=1),
    incremental_backfills=True,
    mode='spark_sql'
)
def product_attributes(product_attributes_src, context=materialization_context()):
  return f"""
    select *,
    TO_TIMESTAMP('{context.end_time}') - INTERVAL 1 MICROSECOND as TIMESTAMP 
    from {product_attributes_src}
        pivot (
            MIN(value) AS v
            for name in ('MFG Brand Name', 'Color Family', 'Material', 'Color/Finish', 'Color')
        )
        where product_uid is not null
  """


@batch_feature_view(
    description='''product title from the product title source table, updated daily with new products''',
    entities=[search_product],
    sources=[product_title_source],
    mode='spark_sql'
    batch_schedule=timedelta(days=1),
    incremental_backfills=True,
)
def product_title(product_title_source, context=materialization_context()):
  return f"""
    select 
        distinct string(product_uid), 
        product_title,
        TO_TIMESTAMP('{context.end_time}') - INTERVAL 1 MICROSECOND as TIMESTAMP 
    from {product_title_source}
  """