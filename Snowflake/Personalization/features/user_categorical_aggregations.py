from Personalization.entities import gaming_user
from Personalization.data_sources import gaming_transactions_batch
from tecton import batch_feature_view, materialization_context, Attribute
from tecton.types import String
from datetime import timedelta

@batch_feature_view(
    description='''Aggregate metrics for each product category in a user's 30 day purchase history. 
    This feature outputs a Snowflake object with the following structure: 
    {'category_1':'user total purchases in category_1', 'category_2': ...}''',
    entities=[gaming_user],
    sources=[gaming_transactions_batch],
    mode='snowflake_sql',
    incremental_backfills=True,
    ttl=timedelta(days=30),
    batch_schedule=timedelta(days=1),
    timestamp_field='TIMESTAMP',
    features=[
        Attribute('USER_PURCHASES', String)
    ]
)
def user_categorical_aggregations(gaming_transactions, context=materialization_context()):
    return f'''
    SELECT
        USER_ID,
        TO_TIMESTAMP('{context.end_time}') - INTERVAL '1 MICROSECOND' AS TIMESTAMP,
        TO_CHAR(OBJECT_AGG(PRODUCT_CATEGORY, SUM(QUANTITY)::variant) OVER (PARTITION BY USER_ID)) AS USER_PURCHASES
    FROM {gaming_transactions}
        WHERE EVENT_TS <TO_TIMESTAMP('{context.end_time}') AND EVENT_TS >= TO_TIMESTAMP('{context.start_time}') - INTERVAL '30 DAYS'
    GROUP BY USER_ID, PRODUCT_CATEGORY
    '''
