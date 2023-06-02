from tecton import Entity

search_product = Entity(
  name='search_product',
  join_keys=['product_uid']
)

search_user = Entity(
  name='search_user',
  join_keys=['user_id']
)