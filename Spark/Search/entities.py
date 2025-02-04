from tecton import Entity
from tecton.types import Field, String

search_product = Entity(
  name='search_product',
  join_keys=[Field('product_uid', String)]
)

search_user = Entity(
  name='search_user',
  join_keys=[Field('user_id', String)]
)