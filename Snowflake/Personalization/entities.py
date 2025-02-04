from tecton import Entity
from tecton.types import String, Field

gaming_user = Entity(
    name='gaming_user',
    join_keys=[Field('USER_ID', String)],
    description='A user of the mobile game',
    owner='vince@tecton.ai',
    tags={'release': 'production'}
)

gaming_product = Entity(
    name='gaming_product',
    join_keys=[Field('PRODUCT_NAME', String)],
    description='A product available in the mobile game',
    owner='vince@tecton.ai',
    tags={'release': 'production'}
)