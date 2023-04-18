from tecton import Entity

gaming_user = Entity(
    name='gaming_user',
    join_keys=['USER_ID'],
    description='A user of the mobile game',
    owner='vince@tecton.ai',
    tags={'release': 'production'}
)

gaming_product = Entity(
    name='gaming_product',
    join_keys=['PRODUCT_NAME'],
    description='A product available in the mobile game',
    owner='vince@tecton.ai',
    tags={'release': 'production'}
)