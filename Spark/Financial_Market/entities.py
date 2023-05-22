from tecton import Entity

stock = Entity(
    name='stock_ticker',
    join_keys=['SYMBOL'],
    description='A stock ticker',
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)