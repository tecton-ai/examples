from tecton import Entity
from tecton.types import Field, String

stock = Entity(
    name='stock_ticker',
    join_keys=[Field('SYMBOL', String)],
    description='A stock ticker',
    tags={'release': 'production'}
)