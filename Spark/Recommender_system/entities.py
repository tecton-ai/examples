from tecton import Entity
from tecton.types import Field, String

user = Entity(
    name='user',
    join_keys=[Field('user_id', String)],
    owner='jake@tecton.ai',
    tags={'release': 'production'}
)

book = Entity(
    name='book',
    join_keys=[Field('isbn', String)],
    owner='jake@tecton.ai',
    tags={'release': 'production'}
)