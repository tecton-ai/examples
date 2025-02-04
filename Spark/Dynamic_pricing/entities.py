from tecton import Entity
from tecton.types import String, Field


origin_zipcode = Entity(
    name='origin_zipcode',
    join_keys=[Field('origin_zipcode', String)],
    description='Zipcode of the origin of a ride',
    owner='felix@tecton.ai',
    tags={'release': 'production'}
)

zipcode = Entity(
    name='zipcode',
    join_keys=[Field('zipcode', String)],
    description='Current zipcode of a driver or user',
    owner='felix@tecton.ai',
    tags={'release': 'production'}
)