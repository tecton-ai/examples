from tecton import Entity


origin_zipcode = Entity(
    name='origin_zipcode',
    join_keys=['origin_zipcode'],
    description='Zipcode of the origin of a ride',
    owner='felix@tecton.ai',
    tags={'release': 'production'}
)

zipcode = Entity(
    name='zipcode',
    join_keys=['zipcode'],
    description='Current zipcode of a driver or user',
    owner='felix@tecton.ai',
    tags={'release': 'production'}
)