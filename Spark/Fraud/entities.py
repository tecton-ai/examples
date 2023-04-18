from tecton import Entity


user = Entity(
    name='fraud_user',
    join_keys=['user_id'],
    description='A user of the platform',
    owner='kevin@tecton.ai',
    tags={'release': 'production'}
)

merchant = Entity(
    name='merchant',
    join_keys=['merchant'],
    description='A merchant',
    owner='mike@tecton.ai',
    tags={'release': 'production'}
)