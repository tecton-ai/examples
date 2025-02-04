from tecton import RequestSource, realtime_feature_view, Attribute
from tecton.types import String, Int64, Field


@realtime_feature_view(
    description='''Total expenses accross a user's bank accounts, computed in real-time from the Plaid Transactions API payload''',
    sources=[RequestSource(schema=[Field('TIMESTAMP', String), Field('PLAID_PAYLOAD', String)])],
    mode='python',
    features=[Attribute('user_total_spend_last_%s_days'%i, Int64) for i in range(30,150,30)]
)
def user_plaid_features(request):
    from datetime import datetime, timedelta
    import json
    import pandas
    
    df = pandas.DataFrame(json.loads(request['PLAID_PAYLOAD']).get('transactions'))
    df['date'] = pandas.to_datetime(df['date'])
    output_dict = {}

    for i in range(30,150,30):
        df_sub = df[df['date']>= pandas.to_datetime(request['TIMESTAMP'])-timedelta(days=i)]
        user_total_spend = int(df_sub['amount'].sum())
        output_dict['user_total_spend_last_%s_days'%i] = user_total_spend
    
    return output_dict