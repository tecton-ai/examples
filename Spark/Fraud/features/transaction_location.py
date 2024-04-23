from tecton import on_demand_feature_view, RequestSource
from tecton.types import Field, Float64, String

request_schema = [Field("merch_lat", Float64), Field("merch_long", Float64)]
transaction_request = RequestSource(schema=request_schema)
output_schema = [Field("city", String),Field("country", String)]


@on_demand_feature_view(
    sources=[transaction_request],
    name="geocoded_address",
    mode="python",
    schema=output_schema,
    environments=["tecton-python-extended:0.5"],
    owner="vince@tecton.ai",
    description="""City and Country of the current transaction 
    calculated from geocoding the merchant latitute and longitude through the OpenStreetMap API
    """
)
def geocoded_address(transaction_request):
  import requests, json
  headers = {
    'User-Agent': 'My User Agent 1.0',
    'From': 'vince@tecton.ai'  # This is another valid field
  }
  lat = transaction_request.get('merch_lat')
  lon = transaction_request.get('merch_long')
  url = 'https://nominatim.openstreetmap.org/reverse'
  params={
    'format':'jsonv2',
    'lon':str(lon),
    'lat':str(lat)
  }
  response = requests.get(url, params=params, headers=headers)
  if response.status_code==200:
    r = json.loads(response.text)
    address=r.get('address',{})
    return {
      'country':address.get('country'), 
      'city':address.get('city')
      }
  else:
    return {'country':None}