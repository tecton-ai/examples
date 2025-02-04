import tecton
from tecton import RequestSource, Attribute, realtime_feature_view
from tecton.types import String, Float64, Field
from Search.features.product_attributes import product_title

request_schema = [
                  Field('search_term', String),
                  Field('product_uid', String)
                  ]
search_query = RequestSource(schema=request_schema)


@realtime_feature_view(
  description='''Jaccard similarity between the tokenized input query and the product title, computed in real-time''',  
  sources=[search_query, product_title],
  features=[
    Attribute('jaccard_similarity_query_token_title_token', Float64)
  ],
  mode='python'
)
def search_query_product_similarity(search_query, product_title):
  def jaccard(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection) / union
    
  #Normalizing and tokenizing search query
  search_term = search_query.get('search_term')
  search_term = search_term.lower()
  tokenized_query = search_term.split(' ')

  #Normalizing and tokenizing product title
  product_title = product_title.get('product_title')
  product_title = product_title.lower()
  product_title_tokenized = product_title.split(' ')
  
  #Compute Jaccard similarity
  jaccard_similarity = jaccard(tokenized_query, product_title_tokenized)
  
  return {
    'jaccard_similarity_query_token_title_token': jaccard_similarity
    }