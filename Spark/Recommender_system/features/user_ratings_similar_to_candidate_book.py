from tecton import realtime_feature_view
from tecton.types import Float64, Int64, Field, String, Timestamp
from Recommender_system.features.book_metadata_features import book_metadata_features
from Recommender_system.features.user_recent_ratings import user_recent_ratings
from tecton import Attribute

@realtime_feature_view(
    description='''Aggregate rating metrics for the current user for the candidate book's category and author.''',
    sources=[book_metadata_features, user_recent_ratings],
    mode='python',
    features=[
        Attribute('avg_rating_for_candidate_book_category', Float64),
        Attribute('num_rating_for_candidate_book_category', Int64),
        Attribute('avg_rating_for_candidate_book_author', Float64),
        Attribute('num_rating_for_candidate_book_author', Int64),
    ]
)
def user_ratings_similar_to_candidate_book(book_metadata_features, user_recent_ratings):
    import json

    user_ratings_json = user_recent_ratings["last_200_ratings"]
    user_ratings = [json.loads(user_rating) for user_rating in user_ratings_json]

    user_ratings_same_category = []
    user_ratings_same_author = []
    candidate_category = book_metadata_features["category"]
    candidate_author = book_metadata_features["book_author"]
    for rating in user_ratings:
        if candidate_category and "category" in rating and rating["category"] == candidate_category:
            user_ratings_same_category.append(rating["rating"])
        if candidate_author and "book_author" in rating and rating["book_author"] == candidate_author:
            user_ratings_same_author.append(rating["rating"])

    output = {
        "avg_rating_for_candidate_book_category": None,
        "num_rating_for_candidate_book_category": len(user_ratings_same_category),
        "avg_rating_for_candidate_book_author": None,
        "num_rating_for_candidate_book_author": len(user_ratings_same_author),
    }

    if output["num_rating_for_candidate_book_category"] > 0:
        output["avg_rating_for_candidate_book_category"] = (
                sum(user_ratings_same_category) / output["num_rating_for_candidate_book_category"])

    if output["num_rating_for_candidate_book_author"] > 0:
        output["avg_rating_for_candidate_book_author"] = (
                sum(user_ratings_same_author) / output["num_rating_for_candidate_book_author"])

    return output