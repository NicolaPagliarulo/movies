from pyspark.sql import functions as F, DataFrame
from constants import MOVIES_MANDATORY_COLS, RATINGS_MANDATORY_COLS


def get_average_ratings(ratings_df) -> DataFrame:
    """
    Helper code which is used for transforms inside the 'movies_transforms.py'.
    Transforms data to get the average ratings of all movies.

    :return: DataFrame
    """
    ratings_df = ratings_df.withColumn("rating", F.col("rating").cast("int"))
    avg_ratings = ratings_df.groupBy("movieId").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("userId").alias("count_of_ratings"),
    )

    return avg_ratings


def check_mandatory_cols(movies_df: DataFrame, ratings_df: DataFrame, is_caller_test_file: bool) -> None:
    """
    Checks if the data which we initially read has all the necessary cols.
    (If we don`t it throws an error which contains the missing cols)

    :param movies_df: The movies_df which we are going to use for our transformations
    :param ratings_df: The ratings_df which we are going to use for our transformations
    :param is_caller_test_file: Bool value which indicates if the caller is a test file or not

    :return: None
    """
    movies_missing_cols = set(MOVIES_MANDATORY_COLS) - set(movies_df.columns)
    if movies_missing_cols:
        raise ValueError(
            f"Missing mandatory cols for movies: {movies_missing_cols}"
        )
    else:
        if not is_caller_test_file:
            print("All mandatory cols for movies are present.")

    ratings_missing_cols = set(RATINGS_MANDATORY_COLS) - set(ratings_df.columns)
    if ratings_missing_cols:
        raise ValueError(
            f"Missing mandatory cols for ratings: {ratings_missing_cols}"
        )
    else:
        if not is_caller_test_file:
            print("All mandatory cols for ratings are present.")
