import pytest
from pathlib import Path
from movies_transforms import MoviesTransforms
from chispa import assert_df_equality
from pyspark.sql import functions as F, types as T
from spark_utils import create_spark_session


TEST_FOLDER_PATH = Path(__file__).resolve().parent
movies_test_path = f"{TEST_FOLDER_PATH}/data/movies_metadata.csv"
ratings_test_path = f"{TEST_FOLDER_PATH}/data/ratings.csv"


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


@pytest.fixture(scope="module")
def top_five_rated_movies_transform_test(spark):
    movies_transforms = MoviesTransforms(spark, movies_path=movies_test_path, ratings_path=ratings_test_path)
    return movies_transforms.top_five_rated_movies_transform()


def test_top_five_rated_movies_match_expected_df(spark, top_five_rated_movies_transform_test):
    top_five_rated_movies_expected = spark.read.csv(f"{TEST_FOLDER_PATH}/expected_data/expected_top_five_rated_movies.csv", header=True)
    top_five_rated_movies_expected = top_five_rated_movies_expected.withColumn("avg_rating",
                                                                               F.col("avg_rating").cast("double")
                                                                  ).withColumn("count_of_ratings",
                                                                               F.col("count_of_ratings").cast("long"))
    assert_df_equality(top_five_rated_movies_transform_test, top_five_rated_movies_expected)


def test_top_five_rated_movies_all_columns_are_present(spark, top_five_rated_movies_transform_test):
    top_five_rated_movies_cols = set(top_five_rated_movies_transform_test.columns)
    assert top_five_rated_movies_cols.issubset({"movieId", "id", "avg_rating", "title", "count_of_ratings"})


def test_top_five_rated_movies_datatypes_are_correct(spark, top_five_rated_movies_transform_test):
    are_all_datatypes_correct = [
        top_five_rated_movies_transform_test.schema["avg_rating"].dataType == T.DoubleType(),
        top_five_rated_movies_transform_test.schema["count_of_ratings"].dataType == T.LongType(),
        top_five_rated_movies_transform_test.schema["id"].dataType == T.StringType(),
        top_five_rated_movies_transform_test.schema["movieId"].dataType == T.StringType(),
        top_five_rated_movies_transform_test.schema["title"].dataType == T.StringType()]
    assert all(are_all_datatypes_correct)
