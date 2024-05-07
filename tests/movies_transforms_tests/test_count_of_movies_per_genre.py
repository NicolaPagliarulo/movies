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
def count_of_movies_per_genre_transform_test(spark):
    movies_transforms = MoviesTransforms(spark, movies_path=movies_test_path, ratings_path=ratings_test_path)
    return movies_transforms.count_of_movies_per_genre_transform()


def test_count_of_movies_per_genre_match_expected_df(spark, count_of_movies_per_genre_transform_test):
    count_of_movies_per_genre_expected = spark.read.csv(f"{TEST_FOLDER_PATH}/expected_data/expected_count_of_movies_per_genre.csv", header=True)
    for col in count_of_movies_per_genre_expected.columns:
        count_of_movies_per_genre_expected = count_of_movies_per_genre_expected.withColumn(col, F.col(col).cast("long"))
    assert_df_equality(count_of_movies_per_genre_transform_test, count_of_movies_per_genre_expected)


def test_count_of_movies_per_genre_all_columns_are_present(spark, count_of_movies_per_genre_transform_test):
    count_of_movies_per_genre_cols = set(count_of_movies_per_genre_transform_test.columns)
    assert count_of_movies_per_genre_cols.issubset({"action movies", "adventure movies", "animation movies",
                                                    "comedy movies", "crime movies", "drama movies", "family movies",
                                                    "fantasy movies", "history movies", "horror movies",
                                                    "romance movies", "thriller movies"})


def test_count_of_movies_per_genre_datatypes_are_correct(spark, count_of_movies_per_genre_transform_test):
    are_all_datatypes_correct = [
        count_of_movies_per_genre_transform_test.schema["action movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["adventure movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["animation movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["comedy movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["crime movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["drama movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["family movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["fantasy movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["history movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["horror movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["romance movies"].dataType == T.LongType(),
        count_of_movies_per_genre_transform_test.schema["thriller movies"].dataType == T.LongType(),
    ]
    assert all(are_all_datatypes_correct)
