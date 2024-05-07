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
def count_distinct_movies_transform_test(spark):
    movies_transforms = MoviesTransforms(
        spark, movies_path=movies_test_path, ratings_path=ratings_test_path
    )
    return movies_transforms.count_distinct_movies_transform()


def test_count_distinct_movies_match_expected_df(spark, count_distinct_movies_transform_test):
    count_distinct_movies_expected = spark.read.csv(
        f"{TEST_FOLDER_PATH}/expected_data/expected_count_distinct_movies.csv",
        header=True,
    )
    count_distinct_movies_expected = count_distinct_movies_expected.withColumn(
        "count_of_distinct_movies", F.col("count_of_distinct_movies").cast("long")
    )
    assert_df_equality(
        count_distinct_movies_transform_test, count_distinct_movies_expected
    )


def test_count_distinct_movies_all_columns_are_present(spark, count_distinct_movies_transform_test):
    count_distinct_movies_cols = set(count_distinct_movies_transform_test.columns)
    assert count_distinct_movies_cols.issubset({"count_of_distinct_movies"})


def test_count_distinct_movies_datatypes_are_correct(spark, count_distinct_movies_transform_test):
    are_all_datatypes_correct = [
        count_distinct_movies_transform_test.schema["count_of_distinct_movies"].dataType
        == T.LongType()
    ]
    assert all(are_all_datatypes_correct)
