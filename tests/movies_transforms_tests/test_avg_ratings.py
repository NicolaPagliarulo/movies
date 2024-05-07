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
def avg_ratings_transform_test(spark):
    movies_transforms = MoviesTransforms(
        spark, movies_path=movies_test_path, ratings_path=ratings_test_path
    )
    return movies_transforms.avg_ratings_transform()


def test_avg_ratings_match_expected_df(spark, avg_ratings_transform_test):
    avg_ratings_expected = spark.read.csv(
        f"{TEST_FOLDER_PATH}/expected_data/expected_avg_ratings.csv", header=True
    )
    avg_ratings_expected = avg_ratings_expected.withColumn(
        "avg_rating", F.col("avg_rating").cast("double")
    ).withColumn("count_of_ratings", F.col("count_of_ratings").cast("long"))
    avg_ratings_expected.schema["count_of_ratings"].nullable = False

    assert_df_equality(avg_ratings_transform_test, avg_ratings_expected)


def test_avg_ratings_all_columns_are_present(spark, avg_ratings_transform_test):
    avg_ratings_cols = set(avg_ratings_transform_test.columns)
    assert avg_ratings_cols.issubset({"movieId", "avg_rating", "count_of_ratings"})


def test_avg_ratings_datatypes_are_correct(spark, avg_ratings_transform_test):
    are_all_datatypes_correct = [
        avg_ratings_transform_test.schema["movieId"].dataType == T.StringType(),
        avg_ratings_transform_test.schema["avg_rating"].dataType == T.DoubleType(),
        avg_ratings_transform_test.schema["count_of_ratings"].dataType == T.LongType(),
    ]
    assert all(are_all_datatypes_correct)
