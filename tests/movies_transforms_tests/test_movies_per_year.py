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
def movies_per_year_transform_test(spark):
    movies_transforms = MoviesTransforms(spark, movies_path=movies_test_path, ratings_path=ratings_test_path)
    return movies_transforms.movies_per_year_transform()


def test_movies_per_year_match_expected_df(spark, movies_per_year_transform_test):
    movies_per_year_expected = spark.read.csv(f"{TEST_FOLDER_PATH}/expected_data/expected_movies_per_year.csv", header=True)
    movies_per_year_expected = movies_per_year_expected.withColumn("movies_released", F.col("movies_released").cast("long"))
    movies_per_year_expected.schema['movies_released'].nullable = False

    assert_df_equality(movies_per_year_transform_test, movies_per_year_expected)


def test_movies_per_year_all_columns_are_present(spark, movies_per_year_transform_test):
    movies_per_year_cols = set(movies_per_year_transform_test.columns)
    assert movies_per_year_cols.issubset({"release_date", "movies_released"})


def test_movies_per_year_datatypes_are_correct(spark, movies_per_year_transform_test):
    are_all_datatypes_correct = [
        movies_per_year_transform_test.schema["release_date"].dataType == T.StringType(),
        movies_per_year_transform_test.schema["movies_released"].dataType == T.LongType(),]
    assert all(are_all_datatypes_correct)
