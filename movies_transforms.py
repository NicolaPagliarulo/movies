import inspect
import logging
import time
import psutil
import configs
from pathlib import Path
from pyspark.sql import functions as F, types as T, SparkSession, DataFrame
from helper_functions.movies_transforms_helpers import check_mandatory_cols, get_average_ratings
from spark_utils import create_spark_session


FOLDER_PATH = Path(__file__).resolve().parent
config = configs.data_processing_config(f"{FOLDER_PATH}/configs/data_processing_config.yaml")
data_sources = config["data_sources"]
write_locations = config["write_locations"]
spark = create_spark_session()


class MoviesTransforms:
    """
    This class is used to process and write the movies data.
    """

    @staticmethod
    def _configure_logging() -> None:
        """
        Static method which sets the logging config

        :return: None
        """
        logging.basicConfig(
            filename="logging/movies_transforms.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def __init__(
        self,
        spark_session: SparkSession,
        movies_path=f"{str(FOLDER_PATH)}/{data_sources['movies_metadata_csv']}",
        ratings_path=f"{str(FOLDER_PATH)}/{data_sources['ratings_csv']}",
    ):
        """
        Initializes the attributes spark, physical_cores, movies, ratings.
        Reads the movies and ratings data.
        Checks if all mandatory cols are present in the movies and ratings dataframes.
        Sets the logging config
        Creates variables for the processed DataFrames

        :param: spark_session: The spark session
        :param: movies_path: The path to the movies data source
        :param: ratings_path: The path to the ratings data source
        """
        self.spark = spark_session
        self._configure_logging()
        caller_file_path = inspect.stack()[1].filename
        self.is_caller_test_file = False
        if "test" in caller_file_path:
            self.is_caller_test_file = True

        start_time = time.time()
        self.movies = self.spark.read.csv(header=True, path=movies_path)
        self.ratings = self.spark.read.csv(header=True, path=ratings_path)
        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        self.log(f"class initialization approximate execution time - {exec_time}")

        check_mandatory_cols(self.movies, self.ratings, self.is_caller_test_file)
        self.physical_cores = psutil.cpu_count(logical=False)
        self.count_of_distinct_movies_df = DataFrame
        self.avg_ratings = DataFrame
        self.top_five_rated = DataFrame
        self.movies_per_year = DataFrame
        self.count_of_movies_per_genre = DataFrame

    def count_distinct_movies_transform(self) -> DataFrame:
        """
        Transforms data to get the count of distinct movies.

        :return: DataFrame
        """
        start_time = time.time()

        count_of_distinct_movies = self.movies.select("title", "id").distinct().count()
        self.count_of_distinct_movies_df = self.spark.createDataFrame(
            [(count_of_distinct_movies,)], [("count_of_distinct_movies")]
        )
        self.show_data(self.count_of_distinct_movies_df, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'count_distinct_movies' transform approximate execution time - {exec_time}")

        return self.count_of_distinct_movies_df

    def avg_ratings_transform(self) -> DataFrame:
        """
        Transforms data to get the average ratings of all the movies.

        :return: DataFrame
        """
        start_time = time.time()

        self.avg_ratings = get_average_ratings(self.ratings)
        self.show_data(self.avg_ratings, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'avg_ratings' transform approximate execution time - {exec_time}")

        return self.avg_ratings

    def top_five_rated_movies_transform(self) -> DataFrame:
        """
        Transforms data to get the top five rated movies.

        :return: DataFrame
        """
        start_time = time.time()

        avg_ratings = get_average_ratings(self.ratings)
        movies_ratings = self.movies.join(
            avg_ratings, self.movies.id == avg_ratings.movieId, how="left"
        ).select("movieId", "id", "avg_rating", "title", "count_of_ratings")
        self.top_five_rated = movies_ratings.orderBy(
            F.col("avg_rating").desc(), F.col("count_of_ratings").desc()
        ).limit(5)
        self.show_data(self.top_five_rated, truncate=False)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'top_five_rated' transform approximate execution time - {exec_time}")

        return self.top_five_rated

    def movies_per_year_transform(self) -> DataFrame:
        """
        Transforms data to get the count of movies released per year.

        :return: DataFrame
        """
        start_time = time.time()

        self.movies_per_year = self.movies.groupBy("release_date").agg(
            F.count("id").alias("movies_released")
        )
        self.show_data(self.movies_per_year, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'movies_per_year' transform approximate execution time - {exec_time}")

        return self.movies_per_year

    def count_of_movies_per_genre_transform(self) -> DataFrame:
        """
        Transforms data to get the count of movies per genre.

        :return: DataFrame
        """
        start_time = time.time()

        json_schema = T.ArrayType(
            T.StructType(
                [
                    T.StructField("id", T.IntegerType()),
                    T.StructField("name", T.StringType()),
                ]
            )
        )
        movie_genres_exploded = self.movies.select(
            "id", F.explode(F.from_json("genres", json_schema)).alias("genre")
        )
        movie_genres_pivoted = (
            movie_genres_exploded.groupBy("id").pivot("genre.name").count().fillna(0)
        )
        all_genres = movie_genres_pivoted.columns[1:]
        count_of_genres_select_expr = [
            f"SUM(`{genre}`) as `{genre.lower()} movies`" for genre in all_genres
        ]
        self.count_of_movies_per_genre = movie_genres_pivoted.selectExpr(
            *count_of_genres_select_expr
        )
        self.show_data(self.count_of_movies_per_genre)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'count_of_movies_per_genre' transform approximate execution time - {exec_time}")

        return self.count_of_movies_per_genre

    def write_dfs(self) -> None:
        """
        Method to write all the result dataframes created from the transformation methods.

        :return: None
        """
        self.count_of_distinct_movies_df.write.mode("overwrite").json(
            f"{str(FOLDER_PATH)}/{write_locations['processed_dataframes']}/count_of_distinct_movies"
        )

        self.avg_ratings.coalesce(self.physical_cores).write.mode("overwrite").json(
            f"{str(FOLDER_PATH)}/{write_locations['processed_dataframes']}/avg_ratings"
        )

        self.top_five_rated.write.mode("overwrite").json(
            f"{str(FOLDER_PATH)}/{write_locations['processed_dataframes']}/top_five_rated"
        )

        self.movies_per_year.coalesce(self.physical_cores).write.mode("overwrite").json(
            f"{str(FOLDER_PATH)}/{write_locations['processed_dataframes']}/movies_per_year"
        )

        self.count_of_movies_per_genre.write.mode("overwrite").json(
            f"{str(FOLDER_PATH)}/{write_locations['processed_dataframes']}/count_of_movies_per_genre"
        )

    def show_data(self, df: DataFrame, truncate=True, n=20) -> None:
        """
        Shows the data of the passed DataFrame if the execution file isn`t a test file

        :param: df: The DataFrame which is going to be shown
        :param: truncate: Option passed to the shown method if we want the data to be truncated or not.
        :param: n: The number of rows to be shown
        :return: None
        """
        if not self.is_caller_test_file:
            df.show(truncate=truncate, n=n)

    def log(self, message: str) -> None:
        """
        Logs the passed message if the execution file isn`t a test file

        :param: message: The message to log
        :return: None
        """
        if not self.is_caller_test_file:
            logging.info(message)


if __name__ == "__main__":
    movies_transforms = MoviesTransforms(spark)
    movies_transforms.count_distinct_movies_transform()
    movies_transforms.avg_ratings_transform()
    movies_transforms.top_five_rated_movies_transform()
    movies_transforms.movies_per_year_transform()
    movies_transforms.count_of_movies_per_genre_transform()
    movies_transforms.write_dfs()
    spark.stop()
