import inspect
import logging
import time
import psutil
import configs
from pathlib import Path
from pyspark.sql import functions as F, types as T, SparkSession, DataFrame
from constants import MOVIES_MANDATORY_COLS, RATINGS_MANDATORY_COLS
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
        Private method which sets the logging config

        :return: None
        """
        logging.basicConfig(
            filename="logging/movies_transforms.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    @staticmethod
    def check_if_execute_from_test() -> bool:
        """
        Helper method to check if the file from which we create our class instance is from a test file

        :return: True|False
        """
        file = inspect.stack()[0]
        if "test" in file.filename:
            return True
        return False

    @staticmethod
    def check_mandatory_cols(movies_df: DataFrame, ratings_df: DataFrame) -> None:
        """
        Checks if the data which we initially read has all the necessary cols.
        (If we don`t it throws an error which contains the missing cols)

        :param: movies_df: The movies_df which we are going to use for our transformations
        :param: ratings_df: The ratings_df which we are going to use for our transformations
        :return: None
        """
        movies_missing_cols = set(MOVIES_MANDATORY_COLS) - set(movies_df.columns)
        if movies_missing_cols:
            raise ValueError(
                f"Missing mandatory cols for movies: {movies_missing_cols}"
            )
        else:
            print("All mandatory cols for movies are present.")

        ratings_missing_cols = set(RATINGS_MANDATORY_COLS) - set(ratings_df.columns)
        if ratings_missing_cols:
            raise ValueError(
                f"Missing mandatory cols for ratings: {ratings_missing_cols}"
            )
        else:
            print("All mandatory cols for ratings are present.")

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
        start_time = time.time()

        self.spark = spark_session
        self.physical_cores = psutil.cpu_count(logical=False)
        self.movies = self.spark.read.csv(header=True, path=movies_path)
        self.ratings = self.spark.read.csv(header=True, path=ratings_path)

        self.check_mandatory_cols(self.movies, self.ratings)
        self._configure_logging()

        self.count_of_distinct_movies_df = DataFrame
        self.avg_ratings = DataFrame
        self.top_five_rated = DataFrame
        self.movies_per_year = DataFrame
        self.count_of_movies_per_genre = DataFrame

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"class initialization approximate execution time - {exec_time}")

    def count_distinct_movies_transform(self) -> DataFrame:
        """
        Transforms data to get the count of distinct movies.

        :return: DataFrame
        """
        start_time = time.time()

        count_of_distinct_movies = self.movies.select("title").distinct().count()
        self.count_of_distinct_movies_df = self.spark.createDataFrame(
            [(count_of_distinct_movies,)], [("count_of_distinct_movies")]
        )

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'avg_ratings' transform approximate execution time - {exec_time}")
        self.show_data(self.count_of_distinct_movies_df, n=10)

        return self.count_of_distinct_movies_df

    def avg_ratings_transform(self) -> DataFrame:
        """
        Transforms data to get the average ratings of all the movies.

        :return: DataFrame
        """
        start_time = time.time()
        self.avg_ratings = self.get_average_ratings()

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'avg_ratings' transform approximate execution time - {exec_time}")
        self.show_data(self.avg_ratings, n=10)

        return self.avg_ratings

    def top_five_rated_movies_transform(self) -> DataFrame:
        """
        Transforms data to get the top five rated movies.

        :return: DataFrame
        """
        start_time = time.time()

        avg_ratings = self.get_average_ratings()
        movies_ratings = self.movies.join(
            avg_ratings, self.movies.id == avg_ratings.movieId, how="left"
        ).select("movieId", "id", "avg_rating", "title", "count_of_ratings")
        self.top_five_rated = movies_ratings.orderBy(
            F.col("avg_rating").desc(), F.col("count_of_ratings").desc()
        ).limit(5)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'top_five_rated' transform approximate execution time - {exec_time}")
        self.show_data(self.top_five_rated, truncate=False)

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

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'movies_per_year' transform approximate execution time - {exec_time}")
        self.show_data(self.movies_per_year, n=10)

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

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)

        self.log(f"'count_of_movies_per_genre' transform approximate execution time - {exec_time}")
        self.show_data(self.count_of_movies_per_genre)

        return self.count_of_movies_per_genre

    def get_average_ratings(self) -> DataFrame:
        """
        Common code which is used for two methods above.
        Transforms data to get the average ratings of all movies.

        :return: DataFrame
        """
        self.ratings = self.ratings.withColumn("rating", F.col("rating").cast("int"))
        avg_ratings = self.ratings.groupBy("movieId").agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.count("movieId").alias("count_of_ratings"),
        )
        return avg_ratings

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
        Shows the data of the passed DataFrame

        :param: df: The DataFrame which is going to be shown
        :param: truncate: Option passed to the shown method if we want the data to be truncated or not.
        :param: n: The number of rows to be shown
        :return: None
        """
        is_from_test_file = self.check_if_execute_from_test()
        if not is_from_test_file:
            df.show(truncate=truncate, n=n)

    def log(self, message: str) -> None:
        """
        Logs the passed message if the execution file isn`t a test file

        :param: message: The message to log
        :return: None
        """
        is_from_test_file = self.check_if_execute_from_test()
        if not is_from_test_file:
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
