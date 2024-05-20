import time
import psutil
import configs
from pathlib import Path
from pyspark.sql import functions as F, types as T, SparkSession, DataFrame
from argparse_utils import movies_transforms_args
from common.common_functions import configure_logging, show_data, log, write_dfs
from helper_functions.movies_transforms_helpers import check_mandatory_cols, get_average_ratings
from spark_utils import create_spark_session


ROOT_FOLDER_PATH = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_FOLDER_PATH.joinpath("configs/data_processing_config.yaml")
config = configs.data_processing_config(CONFIG_PATH)
WRITE_LOCATION = ROOT_FOLDER_PATH.joinpath(config["write_locations"]['processed_dataframes'])
data_sources = config["data_sources"]
log_locations = config["log_locations"]
spark = create_spark_session()


class MoviesTransforms:
    """
    This class is used to process and write the movies data.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        movies_path=str(ROOT_FOLDER_PATH.joinpath(data_sources['movies_metadata_csv'])),
        ratings_path=str(ROOT_FOLDER_PATH.joinpath(data_sources['ratings_csv'])),
    ):
        """
        Initializes the attributes spark, physical_cores, movies, ratings.
        Reads the movies and ratings data.
        Checks if all mandatory cols are present in the movies and ratings dataframes.
        Sets the logging config.
        Creates variables for the processed DataFrames.

        :param: spark_session: The spark session.
        :param: movies_path: The path to the movies data source.
        :param: ratings_path: The path to the ratings data source.
        """
        self.spark = spark_session
        configure_logging(Path(log_locations["movies_transforms_logs"]))

        start_time = time.time()
        self.movies = self.spark.read.csv(header=True, path=movies_path)
        self.ratings = self.spark.read.csv(header=True, path=ratings_path)
        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"data read approximate execution time - {exec_time}")

        check_mandatory_cols(self.movies, self.ratings)
        self.physical_cores = psutil.cpu_count(logical=False)
        self.count_of_distinct_movies = DataFrame
        self.avg_ratings = DataFrame
        self.top_five_rated = DataFrame
        self.movies_per_year = DataFrame
        self.count_of_movies_per_genre = DataFrame
        self.processed_dfs = {}

    def count_distinct_movies_transform(self) -> DataFrame:
        """
        Transforms data to get the count of distinct movies.

        :return: DataFrame
        """
        start_time = time.time()

        count_of_distinct_movies = self.movies.select("title", "id").distinct().count()
        self.count_of_distinct_movies = self.spark.createDataFrame(
            [(count_of_distinct_movies,)], [("count_of_distinct_movies")]
        )
        show_data(self.count_of_distinct_movies, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"'count_distinct_movies' transform approximate execution time - {exec_time}")

        self.processed_dfs["count_of_distinct_movies"] = self.count_of_distinct_movies
        return self.count_of_distinct_movies

    def avg_ratings_transform(self) -> DataFrame:
        """
        Transforms data to get the average ratings of all the movies.

        :return: DataFrame
        """
        start_time = time.time()

        self.avg_ratings = get_average_ratings(self.ratings)
        show_data(self.avg_ratings, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"'avg_ratings' transform approximate execution time - {exec_time}")

        self.processed_dfs["avg_ratings"] = self.avg_ratings
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
        show_data(self.top_five_rated, truncate=False)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"'top_five_rated' transform approximate execution time - {exec_time}")

        self.processed_dfs["top_five_rated"] = self.top_five_rated
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
        show_data(self.movies_per_year, n=10)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"'movies_per_year' transform approximate execution time - {exec_time}")

        self.processed_dfs["movies_per_year"] = self.movies_per_year
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
        show_data(self.count_of_movies_per_genre)

        end_time = time.time()
        exec_time = round(end_time - start_time, 2)
        log(f"'count_of_movies_per_genre' transform approximate execution time - {exec_time}")

        self.processed_dfs["count_of_movies_per_genre"] = self.count_of_movies_per_genre
        return self.count_of_movies_per_genre


if __name__ == "__main__":
    args = movies_transforms_args()
    movies_transforms = MoviesTransforms(spark)
    movies_transforms.count_distinct_movies_transform()
    movies_transforms.avg_ratings_transform()
    movies_transforms.top_five_rated_movies_transform()
    movies_transforms.movies_per_year_transform()
    movies_transforms.count_of_movies_per_genre_transform()
    processed_dfs = movies_transforms.processed_dfs
    if not args.dry_run:
        write_dfs(processed_dfs, WRITE_LOCATION)
    spark.stop()
