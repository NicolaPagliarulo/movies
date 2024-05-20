import inspect
import logging
from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame


def configure_logging(filename: str, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s") -> None:
    """
    Function that sets the logging config.

    :param: filename: path to the .log file.
    :param: level: the level of the logging.
    :param: format: the format for the logs.
    :return: None
    """
    logging.basicConfig(
        filename=filename,
        level=level,
        format=format,
    )


def show_data(df: DataFrame, truncate=True, n=20) -> None:
    """
    Shows the data of the passed DataFrame if the execution file isn`t a test file.

    :param: df: The DataFrame which is going to be shown.
    :param: truncate: Option passed to the shown method if we want the data to be truncated or not.
    :param: n: The number of rows to be shown.
    :return: None
    """
    is_caller_a_test_file = check_if_entry_point_is_a_test_file()
    if not is_caller_a_test_file:
        df.show(truncate=truncate, n=n)


def log(message: str) -> None:
    """
    Logs the passed message if the execution file isn`t a test file.

    :param: message: The message to log.
    :return: None
    """
    is_entry_point_a_test_file = check_if_entry_point_is_a_test_file()
    if not is_entry_point_a_test_file:
        logging.info(message)


def check_if_entry_point_is_a_test_file() -> bool:
    """
    Checks if the entry point is a test file.
    :return: bool
    """
    is_entry_point_a_test_file = False
    for frame in inspect.stack():
        if "tests" in frame.filename:
            is_entry_point_a_test_file = True
            break
    return is_entry_point_a_test_file


def write_dfs(dfs: Dict[str, DataFrame], path: Path) -> None:
    """
    Function to write dataframes.
    :param: dfs: dictionary that has a dataframe name as key and the actual dataframe as value.
    :param: path: the path where the dataframes are written to.
    :return: None
    """
    for df_name, df in dfs.items():
        df.write.mode("overwrite").json(str(path.joinpath(df_name)))
