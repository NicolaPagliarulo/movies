import yaml


def data_processing_config(file_path):
    """
    Reads the configuration from a YAML file.

    :param file_path: The path to the YAML configuration file.
    :return: dict
    """
    with open(file_path, "r") as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config
