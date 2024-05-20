import argparse


def movies_transforms_args():
    """
    This functions purpose is to add and pars arguments to the movies_transforms.py script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", help="if this argument is present the dataframes won`t be written out")
    return parser.parse_args()
