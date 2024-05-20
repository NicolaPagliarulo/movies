# Project Movies

This project reads 2 data sources (movies, ratings_small) and processes them through 5 various transformations.
After the transformations are completed, it writes the processed dataframes in a JSON format.

# ! This project uses Apache Spark !
If you haven`t set up your Apache Spark I suggest you follow a tutorial on how to do it and take into consideration your os.

Linux - https://www.youtube.com/watch?v=ei_d4v9c2iA
Windows - https://www.youtube.com/watch?v=OmcSTQVkrvo&t=522s

## Installation

First you will need to clone the repo on your system.

```bash
git clone https://github.com/NicolaPagliarulo/movies.git
```
After that you will need to install the dependencies.
(You need to set up a virtual environment before proceeding)

```bash
python setup.py install
```

# Usage

## Running the 'movies_transforms.py'
If the installation above went smoothly you can now go to the 'movies_transforms.py' file and run it.
You can either set up a run configuration or use this command (replace the PATH_TO_MOVIES_TRANSFORMS with your path)
Example - `python C:\python_projects\movies\movies_transforms.py`

```bash
python {PATH_TO_MOVIES_TRANSFORMS}
```
If you don`t want to write the dataframes you can add the '--dry_run' argument
```bash
python {PATH_TO_MOVIES_TRANSFORMS} --dry_run
```
(If you are using the run config, you can add it in the 'script parameters' field instead)

After the run is complete, you will see that the 'processed_dataframes' folder has been populated with sub-folders (if --dry_run is not used).
They are the result of the transformation methods and are written in a JSON format.
There is also a logging folder that got populated with a 'movies_transforms.log' file, which keeps the runtime of each transform.

## Running the tests
To run the tests, you can use this command (replace the PATH_TO_TESTS_FOLDER with your path)
Example - `pytest C:\python_projects\movies\tests`

```bash
pytest {PATH_TO_TESTS_FOLDER}
```
