# Project Movies

This project reads 2 data sources (movies, ratings_small) and processes them through 5 various transformations.
After the transformations are completed, it writes the processed dataframes in a JSON format.


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
After the run is complete you will see that the 'processed_dataframes' folder has been populated with sub-folders.
They are the result of the transformation methods and are written in a JSON format.
There is also a logging folder that got populated with a 'movies_transforms.log' file, which keeps the runtime of each transform.
