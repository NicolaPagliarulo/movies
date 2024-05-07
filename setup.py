from setuptools import setup, find_packages


setup(
    name='python-task-adastra',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'chispa',
        'pyYAML',
        'pyspark',
        'psutil',
        'pytest'
    ],
    author='Nicola Pagliarulo',
    author_email='nicola.pagliarulo200@gmail.com',
    description='A package for transforming movie data using PySpark',
    url='https://github.com/yourusername/movie-transforms',
    license='MIT',
)
