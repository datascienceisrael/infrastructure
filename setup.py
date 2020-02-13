import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="dsg-infra",
    version="0.0.0.9001",
    author="Data Science Group",
    author_email="or@datascience.co.il, shaul@datascience.co.il, pavels@datascience.co.il",
    description="DSG common infrastructure libraries",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/datascienceisrael/infrastructure",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
)
