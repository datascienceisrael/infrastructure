import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='infra',
    version='0.1.0',
    author="Or Chen",
    author_email="or@datascience.co.il",
    description="DSG common infrastructure libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datascienceisrael/infrastructure",
    packages=setuptools.find_packages(),
    classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent"
    ],
)
