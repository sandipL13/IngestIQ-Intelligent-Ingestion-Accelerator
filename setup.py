from setuptools import setup, find_packages

setup(
    name="etl-framework",
    version="1.0.0",
    packages=find_packages(),
    install_requires=["pyspark", "boto3"],
)
