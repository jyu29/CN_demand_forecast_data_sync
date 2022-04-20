from setuptools import setup

setup(
    # Metadata
    name='forecast-data-ingestion',
    version='1.0.0',
    author='FUN Team',
    author_email='forecastunited@decathlon.net',
    description='The Data Ingestion brick (with Spark) of the Demand Forecasting project for APO.',
    install_requires=["boto3", "PyYAML"]
)
