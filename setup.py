"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="docker_kafka_reconciliation",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="Submits Kafka reconciliation queries to Athena and uploads the results to S3",
    entry_points={
        "console_scripts": [
            "query=kafka_reconciliation:main"
        ]
    },
    package_dir={"": "kafka_reconciliation"},
    packages=setuptools.find_packages("kafka_reconciliation"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
