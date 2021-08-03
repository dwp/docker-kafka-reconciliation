# docker-kafka-reconciliation

## About the project
This project produces a Docker Image which executes athena queries and uploads the results to S3. 
These queries are used to compare data produced by dataworks and Crown. 


## Command line Arguments

|Variable name|Shortened|Example|Description|Required|
|:---|:---|:---|:---|:---|
|manifest_missing_imports_table_name| -i |manifest_missing_imports|The Athena table name for missing imports.|
|manifest_missing_exports_table_name| -e |manifest_missing_exports|The Athena table name for missing exports.|
|manifest_counts_table_name| -c |manifest_counts|The Athena table name for manifest counts.|
|manifest_mismatched_timestamps_table_name| -t |manifest_mismatched_timestamps|The Athena table name for mismatched timestamps.|
|manifest_report_count_of_ids| -r |10|Number of ids.|
|manifest_bucket| -b |manifest_bucket|S3 bucket name.|yes|
|manifest_prefix| -p |kafka/reconciliation/|Base S3 prefix.|


## Local Setup

A Makefile is provided for project setup.

Run `make setup-local` This will create a Python virtual environment and install and dependencies. 

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.



