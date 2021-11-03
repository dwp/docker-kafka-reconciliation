import argparse
import json
import os

from utility import results, athena, console_printer
from utility.aws import get_client
from utility.s3 import upload_file_to_s3_and_wait_for_consistency

query_types = ["main"]
MANIFEST_QUERIES_LOCAL = "/queries"
S3_TIMEOUT = 30
TEST_RUN_NAME = "dataworks_kafka_reconciliation"
TEMP_FOLDER = "/results"


def main():
    print(f"Executing kafka reconciliation")

    successful_queries = []
    failed_queries = []

    try:
        args = command_line_args()
        athena_client = get_client(service_name="athena", region=args.region)
        s3_client = get_client(service_name="s3")
        for query_type in query_types:
            print(f"Starting reconciliation for query type {query_type}")
            queries = generate_comparison_queries(args, query_type)
            successes, failures = run_queries(queries, query_type, args, athena_client)
            successful_queries.extend(successes)
            failed_queries.extend(failures)

        if len(failed_queries) > 0:
            print(
                "The following queries failed to execute: "
                + ", ".join(failed_queries)
            )
            exit(1)

        results_string, results_json = results.generate_formatted_results(
            successful_queries, args.test_run_name
        )
        upload_query_results(results_string, results_json, args, s3_client)

        print(f"All queries executed successfully")
        exit(0)
    except Exception as ex:
        print(f"Exception in main {ex}")
        exit(1)


def command_line_args():
    parser = \
        argparse.ArgumentParser(description='Submits athena queries.')

    parser.add_argument('-i', '--manifest_missing_imports_table_name', default="manifest_missing_imports_parquet",
                        type=str,
                        help='The Athena table name for missing imports.')

    parser.add_argument('-e', '--manifest_missing_exports_table_name', default="manifest_missing_exports_parquet",
                        type=str,
                        help='The Athena table name for missing exports.')

    parser.add_argument('-c', '--manifest_counts_table_name', default="manifest_counts_parquet", type=str,
                        help='The Athena table name for manifest counts.')

    parser.add_argument('-t', '--manifest_mismatched_timestamps_table_name',
                        default="manifest_mismatched_timestamps_parquet",
                        type=str,
                        help='The Athena table name for mismatched timestamps.')

    parser.add_argument('-r', '--manifest_report_count_of_ids', default="10", type=str,
                        help='')

    parser.add_argument('-p', '--manifest_s3_prefix', default="business-data/manifest/query-output", type=str,
                        help='Base S3 prefix to save results and queries')

    parser.add_argument('-b', '--manifest_s3_bucket', type=str, required=True,
                        help='The s3 bucket to upload  results to')

    args = parser.parse_args()

    if {'AWS_BATCH_JOB_ID', 'AWS_BATCH_JOB_ATTEMPT'}.issubset(os.environ):
        args.test_run_name = f"{os.environ['AWS_BATCH_JOB_ID']}_{os.environ['AWS_BATCH_JOB_ATTEMPT']}"
    else:
        args.test_run_name = TEST_RUN_NAME

    args.region = os.environ.get('AWS_DEFAULT_REGION', "eu-west-2")

    print(f"Parsed Command line arguments {args}")

    return args


def generate_comparison_queries(args, query_type):
    manifest_queries = []
    for query_file in os.listdir(
            os.path.join(MANIFEST_QUERIES_LOCAL, query_type)
    ):
        if os.path.splitext(query_file)[1] == ".json":
            with open(
                    os.path.join(MANIFEST_QUERIES_LOCAL, query_type, query_file),
                    "r",
            ) as metadata_file:
                metadata = json.loads(metadata_file.read())

            with open(
                    os.path.join(
                        MANIFEST_QUERIES_LOCAL, query_type, metadata["query_file"]
                    ),
                    "r",
            ) as query_sql_file:
                base_query = query_sql_file.read()

            query = base_query.replace(
                "[parquet_table_name_missing_imports]",
                args.manifest_missing_imports_table_name,
            )
            query = query.replace(
                "[parquet_table_name_missing_exports]",
                args.manifest_missing_exports_table_name,
            )
            query = query.replace(
                "[parquet_table_name_counts]", args.manifest_counts_table_name
            )
            query = query.replace(
                "[parquet_table_name_mismatched]",
                args.manifest_mismatched_timestamps_table_name,
            )
            query = query.replace(
                "[count_of_ids]", str(args.manifest_report_count_of_ids)
            )
            query = query.replace(
                "[specific_id]", "521ee02f-6d75-42da-b02a-560b0bb7cbbc"
            )
            query = query.replace("[specific_timestamp]", "1585055547016")
            manifest_queries.append([metadata, query])
    print(f"Number of queries generated for query type {query_type}: {len(manifest_queries)}")
    return manifest_queries


def run_queries(manifest_queries, query_type, args, athena_client):
    print(f"Running queries for query type {query_type}")
    print(f"Manifest queries {manifest_queries}")
    manifest_query_results = []
    failed_queries = []
    s3_location = "s3://" + os.path.join(
        args.manifest_s3_bucket, args.manifest_s3_prefix, "queries"
    )
    for query_number in range(1, len(manifest_queries) + 1):
        for manifest_query in manifest_queries:
            if int(manifest_query[0]["order"]) == query_number:
                if manifest_query[0]["enabled"] and (
                        manifest_query[0]["query_type"] == query_type
                ):
                    print(
                        f"Running query with name of '{manifest_query[0]['query_name']}' "
                        + f"and description of '{manifest_query[0]['query_description']}' "
                        + f"and order of '{manifest_query[0]['order']}'"
                    )
                    try:
                        query_result = athena.execute_athena_query(
                            s3_location,
                            manifest_query[1],
                            athena_client
                        )
                        print(f"Query result {query_result}")

                        results_array = [
                            manifest_query[0],
                            query_result
                        ]
                        manifest_query_results.append(results_array)
                    except Exception as ex:
                        print(
                            f"Error occurred running query named '{manifest_query[0]['query_name']}': '{ex}'"
                        )
                        failed_queries.append(manifest_query[0]["query_name"])
                else:
                    print(
                        f"Not running query with name of '{manifest_query[0]['query_name']}' "
                        + f"because 'enabled' value is set to '{manifest_query[0]['enabled']}'"
                    )

    print(f"All queries finished execution for query type {query_type}")
    print(f"Number of SUCCESS queries for  {query_type}: {len(manifest_query_results)}")
    print(f"Number of FAILED queries for  {query_type}: {len(failed_queries)}")
    return manifest_query_results, failed_queries


def upload_query_results(results_string, results_json, args, s3_client):
    print("Generating test result")
    print(f"\n\n\n\n\n{results_string}\n\n\n\n\n")

    results_file_name = f"{args.test_run_name}_results.txt"
    results_file = os.path.join(TEMP_FOLDER, results_file_name)

    s3_output_prefix = os.path.join(
        args.manifest_s3_prefix, "results"
    )
    with open(results_file, "wt") as open_results_file:
        open_results_file.write(console_printer.strip_formatting(results_string))

    s3_uploaded_location_txt = os.path.join(
        s3_output_prefix, results_file_name
    )
    upload_file_to_s3_and_wait_for_consistency(
        results_file,
        args.manifest_s3_bucket,
        S3_TIMEOUT,
        s3_uploaded_location_txt,
        s3_client=s3_client
    )

    print(
        f"Uploaded text results file to S3 bucket with name of '{args.manifest_s3_bucket}' / at location '{s3_uploaded_location_txt}'"
    )

    os.remove(results_file)

    print("Generating json result")

    json_file_name = f"{args.test_run_name}_results.json"
    json_file = os.path.join(TEMP_FOLDER, json_file_name)
    with open(json_file, "w") as open_json_file:
        json.dump(results_json, open_json_file, indent=4)

    s3_uploaded_location_json = os.path.join(
        s3_output_prefix, json_file_name
    )
    upload_file_to_s3_and_wait_for_consistency(
        json_file,
        args.manifest_s3_bucket,
        S3_TIMEOUT,
        s3_uploaded_location_json,
        s3_client=s3_client
    )

    print(
        f"Uploaded json results file to S3 bucket with name of '{args.manifest_s3_bucket}' at location '{s3_uploaded_location_json}'"
    )

    os.remove(json_file)

    print(f"Query execution step completed")


if __name__ == '__main__':
    main()
