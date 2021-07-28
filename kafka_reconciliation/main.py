import argparse
import os
import json

from kafka_reconciliation.utility import results, athena, console_printer

query_types = ["additional", "main", "specific"]
manifest_queries_local = "/queries"


def main():
    args = command_line_args()
    for query_type in query_types:
        queries = generate_comparison_queries(args, query_type)
        successful_queries, failed_queries = run_queries(queries, query_type, args)
    # replace sql templates with args
    # submit sql files to athena and wait
    # generate reports (text and json)
    # upload reports to s3(docker env vars for location)
    pass


def command_line_args():
    parser = \
        argparse.ArgumentParser(description='Submits athena queries.')

    parser.add_argument('-i', '--manifest_missing_imports_table_name', default="missing_imports", type=str,
                        help='The Athena table name for missing imports.')

    parser.add_argument('-e', '--manifest_missing_exports_table_name', default="missing_exports", type=str,
                        help='The Athena table name for missing exports.')

    parser.add_argument('-c', '--manifest_counts_table_name', default="manifest_counts", type=str,
                        help='.')

    parser.add_argument('-t', '--manifest_mismatched_timestamps_table_name', default="manifest_mismatched", type=str,
                        help='')

    parser.add_argument('-r', '--manifest_report_count_of_ids', default="manifest_report_count", type=str,
                        help='')

    parser.add_argument('-dc', '--distinct_default_database_collection_list_full', default="", type=str,
                        help='')

    parser.add_argument('-dl', '--distinct_default_database_list_full', default="", type=str,
                        help='')

    parser.add_argument('-s', '--s3_output_location', type=str,
                        help='')



    return parser.parse_args()


def generate_comparison_queries(args, query_type):
    manifest_queries = []
    for query_file in os.listdir(
            os.path.join(manifest_queries_local, query_type)
    ):
        if os.path.splitext(query_file)[1] == ".json":
            with open(
                    os.path.join(manifest_queries_local, query_type, query_file),
                    "r",
            ) as metadata_file:
                metadata = json.loads(metadata_file.read())

            with open(
                    os.path.join(
                        manifest_queries_local, query_type, metadata["query_file"]
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
            )#TODO what are these ids and timestamps
            query = query.replace(
                "[specific_id]", "521ee02f-6d75-42da-b02a-560b0bb7cbbc"
            )
            query = query.replace("[specific_timestamp]", "1585055547016")
            query = query.replace(
                "[distinct_default_database_collection_list_full]",
                args.distinct_default_database_collection_list_full,
            )
            query = query.replace(
                "[distinct_default_database_list]",
                args.distinct_default_database_list_full,
            )
            manifest_queries.append([metadata, query])

    return manifest_queries


def run_queries(manifest_queries, query_type, args):
    manifest_query_results = []
    failed_queries = []
    for query_number in range(1, len(manifest_queries) + 1):
        for manifest_query in manifest_queries:
            if int(manifest_query[0]["order"]) == query_number:
                if manifest_query[0]["enabled"] and (
                    manifest_query[0]["query_type"] == query_type
                ):
                    console_printer.print_info(
                        f"Running query with name of '{manifest_query[0]['query_name']}' "
                        + f"and description of '{manifest_query[0]['query_description']}' "
                        + f"and order of '{manifest_query[0]['order']}'"
                    )
                    try:
                        results_array = [
                            manifest_query[0],
                            athena.execute_athena_query(
                                args.manifest_s3_output_location_queries,
                                manifest_query[1],
                            ),
                        ]
                        manifest_query_results.append(results_array)
                    except Exception as ex:
                        console_printer.print_warning_text(
                            f"Error occurred running query named '{manifest_query[0]['query_name']}': '{ex}'"
                        )
                        failed_queries.append(manifest_query[0]["query_name"])
                else:
                    console_printer.print_info(
                        f"Not running query with name of '{manifest_query[0]['query_name']}' "
                        + f"because 'enabled' value is set to '{manifest_query[0]['enabled']}'"
                    )

    console_printer.print_info(f"All queries finished execution for query type {query_type}")
    return manifest_query_results, failed_queries


def upload_query_results(successful_queries, failed_queries, args):
    console_printer.print_info("Generating test result")
    results_string = results.generate_formatted_results(
        context.manifest_query_results
    )
    console_printer.print_info(f"\n\n\n\n\n{results_string}\n\n\n\n\n")

    results_file_name = f"{context.test_run_name}_results.txt"
    results_file = os.path.join(context.temp_folder, results_file_name)
    with open(results_file, "wt") as open_results_file:
        open_results_file.write(console_printer.strip_formatting(results_string))

    s3_uploaded_location_txt = os.path.join(
        context.manifest_s3_output_prefix_results, results_file_name
    )
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        results_file,
        context.manifest_s3_bucket,
        context.timeout,
        s3_uploaded_location_txt,
    )

    console_printer.print_bold_text(
        f"Uploaded text results file to S3 bucket with name of '{context.manifest_s3_bucket}' at location '{s3_uploaded_location_txt}'"
    )

    os.remove(results_file)

    console_printer.print_info("Generating json result")
    results_json = results.generate_json_formatted_results(
        context.manifest_query_results, context.test_run_name
    )

    json_file_name = f"{context.test_run_name}_results.json"
    json_file = os.path.join(context.temp_folder, json_file_name)
    with open(json_file, "w") as open_json_file:
        json.dump(results_json, open_json_file, indent=4)

    s3_uploaded_location_json = os.path.join(
        context.manifest_s3_output_prefix_results, json_file_name
    )
    aws_helper.upload_file_to_s3_and_wait_for_consistency(
        json_file,
        context.manifest_s3_bucket,
        context.timeout,
        s3_uploaded_location_json,
    )

    console_printer.print_bold_text(
        f"Uploaded json results file to S3 bucket with name of '{context.manifest_s3_bucket}' at location '{s3_uploaded_location_json}'"
    )

    os.remove(json_file)

    if len(context.failed_queries) > 0:
        raise AssertionError(
            "The following queries failed to execute: "
            + ", ".join(context.failed_queries)
        )
    else:
        console_printer.print_info(f"All queries executed successfully")

    console_printer.print_info(f"Query execution step completed")
