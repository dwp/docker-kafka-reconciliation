import os
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock
from unittest.mock import call

from moto import mock_athena

from kafka_reconciliation import main


class TestRelauncher(unittest.TestCase):

    def test_main_comparison_query_generation(self):
        args = self.get_testing_args()
        generated_queries = main.generate_comparison_queries(args, "main")

        for [_, query] in generated_queries:
            self.assertIn(args.manifest_counts_table_name, query)

    @mock_athena
    @mock.patch("kafka_reconciliation.utility.athena.poll_athena_query_status")
    @mock.patch("boto3.client")
    def test_run_queries(self, _mock_boto_client, mock_poll_athena):
        mock_poll_athena.side_effect = ["SUCCEEDED", "SUCCEEDED", "SUCCEEDED", "SUCCEEDED", "FAILED", "FAILED",
                                        "FAILED", "FAILED"]
        args = self.get_testing_args()
        main_queries = main.generate_comparison_queries(args, "main")
        manifest_query_results, failed_queries = main.run_queries(main_queries, "main", args)
        self.assertEqual(len(manifest_query_results), 4)
        self.assertEqual(len(failed_queries), 4)

    @mock.patch("kafka_reconciliation.main.upload_file_to_s3_and_wait_for_consistency")
    def test_upload_query_results(self, mock_upload):
        path = Path(os.getcwd())
        results_path = f"{path.parent.absolute()}/tests"
        main.TEMP_FOLDER = results_path
        main.TEST_RUN_NAME = "upload_tests"
        main.S3_TIMEOUT = 5

        mock_athena.return_value = "mock_s3_path"
        results_string = "test results"
        results_json = {"test": "test"}

        main.upload_query_results(results_string, results_json, self.get_testing_args())
        self.assertEqual(mock_upload.call_count, 2)
        calls = [
            call(
                f'{results_path}/upload_tests_results.txt',
                'test_manifest_bucket',
                5,
                'test_s3_query_location/upload_tests_results.txt'
            ),
            call(
                f'{results_path}/upload_tests_results.json',
                'test_manifest_bucket',
                5,
                'test_s3_query_location/upload_tests_results.json'
            )
        ]

        mock_upload.assert_has_calls(calls)

    def test_default_command_line_args(self):
        default_args = main.command_line_args()
        expected_args = Namespace(
            distinct_default_database_collection_list_full='default_collection_list',
            distinct_default_database_list_full='default_database_list',
            manifest_counts_table_name='manifest_counts',
            manifest_mismatched_timestamps_table_name='manifest_mismatched',
            manifest_missing_exports_table_name='missing_exports',
            manifest_missing_imports_table_name='missing_imports',
            manifest_report_count_of_ids='manifest_report_count',
            manifest_s3_bucket='manifest_bucket',
            manifest_s3_output_location_queries='s3_output_query_location',
            manifest_s3_output_prefix_results='s3_output_location'
        )
        self.assertEqual(default_args, expected_args)

    def setUp(self):
        path = Path(os.getcwd())
        query_path = f"{path.parent.absolute()}/queries"
        main.MANIFEST_QUERIES_LOCAL = query_path

    @staticmethod
    def get_testing_args():
        return Namespace(
            distinct_default_database_collection_list_full='test_default_collection_list',
            distinct_default_database_list_full='test_default_database_list',
            manifest_counts_table_name='test_manifest_counts',
            manifest_mismatched_timestamps_table_name='test_manifest_mismatched',
            manifest_missing_exports_table_name='test_missing_exports',
            manifest_missing_imports_table_name='test_missing_imports',
            manifest_report_count_of_ids='test_manifest_report_count',
            manifest_s3_bucket='test_manifest_bucket',
            manifest_s3_output_location_queries='test_s3_output_location',
            manifest_s3_output_prefix_results='test_s3_query_location'

        )
