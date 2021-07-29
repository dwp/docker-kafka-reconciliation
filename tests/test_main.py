import unittest
from argparse import Namespace

from kafka_reconciliation import main



class TestRelauncher(unittest.TestCase):

    def test_default_command_line_args(self):
        default_args = main.command_line_args()
        expected_args = Namespace(distinct_default_database_collection_list_full='default_collection_list',
                                  distinct_default_database_list_full='default_database_list',
                                  manifest_counts_table_name='manifest_counts',
                                  manifest_mismatched_timestamps_table_name='manifest_mismatched',
                                  manifest_missing_exports_table_name='missing_exports',
                                  manifest_missing_imports_table_name='missing_imports',
                                  manifest_report_count_of_ids='manifest_report_count',
                                  manifest_s3_bucket='manifest_bucket',
                                  manifest_s3_output_location_queries='s3_output_location')
        self.assertEqual(default_args, expected_args)
