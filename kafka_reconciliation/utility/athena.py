from kafka_reconciliation.utility import console_printer
from kafka_reconciliation.utility.aws import get_client
import time


def execute_athena_query(output_location, query):
    """Executes the given individual query against athena and return the result.

    Keyword arguments:
    output_location -- the s3 location to output the results of the execution to
    query -- the query to execute
    """
    console_printer.print_info(
        f"Executing query and sending output results to '{output_location}'"
    )

    athena_client = get_client(service_name="athena")
    query_start_resp = athena_client.start_query_execution(
        QueryString=query, ResultConfiguration={"OutputLocation": output_location}
    )
    execution_state = poll_athena_query_status(query_start_resp["QueryExecutionId"])

    if execution_state != "SUCCEEDED":
        raise KeyError(
            f"Athena query execution failed with final execution status of '{execution_state}'"
        )

    return athena_client.get_query_results(
        QueryExecutionId=query_start_resp["QueryExecutionId"]
    )


def poll_athena_query_status(id):
    """Polls athena for the status of a query.

    Keyword arguments:
    id -- the id of the query in athena
    """
    athena_client = get_client(service_name="athena")
    time_taken = 1
    while True:
        query_execution_resp = athena_client.get_query_execution(QueryExecutionId=id)

        state = query_execution_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            console_printer.print_info(
                f"Athena query execution finished in {str(time_taken)} seconds with status of '{state}'"
            )
            return state

        time.sleep(1)
        time_taken += 1
