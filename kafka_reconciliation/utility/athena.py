import time


def execute_athena_query(output_location, query, athena_client):
    """Executes the given individual query against athena and return the result.

    Keyword arguments:
    output_location -- the s3 location to output the results of the execution to
    query -- the query to execute
    """
    print(
        f"Executing query and sending output results to '{output_location}'"
    )


    query_start_resp = athena_client.start_query_execution(
        QueryString=query, ResultConfiguration={"OutputLocation": output_location}
    )
    print(f"Query start response {query_start_resp}")
    if 'QueryExecutionId' in query_start_resp:
        execution_state = poll_athena_query_status(query_start_resp["QueryExecutionId"], athena_client)

        if execution_state != "SUCCEEDED":
            print(f"Non successful execution state returned: {execution_state}")
            raise KeyError(
                f"Athena query execution failed with final execution status of '{execution_state}'"
            )

        return athena_client.get_query_results(
            QueryExecutionId=query_start_resp["QueryExecutionId"]
        )
    else:
        raise Exception("Athena response didn't contain QueryExecutionId")


def poll_athena_query_status(id, athena_client):
    """Polls athena for the status of a query.

    Keyword arguments:
    id -- the id of the query in athena
    """
    print(f"Polling athena query status for id {id}")
    time_taken = 1
    while True:
        query_execution_resp = athena_client.get_query_execution(QueryExecutionId=id)

        state = query_execution_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print(
                f"Athena query execution finished in {str(time_taken)} seconds with status of '{state}'"
            )
            return state

        time.sleep(1)
        time_taken += 1
