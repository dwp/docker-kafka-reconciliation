import boto3
from botocore.config import Config


def get_client(service_name, read_timeout_seconds=120):
    """Creates a standardised boto3 client for the given service.

    Keyword arguments:
    service_name -- the aws service name (i.e. s3, lambda etc)
    read_timeout -- timeout for operations, defaults to 120 seconds
    """
    print(f"Getting client for service {service_name}")
    max_connections = 25 if service_name.lower() == "s3" else 10

    client_config = Config(
        read_timeout=read_timeout_seconds,
        max_pool_connections=max_connections,
        retries={"max_attempts": 10, "mode": "standard"},
    )

    client = boto3.client(
        service_name, config=client_config
    )
    print(f"Boto client {client}")
    return client
