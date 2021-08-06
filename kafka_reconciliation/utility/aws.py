import boto3
from botocore.config import Config


def get_client(service_name, region=None, read_timeout_seconds=120):
    """Creates a standardised boto3 client for the given service.

    Keyword arguments:
    service_name -- the aws service name (i.e. s3, lambda etc)
    region -- use a specific region for the client (defaults to no region with None)
    read_timeout -- timeout for operations, defaults to 120 seconds
    """
    print(f"Getting client for service {service_name}")
    max_connections = 25 if service_name.lower() == "s3" else 10

    client_config = Config(
        read_timeout=read_timeout_seconds,
        max_pool_connections=max_connections,
        retries={"max_attempts": 10, "mode": "standard"},
    )

    if region is None:
        return boto3.client(service_name, config=client_config)
    else:
        return boto3.client(
            service_name, region_name=region, config=client_config
        )
