import boto3
from botocore.config import Config
from kafka_reconciliation.utility import console_printer

def get_client(service_name, profile=None, region=None, read_timeout_seconds=120):
    """Creates a standardised boto3 client for the given service.

    Keyword arguments:
    service_name -- the aws service name (i.e. s3, lambda etc)
    profile -- the profile to use (defaults to no profile)
    region -- use a specific region for the client (defaults to no region with None)
    read_timeout -- timeout for operations, defaults to 120 seconds
    """
    global boto3_session

    ensure_session(profile)

    max_connections = 25 if service_name.lower() == "s3" else 10

    client_config = Config(
        read_timeout=read_timeout_seconds,
        max_pool_connections=max_connections,
        retries={"max_attempts": 10, "mode": "standard"},
    )

    if region is None:
        return boto3_session.client(service_name, config=client_config)
    else:
        return boto3_session.client(
            service_name, region_name=region, config=client_config
        )


def ensure_session(profile=None):
    """Ensures the global boto3 session is created.

    Keyword arguments:
    profile -- the profile name to use (if None, default profile is used)
    """
    global aws_role
    global aws_profile
    global boto3_session

    if boto3_session is None or aws_profile != profile:
        aws_profile = profile

        # Initial sessions using default creds to assume the role
        boto3_session = boto3.session.Session()
        credentials_dict = assume_role()

        if profile is None:
            boto3_session = boto3.session.Session(
                aws_access_key_id=credentials_dict["AccessKeyId"],
                aws_secret_access_key=credentials_dict["SecretAccessKey"],
                aws_session_token=credentials_dict["SessionToken"],
            )
            console_printer.print_info(f"Creating new aws session with no profile")
        else:
            boto3_session = boto3.session.Session(
                aws_access_key_id=credentials_dict["AccessKeyId"],
                aws_secret_access_key=credentials_dict["SecretAccessKey"],
                aws_session_token=credentials_dict["SessionToken"],
                profile_name=profile,
            )
            console_printer.print_info(
                f"Creating new aws session with profile of '{aws_profile}'"
            )


def assume_role():
    """Assumes the role needed for the boto3 session.

    Keyword arguments:
    profile -- the profile name to use (if None, default profile is used)
    """
    global aws_role_arn
    global aws_session_timeout_seconds
    global boto3_session

    if aws_role_arn is None or aws_session_timeout_seconds is None:
        raise AssertionError(
            f"AWS role arn ('{aws_role_arn}') or session timeout in seconds ('{aws_session_timeout_seconds}') not set, ensure 'set_details_for_role_assumption()' has been run"
        )

    session_name = "e2e_test_run_" + str(uuid.uuid4())
    console_printer.print_info(
        f"Assuming aws role via sts with arn of '{aws_role_arn}', timeout of '{aws_session_timeout_seconds}' and session name of '{session_name}'"
    )

    sts_client = boto3_session.client("sts")
    assume_role_dict = sts_client.assume_role(
        RoleArn=aws_role_arn,
        RoleSessionName=f"{session_name}",
        DurationSeconds=int(aws_session_timeout_seconds),
    )

    return assume_role_dict["Credentials"]
