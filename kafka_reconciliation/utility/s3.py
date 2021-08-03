from utility.aws import get_client


def upload_file_to_s3_and_wait_for_consistency(
    file_location, s3_bucket, seconds_timeout, s3_key, s3_client=None
):
    """Uploads the given file to s3 and waits for the file to be written, will error if not consistent after 6 minutes.

    Keyword arguments:
    file_location -- the local path and name of the file to upload
    s3_bucket -- the bucket to upload to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_key -- the key of where to upload the file to in s3
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    s3_client.upload_file(file_location, s3_bucket, s3_key)

    return wait_for_file_to_be_in_s3(s3_bucket, s3_key, seconds_timeout, s3_client)


def wait_for_file_to_be_in_s3(s3_bucket, s3_key, seconds_timeout, s3_client=None):
    """Waits for the file to be in s3, will error if not consistent after 2 minutes.

    Keyword arguments:
    s3_bucket -- the bucket to upload to in s3
    s3_key -- the key of where to upload the file to in s3
    seconds_timeout -- the number of seconds to wait for
    s3_client -- client to override the standard one
    """
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    waiter = s3_client.get_waiter("object_exists")
    waiter.wait(
        Bucket=s3_bucket,
        Key=s3_key,
        WaiterConfig={"Delay": 1, "MaxAttempts": seconds_timeout},
    )
    return s3_key

