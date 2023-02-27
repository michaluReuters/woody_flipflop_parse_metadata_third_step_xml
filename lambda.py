import json
import os
import boto3 as boto3
from aws_lambda_powertools import Logger
from domain.utils.utils import prepare_data, file_in_s3_bucket

logger = Logger()
client = boto3.client('events')
s3_client = boto3.client("s3")
LAMBDA_NAME = os.environ["AWS_LAMBDA_FUNCTION_NAME"]


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    dict_event = event['detail']
    name = dict_event['name']
    prefix = dict_event['prefix']

    if not file_in_s3_bucket(name, prefix):
        logger.info(f"Could not find resource in s3 for: {name}.xml")
        raise FileNotFoundError

    object_key = f"{prefix}/{name}.xml"
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    file_content = s3_client.get_object(
        Bucket=bucket_name, Key=object_key)["Body"].read().decode("UTF-8")

    required_fields = prepare_data(file_content, prefix)

    dict_event.update(required_fields)
    dict_event.pop("prefix")
    data_str = json.dumps(dict_event)

    entry = {
        "Source": "step3-complete",
        "Resources": ["metadata-import-step3"],
        "DetailType": "metadata-step-complete",
        'Detail': data_str
    }
    logger.info(f"Completing step 3 with entry: {entry}")
    response = client.put_events(
        Entries=[entry, ]
    )

    logger.info(response)
