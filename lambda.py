import json
import os
import boto3 as boto3
from aws_lambda_powertools import Logger
from domain.utils.utils import prepare_data, prepare_request_body

logger = Logger()
client = boto3.client('events')
s3_client = boto3.client("s3")


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    dict_event = event['detail']
    name = dict_event['file_name']
    prefix = dict_event['prefix']

    object_key = f"{prefix}/{name}.xml"
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    file_content = s3_client.get_object(
        Bucket=bucket_name, Key=object_key)["Body"].read().decode("UTF-8")

    required_fields = prepare_data(file_content, prefix)

    for k, v in required_fields.items():
        dict_event[k] = v

    result = prepare_request_body(prefix, dict_event)

    data_str = json.dumps(result)

    entry = {
        "Source": "new-ppe-sonyhivemetadata-step3-complete",
        "Resources": ["new-ppe-sh-sonyhive-metadata-import-step3-json-lambda"],
        "DetailType": "metadata-step-complete",
        'Detail': data_str
    }
    logger.info(f"Completing step 2 with entry: {entry}")
    response = client.put_events(
        Entries=[entry, ]
    )

    logger.info(response)





