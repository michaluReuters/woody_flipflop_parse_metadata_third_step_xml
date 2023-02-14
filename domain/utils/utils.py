import json
import os
import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from domain.utils.xml_handler import extract_value_from_xml

logger = Logger()
appconfig = boto3.client('appconfig')
s3_bucket = boto3.resource("s3")
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")


def prepare_data(xml_content, prefix):
    """
    This function prepares data from an XML content by calling the required fields from a configuration and extracting values from the XML content.

    :param:
        xml_content (str): The XML content to extract values from.
        prefix (str): The prefix used to identify the configuration in Parameter Store.

    :return:
        dict: A dictionary containing the extracted values, where each key is the key from the configuration and each value is the extracted value from the XML content.

    :raises:
        KeyError: If a key in the configuration does not exist in the XML content.
    """
    required_fields = call_for_required_fields(prefix)
    list_of_required_values = {v: (extract_value_from_xml(xml_content, k)) for (k, v) in
                               required_fields.items()}
    logger.info(list_of_required_values)
    return list_of_required_values


def file_in_s3_bucket(file_name_sns, prefix) -> bool:
    """
    Checks if specified file exists in s3 bucket

    :param:
        file_name_sns: file that needs to be checked

    :return:
        bool: status
    """

    try:
        logger.info(f"Looking for a file in bucket: {file_name_sns} and prefix: {prefix}")
        s3_bucket.Object(s3_bucket_name, f"{prefix}/{file_name_sns}.xml").load()
        logger.info(f"File found in bucket!")
    except ClientError:
        logger.error(f"File not found in bucket:{prefix}/{file_name_sns}")
        return False
    return True


def get_hive_api_structure(prefix):
    """
    This function retrieves the Hive API structure for a given prefix using AWS Systems Manager Parameter Store.

    :param:
        prefix (str): The prefix used to identify the Hive API structure in Parameter Store.

    :returns:
        dict: A dictionary representation of the Hive API structure.

    :raises:
        KeyError: If the required environment variables are not set.
    """
    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_HIVE_API_CALL_ID'),
        VersionNumber=int(os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_HIVE_API_CALL_VERSION'))
    )['Content'].read().decode('utf-8')

    return json.loads(configuration_prefixes)


def remove_special_characters(phrase: str) -> str:
    """
    This function removes unnecessary emojis and illegal characters from phrase

    :param:
        phrase: sentence that needs to be checked for special characters

    :return:
        str : without special characters
    """
    all_words = phrase.split()
    result = ["".join(ch for ch in word if ch.isalnum()) for word in all_words]
    words = (" ".join(result)).split()
    return " ".join(words)


def call_for_required_fields(prefix):
    """
    This function retrieves the required fields for a configuration with a given prefix using AWS Systems Manager Parameter Store.

    Args:
    prefix (str): The prefix used to identify the configuration in Parameter Store.

    Returns:
    dict: A dictionary where each key represents the source field and each value represents the destination field.

    Raises:
    KeyError: If the required environment variables are not set.
    """
    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_ID'),
        VersionNumber=int(os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_VERSION'))
    )['Content'].read().decode('utf-8')

    data = json.loads(configuration_prefixes)
    required_fields = {config["source-field"]: config["destination-field"] for config in data}
    return required_fields
