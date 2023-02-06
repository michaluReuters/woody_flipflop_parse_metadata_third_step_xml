import json
import os
import boto3
from aws_lambda_powertools import Logger
from domain.utils.xml_handler import extract_value_from_xml

logger = Logger()
appconfig = boto3.client('appconfig')


def prepare_data(xml_content, prefix):
    required_fields = call_for_required_fields(prefix)
    list_of_required_values = {v: (extract_value_from_xml(xml_content, k)) for (k, v) in
                               required_fields.items()}
    logger.info(list_of_required_values)
    return list_of_required_values


def call_for_required_fields(prefix):
    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_ID'),
        VersionNumber=int(os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_VERSION'))
    )['Content'].read().decode('utf-8')

    data = json.loads(configuration_prefixes)
    required_fields = {config["source-field"]: config["destination-field"] for config in data}
    return required_fields
