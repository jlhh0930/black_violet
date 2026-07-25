from distutils.command.config import config

import boto3
from botocore.exceptions import ClientError
import json
import uuid
from io import StringIO

def get_secret(secret_arn, region_name, log):
    session = boto3.Session(region_name=region_name)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = {}
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_arn
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            log.exception("DecryptionFailureException - Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            log.exception("InternalServiceErrorException - an error occurred on the server side.")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            log.exception("InvalidParameterException - the parameter " + secret_arn + " is an invalid value for the parameter .")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            log.exception("InvalidRequestException - the parameter " + secret_arn + " is not valid for the current state of the resource.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            log.exception("ResourceNotFoundException - the resource " + secret_arn + " does not exist.")
        elif e.response['Error']['Code'] == 'AccessDeniedException':
            log.exception("AccessDeniedException - this user does not have access to the resource " + secret_arn + ".")
        else:
            log.exception("Unexpected error: Unknown error.  Raised: " + str(e))
    else:
        if 'SecretString' in get_secret_value_response:
            out_dict = eval(get_secret_value_response['SecretString'])
            log.info(f'Secret retrieved for arn: {secret_arn}')
            return out_dict

def send_hst_msg(config, subject, message):
    session = boto3.Session(region_name=config.region_name)
    client = session.client('sns')

    hst_messaging_arn = config.hst_messaging_arn
    hst_message = {
        'application': config.application,
        'severity': config.severity,
        'environment': config.env,
        'action': 'open',
        'dscription': subject,
        'messageBody': message,
        'messageId': str(uuid.uuid4()),
        'source': config.source
    }

    hst_response = client.publish(
        TopicArn=hst_messaging_arn,
        Message=json.dumps(hst_message),
        MessageStructure='json'
    )

    return hst_response

def send_queue_msg(config, env, log):
    session = boto3.Session(region_name=config.region_name)
    client = session.client('sqs')

    message_args = {}

    message = {
        'application': config.application,
        'environment': env,
        'args': message_args
    }

    response = client.send_message(QueueUrl=config.queue_url, MessageBody=json.dumps(message))

    log.info(response['MessageId'])
    return response

def list_s3_objects(config, s3_resource, s3_bucket, s3_prefix, log):
    log.info(f'Listing all objects in {s3_bucket}/{s3_prefix}')

    paginator = s3_resource.meta.client.get_paginator('list_objects_v2')
    objects = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    key_lst = []
    for page in objects:
        log.debug(page)

        for obj in page.get('Contents', []):
            key_lst.append(obj['Key'])

    return key_lst

def write_s3_df_to_csv(config, log, s3_bucket, obj_key, df):
    s3_resource = boto3.resource('s3')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)

    s3_resource.Object(s3_bucket, obj_key).put(Body=csv_buffer.getvalue())

    return

def upload_obj_to_s3(log, s3_bucket, obj_key, file_obj):
    session = boto3.Session(region_name=config.region_name)
    s3_client = session.client('s3', region_name=config.region_name)

    try:
        log.info(f'Uploading {obj_key} to {s3_bucket}/{obj_key}')
        s3_client.upload_fileobj(file_obj, s3_bucket, obj_key)
        log.info(f'SUCCESS - Uploaded {obj_key} to {s3_bucket}/{obj_key}')
        return True
    except ClientError as e:
        log.error(f'ERROR - {e}')
        raise e

class AWSGeo(object):
    def __init__(self, logger, to_geocode_dict: dict):
        self.pre_geo_dict = to_geocode_dict
        self.post_geo_dict = {}
        self.geocoder = boto3.client('geocoder', region_name=config.region_name)
        self.logger = logger

    def call_geocode_api(self, address: str):
        return self.geocoder.geocode(QueryText=address, IntendedUse=config.intended_use)

    def process_addresses(self):
        for entity in self.pre_geo_dict:
            try:
                self.post_geo_dict['aws_result_items'] = self.call_geocode_api(str(entity))
            except Exception as e:
                self.logger.error(f'ERROR Geocoding failed - {e}')
                raise e