import boto3
from botocore.exceptions import ClientError
import json
import uuid
from io import StringIO

def get_secret(secret_arn, config, logger):
    session = boto3.Session(region_name=config.region_name)
    client = session.client(
        service_name='secretsmanager',
        region_name=config.region_name
    )

    get_secret_value_response = {}

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_arn
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("DecryptionFailureException - Secrets Manager can't decrypt the secret using the provided KMS key")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side.")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("InvalidParameterException - Parameter value is invalid.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("InvalidRequestException - Parameter value is invalid for the current state of the resource.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("ResourceNotFoundException - The specified resource does not exist or cannot be found.")
    else:
        meta = get_secret_value_response['ResponseMetadata']
        logger.exception(f'''SecretString Not found in API response\nResponseMetadata:\n{meta}\n''')

def send_hst_msg(config, subject, message):
    session = boto3.Session(region_name=config.region_name)
    client = session.client('sns')

    hst_messaging_arn = config.hst_messaging_arn
    hst_message = {
        'application': config.application,
        'severity': config.severity,
        'environment': config.environment,
        'action': "open",
        'description': subject,
        'messageBody': message,
        'messageId': str(uuid.uuid4()),
        'source': f"{config.env} - {config.application}"
    }

    hst_response = client.publish(
        TopicArn=hst_messaging_arn,
        Message=json.dumps({'default': json.dumps(hst_message)}),
        MessageStructure='json'
    )

    return hst_response

def send_hyper_msg(config, table, logger):
    session = boto3.Session(region_name=config.region_name)
    client = session.client('sqs')

    # populate sqs args as needed
    sqs_args = {}

    message = {
        'environment': config.environment,
        'sqs_args': sqs_args
    }

    response = client.send_message(
        QueueUrl=config.queue_url,
        MessageBody=json.dumps(message),
        DelaySeconds=config.send_delay
    )

    logger.info(response['MessageId'])
    return response

def list_s3_objects(config, logger, s3_resource, s3_bucket, s3_prefix):
    logger.info(f'Listing all objects  in {s3_bucket}/{s3_prefix}')

    paginator = s3_resource.meta.client.get_paginator('list_objects_v2')
    objects = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    key_list = []

    for page in objects:
        logger.debug(page)

        for obj in page.get('Contents', []):
            key_list.append(obj['Key'])

    return key_list

def retrieve_s3_objects(config, logger, s3_bucket, obj_key):
    s3 = boto3.resource('s3')

    content_object = s3.Object(s3_bucket, obj_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')

    return file_content

def write_s3_df_to_csv(config, logger, s3_bucket, obj_key, df):
    s3 = boto3.resource('s3')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)

    s3.Object(s3_bucket, obj_key).put(Body=csv_buffer.getvalue())

    return

def upload_obj_to_s3(config, logger, s3_bucket, obj_key, file_obj):
    session = boto3.Session(region_name=config.region_name)
    client = session.client('s3')

    try:
        logger.info(f'Uploading {obj_key} to {s3_bucket}/{obj_key}')
        client.upload_fileobj(file_obj, s3_bucket, obj_key)
        logger.info(f'SUCCESS - Uploaded {obj_key} to {s3_bucket}/{obj_key}')
        return True
    except ClientError as e:
        logger.error(e)
        raise e

class AWSGeo(object):
    def __init__(self, logger, to_geo: dict):
        self.pre_geo_dict = to_geo
        self.post_geo_dict = {}
        self.geocoder = boto3.client('geocoder')
        self.logger = logger

    def call_places_geocode_api(self, address:str):
        return self.geocoder.geocode(QueryText=address, IntendedUse='Storage')

    def process_addresses(self):
        for entity in self.pre_geo_dict.keys():
            try:
                self.post_geo_dict['aws_result_items'] = self.call_places_geocode_api(str(entity))
            except Exception as e:
                err_msg = (f'Address Geocoding failed: \n{e}')
                self.logger.error(err_msg)
                raise e
