import os

import boto3
import requests
from requests_aws4auth import AWS4Auth

# Dynamo config
boto3.resource('dynamodb')
deserializer = boto3.dynamodb.types.TypeDeserializer()

# Open-Search config
OPEN_SEARCH_HOST = os.environ.get('OPEN_SEARCH_HOST') or 'https://search.escritoriodigital.cl'
region = os.environ.get('OPEN_SEARCH_HOST') or 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
host = OPEN_SEARCH_HOST  # the OpenSearch Service domain, e.g. https://search-mydomain.us-west-1.es.amazonaws.com
datatype = '_doc'
headers = {"Content-Type": "application/json"}


def handler(message, context):
    records = message['Records']
    for record in records:
        event_source_arn: str = record['eventSourceARN']
        dynamodb_object = record['dynamodb']
        new_image = dynamodb_object['NewImage']
        new_record = {k: deserializer.deserialize(v) for k, v in new_image.items()}
        table_event = event_source_arn.split("table/")[1].split("/")[0]
        print(table_event, new_record)
        url = host + '/' + table_event + '/' + datatype
        response = requests.post(url, auth=awsauth, json=new_record, headers=headers)
        print(response.text)
