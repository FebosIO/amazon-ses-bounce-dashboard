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
        event_name = record['eventName']  # INSERT | MODIFY | REMOVE
        event_source_arn: str = record['eventSourceARN']
        dynamodb_object = record['dynamodb']

        table_event = event_source_arn.split("table/")[1].split("/")[0]
        keys = {k: deserializer.deserialize(v) for k, v in dynamodb_object['Keys'].items()}
        # Id de objeto a sincronizar
        id = keys['id']
        # URL de objeto indexado
        url = host + '/' + table_event + '/' + datatype + "/" + id
        if event_name == 'REMOVE':
            response = requests.delete(url, auth=awsauth, headers=headers)
            if response.status_code > 300 and response.status_code < 200:
                print(response.text)
        else:
            new_image = dynamodb_object['NewImage']
            new_record = {k: deserializer.deserialize(v) for k, v in new_image.items()}
            response = requests.put(url, auth=awsauth, json=new_record, headers=headers)
            if response.status_code > 300 and response.status_code < 200:
                print(response.text)
