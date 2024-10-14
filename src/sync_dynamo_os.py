import json
import os
import traceback
from decimal import *

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


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # 👇️ if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            obj: Decimal
            if obj.is_normal():
                return int(obj.to_integral_exact())
            return str(obj)
        # 👇️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)


def handler(message, context):
    records = message['Records']
    try:
        for record in records:
            event_name = record['eventName']  # INSERT | MODIFY | REMOVE
            event_source_arn: str = record['eventSourceARN']
            dynamodb_object = record['dynamodb']

            table_event = event_source_arn.split("table/")[1].split("/")[0]
            keys = {k: deserializer.deserialize(v) for k, v in dynamodb_object['Keys'].items()}
            new_image = dynamodb_object.get('NewImage', dynamodb_object.get('OldImage'), {})
            new_record = {k: deserializer.deserialize(v) for k, v in new_image.items()}


            if 'id' in keys:
                id = keys['id']
            elif 'id' in new_record:
                id = new_record['id']
            else:
                id = keys[keys.keys()[0]]
            url = host + '/' + table_event + '/' + datatype + "/" + id



            # URL de objeto indexado
            if event_name == 'REMOVE':
                response = requests.delete(url, auth=awsauth, headers=headers)
                if response.status_code > 300 and response.status_code < 200:
                    print(response.text)
            else:
                json_str = json.dumps(new_record, cls=DecimalEncoder)
                response = requests.put(url, auth=awsauth, data=json_str, headers=headers)
                if response.status_code > 300 and response.status_code < 200:
                    print(response.text)
    except:
        traceback.print_exc()
        print(records)
        raise Exception("Something went wrong")
