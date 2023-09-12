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
                json_str = json.dumps(new_record, cls=DecimalEncoder)
                response = requests.put(url, auth=awsauth, data=json_str, headers=headers)
                if response.status_code > 300 and response.status_code < 200:
                    print(response.text)
    except:
        traceback.print_exc()
        print(records)


if __name__ == '__main__':
    data = [{'eventID': '425fa13ca5bcee37fc5316400f7483cc', 'eventName': 'INSERT', 'eventVersion': '1.1',
             'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
             'dynamodb': {'ApproximateCreationDateTime': 1694530154.0,
                          'Keys': {'id': {'S': '0100018a89de2a4b-cd076f3b-ce23-44b1-ad49-7c67d7a22f06-000000'},
                                   'timestamp': {'S': '2023-09-12T14:49:14.124Z'}},
                          'NewImage': {'estado': {'S': 'queued'}, 'mail': {
                              'M': {'sourceArn': {'S': 'arn:aws:ses:us-east-1:830321976775:identity/phoqo.cl'},
                                    'headers': {'L': [{'M': {'name': {'S': 'From'},
                                                             'value': {'S': 'Informaciones <information@phoqo.cl>'}}}, {
                                                          'M': {'name': {'S': 'To'},
                                                                'value': {'S': 'MARIOLABORDA@GMAIL.COM'}}}, {
                                                          'M': {'name': {'S': 'Subject'}, 'value': {
                                                              'S': 'Hemos recibido la orden de trabajo  809'}}},
                                                      {'M': {'name': {'S': 'MIME-Version'}, 'value': {'S': '1.0'}}}, {
                                                          'M': {'name': {'S': 'Content-Type'}, 'value': {
                                                              'S': 'multipart/alternative;  boundary="----=_Part_1213840_1319207022.1694530153040"'}}}]},
                                    'sendingAccountId': {'S': '830321976775'},
                                    'destination': {'L': [{'S': 'MARIOLABORDA@GMAIL.COM'}]},
                                    'headersTruncated': {'BOOL': False},
                                    'messageId': {'S': '0100018a89de2a4b-cd076f3b-ce23-44b1-ad49-7c67d7a22f06-000000'},
                                    'source': {'S': 'Informaciones <information@phoqo.cl>'},
                                    'timestamp': {'S': '2023-09-12T14:49:13.035Z'}, 'commonHeaders': {
                                      'M': {'subject': {'S': 'Hemos recibido la orden de trabajo  809'}, 'messageId': {
                                          'S': '0100018a89de2a4b-cd076f3b-ce23-44b1-ad49-7c67d7a22f06-000000'},
                                            'from': {'L': [{'S': 'Informaciones <information@phoqo.cl>'}]},
                                            'to': {'L': [{'S': 'MARIOLABORDA@GMAIL.COM'}]}}}, 'tags': {
                                      'M': {'ses:operation': {'L': [{'S': 'SendEmail'}]},
                                            'ses:configuration-set': {'L': [{'S': 'default'}]},
                                            'ses:source-ip': {'L': [{'S': '54.85.238.210'}]},
                                            'ses:from-domain': {'L': [{'S': 'phoqo.cl'}]},
                                            'ses:caller-identity': {'L': [{'S': 'lambda-vpc-execution-role'}]},
                                            'ses:outgoing-ip': {'L': [{'S': '54.240.65.165'}]}}}}},
                                       'expiration': {'N': '1726066154'},
                                       'id': {'S': '0100018a89de2a4b-cd076f3b-ce23-44b1-ad49-7c67d7a22f06-000000'},
                                       'event': {'M': {'smtpResponse': {
                                           'S': '250 2.0.0 OK  1694530154 qd13-20020a05620a658d00b0076db2d9ae71si6011671qkn.257 - gsmtp'},
                                                       'processingTimeMillis': {'N': '1089'},
                                                       'recipients': {'L': [{'S': 'MARIOLABORDA@GMAIL.COM'}]},
                                                       'reportingMTA': {'S': 'a65-165.smtp-out.amazonses.com'},
                                                       'timestamp': {'S': '2023-09-12T14:49:14.124Z'}}},
                                       'type': {'S': 'Delivery'}, 'timestamp': {'S': '2023-09-12T14:49:14.124Z'}},
                          'SequenceNumber': '223303100000000019669048880', 'SizeBytes': 1494,
                          'StreamViewType': 'NEW_IMAGE'},
             'eventSourceARN': 'arn:aws:dynamodb:us-east-1:830321976775:table/ses-event/stream/2023-09-12T14:32:55.157'}]
    handler({'Records':data}, {})
