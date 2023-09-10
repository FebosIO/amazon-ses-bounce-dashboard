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
        url = host + '/' + table_event + '/' + datatype +"/"+new_record['id']
        response = requests.put(url, auth=awsauth, json=new_record, headers=headers)
        print(response.text)


if __name__ == '__main__':
    true = True
    json = {
        "Records": [
            {
                "eventID": "bb2c8adbb83927f1e64af16529654246",
                "eventName": "MODIFY",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1694205160.0,
                    "Keys": {
                        "id": {
                            "S": "920c0373233e5245e829b0c29f62ff71da4b"
                        }
                    },
                    "NewImage": {
                        "servicio": {
                            "S": "aprobaciones"
                        },
                        "proceso": {
                            "S": ""
                        },
                        "messageId": {
                            "S": "0100018a767f2afe-f58bd03e-f6bb-49eb-b030-055903c0e81a-000000"
                        },
                        "pais": {
                            "S": "chile"
                        },
                        "manifiesto": {
                            "S": "febos-io/chile/produccion/email/920c0373233e5245e829b0c29f62ff71da4b/920c0373233e5245e829b0c29f62ff71da4b.json"
                        },
                        "application": {
                            "S": "FEB"
                        },
                        "destinatarios": {
                            "L": [
                                {
                                    "S": "johanna.zuniga@gobiernovalparaiso.cl"
                                },
                                {
                                    "S": "juancarlos.cardenas@gobiernovalparaiso.cl"
                                }
                            ]
                        },
                        "stage": {
                            "S": "produccion"
                        },
                        "copias": {
                            "L": []
                        },
                        "domain": {
                            "S": "empresas.febos.cl"
                        },
                        "ConfigurationSetName": {
                            "S": "default"
                        },
                        "id": {
                            "S": "920c0373233e5245e829b0c29f62ff71da4b"
                        },
                        "documentoId": {
                            "S": "4360b28b20df724a4e29d5a258125f9a8dbe"
                        },
                        "empresa": {
                            "S": "72235100-2"
                        },
                        "timestamp": {
                            "S": "2023-09-08T20:32:39.861Z"
                        }
                    },
                    "OldImage": {
                        "servicio": {
                            "S": "aprobaciones"
                        },
                        "proceso": {
                            "S": ""
                        },
                        "messageId": {
                            "NULL": true
                        },
                        "pais": {
                            "S": "chile"
                        },
                        "manifiesto": {
                            "S": "febos-io/chile/produccion/email/920c0373233e5245e829b0c29f62ff71da4b/920c0373233e5245e829b0c29f62ff71da4b.json"
                        },
                        "application": {
                            "S": "FEB"
                        },
                        "destinatarios": {
                            "L": [
                                {
                                    "S": "johanna.zuniga@gobiernovalparaiso.cl"
                                },
                                {
                                    "S": "juancarlos.cardenas@gobiernovalparaiso.cl"
                                }
                            ]
                        },
                        "stage": {
                            "S": "produccion"
                        },
                        "copias": {
                            "L": []
                        },
                        "domain": {
                            "S": "empresas.febos.cl"
                        },
                        "ConfigurationSetName": {
                            "S": "default"
                        },
                        "id": {
                            "S": "920c0373233e5245e829b0c29f62ff71da4b"
                        },
                        "documentoId": {
                            "S": "4360b28b20df724a4e29d5a258125f9a8dbe"
                        },
                        "empresa": {
                            "S": "72235100-2"
                        },
                        "timestamp": {
                            "S": "2023-09-08T20:32:39.861Z"
                        }
                    },
                    "SequenceNumber": "203673600000000020074864356",
                    "SizeBytes": 1065,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "eventSourceARN": "arn:aws:dynamodb:us-east-1:830321976775:table/ses-email/stream/2023-09-08T20:28:26.191"
            }
        ]
    }
    handler(json, {})
