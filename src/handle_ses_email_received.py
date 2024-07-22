import base64
import json
import os
import pathlib
import traceback
import eml_parser

from utils import s3
from utils.dynamo import get_dynamo_client

TABLE_EVENT_NAME = os.environ.get('TABLE_EVENT_NAME', 'ses-event')
TABLE_EMAIL_SUPPRESSION_NAME = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME', 'ses-email-suppression')

# Inicializa el cliente DynamoDB y el cliente de SQS (para enviar eventos)
dynamodb = get_dynamo_client()
table_event = dynamodb.Table(TABLE_EVENT_NAME)
table_suppression = dynamodb.Table(TABLE_EMAIL_SUPPRESSION_NAME)

# Constants
TTL = int(os.environ.get('TTL', 525600))


def handler(message, context):
    records = message.get('Records', [])
    for record in records:
        try:
            procesar_record(record)
        except:
            traceback.print_exc()
            pass


def procesar_record(record):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)
    receipt = sqs_body.get('receipt')
    action = receipt.get('action')
    bucket_name = action.get('bucketName')
    object_key = action.get('objectKey')
    # download file from s3
    s3_response = s3.s3_get_object_bytes(f"{bucket_name}/{object_key}")
    file_bytes = s3_response[0]

    ep = eml_parser.EmlParser(include_attachment_data=True)
    m = ep.decode_email_bytes(file_bytes)
    out_path = pathlib.Path("/tmp")

    if 'attachment' in m:
        for a in m['attachment']:
            out_filepath = out_path / a['filename']

            print(f'\tWriting attachment: {out_filepath}')
            with out_filepath.open('wb') as a_out:
                a_out.write(base64.b64decode(a['raw']))
    print("Parseado", m)


if __name__ == "__main__":
    with open('/Users/claudiomiranda/IdeaProjects/amazon-ses-bounce-dashboard/events/email_received.json', 'r') as f:
        message = json.load(f)
        if 'Records' not in message:
            message = {
                "Records": [
                    {
                        "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
                        "receiptHandle": "MessageReceiptHandle",
                        "body": message,
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "SentTimestamp": "1523232000000",
                            "SenderId": "123456789012",
                            "ApproximateFirstReceiveTimestamp": "1523232000001"
                        },
                        "messageAttributes": {},
                        "md5OfBody": "{{{md5_of_body}}}",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
                        "awsRegion": "us-east-1"
                    }
                ]
            }

        handler(message, None)
