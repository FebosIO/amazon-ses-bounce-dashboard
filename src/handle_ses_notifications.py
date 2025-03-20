import json
import os
import traceback
import uuid
from datetime import datetime, timedelta

from utils import sqs
from utils.dynamo import get_dynamo_client
from utils.events import send_event

TABLE_EVENT_NAME = os.environ.get('TABLE_EVENT_NAME', 'ses-event')
TABLE_EMAIL_SUPPRESSION_NAME = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME', 'ses-email-suppression')

# Inicializa el cliente DynamoDB y el cliente de SQS (para enviar eventos)
dynamodb = get_dynamo_client()
table_event = dynamodb.Table(TABLE_EVENT_NAME)
table_suppression = dynamodb.Table(TABLE_EMAIL_SUPPRESSION_NAME)

# Constants
TTL = int(os.environ.get('TTL', 525600))


def handler(message, context):
    return sqs.procesar_mensajes(message, procesar_record, context)


# Funciones utilitarias
def miliseconds_to_epoch(date):
    return int(date.timestamp() * 1000)


def sum_minutes_to_date_from_iso(date_str, ttl_minutes):
    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return date + timedelta(minutes=ttl_minutes)


def procesa_record(record):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)

    message = sqs_body.get('Message', '{}')
    if isinstance(message, str):
        message = json.loads(message)


def procesar_record(record, context):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)
    message = sqs_body.get('Message', sqs_body)
    if isinstance(message, str):
        message = json.loads(message)
    type_ = message['eventType']
    mail = message['mail']
    message_id = message['mail']['messageId']
    event_detail = message.get(type_.lower(), {})
    timestamp = event_detail.get('timestamp', datetime.utcnow().isoformat())
    expiration = miliseconds_to_epoch(sum_minutes_to_date_from_iso(timestamp, TTL))

    data = {
        'id': str(uuid.uuid4()),
        'messageId': message_id,
        'estado': 'queued',
        'timestamp': timestamp,
        'type': type_,
        'event': event_detail,
        'expiration': expiration,
        'mail': mail
    }
    table_event.put_item(Item=data)
    procesar_eventos_suppression(type_, event_detail, mail, timestamp, message_id,
                                 obtener_company_from_email_tags(mail), obtener_stage_from_email_tags(mail))


def obtener_stage_from_email_tags(mail):
    try:
        if 'stage' in mail['tags']:
            return ",".join(mail['tags']['stage'])
    except Exception as e:
        print(e)
    return 'produccion'


def obtener_company_from_email_tags(mail):
    try:
        if 'empresa' in mail['tags']:
            return ",".join(mail['tags']['empresa'])
    except Exception as e:
        print(e)
    return '0'


def procesar_eventos_suppression(type_, event_detail, mail, timestamp, message_id, company_id='0',
                                 stage='produccion'):
    if type_ == 'Bounce':
        recipients = event_detail['bouncedRecipients']
        for recipient in recipients:
            email_address = recipient['emailAddress']
            diagnostic_code = recipient.get('diagnosticCode', '')
            action = recipient.get('action', '')
            event = {
                'id': email_address,
                'emailAddress': email_address,
                'timestamp': timestamp,
                'type': type_,
                'stage': stage,
                'message': diagnostic_code or action,
                'messageId': message_id,
                'companyId': company_id
            }
            table_suppression.put_item(Item=event)
            send_event('email-suppression', event)

    elif type_ == 'Reject':
        reason = event_detail['reason']
        for destination in mail['destination']:
            event = {
                'id': destination,
                'emailAddress': destination,
                'timestamp': timestamp,
                'type': type_,
                'stage': stage,
                'message': reason,
                'messageId': message_id,
                'companyId': company_id
            }
            table_suppression.put_item(Item=event)
            send_event('email-suppression', event)

    elif type_ == 'Complaint':
        recipients = event_detail['complainedRecipients']
        diagnostic_code = event_detail.get('complaintFeedbackType', '')
        for recipient in recipients:
            email_address = recipient['emailAddress']
            event = {
                'id': email_address,
                'emailAddress': email_address,
                'timestamp': timestamp,
                'type': type_,
                'stage': stage,
                'message': diagnostic_code,
                'messageId': message_id,
                'companyId': company_id
            }
            table_suppression.put_item(Item=event)
            send_event('email-suppression', {
                'emailAddress': email_address,
                'companyId': company_id
            })


if __name__ == "__main__":
    with open('/Users/claudiomiranda/IdeaProjects/amazon-ses-bounce-dashboard/events/email_received.json', 'r') as f:
        message = json.load(f)
        if 'Records' not in message:
            message = {
                "Records": [
                    {
                        "messageId": "f49f17b1-dd05-41a6-8078-1d2f90f04303",
                        "receiptHandle": "AQEBeGF66n0QmhIQjN1ViHu4TXkyvnoUYWHqRVU0t+AF7GNNAslJOL9+yqHxNGX0betyYsJgixxVHu7Q1p9CChS7RVEmbLfOBebQx+MGsRwicqRCqLFNKAo/1ZsAlJyL6xPyOpNsPBVxDc3D/La7bTFcYdKv9HNZG0F55c9UJVkmaEW+wc9jV4PW+7IiIMya3k4GUCjWpdHRk3VjHIyQ+qJQfjDbjSoAFgUqjm7SPVmiHH0j9BF16GnusGOq9bJJC6Po3l3c9PUSoBGbZ4MUfoKvj/BOcd/k67tBsNwh3vY34yZjCpk+wq9XcThR+phdJF/LHcu5QBi1aE4EOFMjGgqZD9pv/5z60pfUVNKmiZjH2FGwBe2VfP9ajCsK1Y1Pqrul",
                        "body": "{\"eventType\":\"Open\",\"mail\":{\"timestamp\":\"2024-10-10T14:32:47.835Z\",\"source\":\"notificaciones@empresas.febos.cl\",\"sendingAccountId\":\"830321976775\",\"messageId\":\"0100019276d8b9db-68b3478b-d341-4361-9888-11e7d39ba92d-000000\",\"destination\":[\"jlmingo@interkambio.cl\"],\"headersTruncated\":false,\"headers\":[{\"name\":\"Content-Type\",\"value\":\"multipart/mixed; boundary=\\\"===============4188404272342821134==\\\"\"},{\"name\":\"MIME-Version\",\"value\":\"1.0\"},{\"name\":\"From\",\"value\":\"Febos <notificaciones@empresas.febos.cl>\"},{\"name\":\"Subject\",\"value\":\"Envío de DTE\"},{\"name\":\"To\",\"value\":\"jlmingo@interkambio.cl\"}],\"commonHeaders\":{\"from\":[\"Febos <notificaciones@empresas.febos.cl>\"],\"to\":[\"jlmingo@interkambio.cl\"],\"messageId\":\"0100019276d8b9db-68b3478b-d341-4361-9888-11e7d39ba92d-000000\",\"subject\":\"Envío de DTE\"},\"tags\":{\"ses:source-tls-version\":[\"TLSv1.3\"],\"ses:operation\":[\"SendRawEmail\"],\"ses:configuration-set\":[\"default\"],\"stage\":[\"produccion\"],\"ses:source-ip\":[\"3.230.171.119\"],\"ses:from-domain\":[\"empresas.febos.cl\"],\"ses:caller-identity\":[\"ses-event-manager-ProcessSESSendQueueRole-T37E7DP89RXL\"],\"empresa\":[\"85218700-K\"]}},\"open\":{\"timestamp\":\"2024-10-10T14:32:55.387Z\",\"userAgent\":\"Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko Firefox/11.0 (via ggpht.com GoogleImageProxy)\",\"ipAddress\":\"66.249.84.71\"}}\n",
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "SentTimestamp": "1728570775505",
                            "SenderId": "AIDAIT2UOQQY3AUEKVGXU",
                            "ApproximateFirstReceiveTimestamp": "1728570775510"
                        },
                        "messageAttributes": {},
                        "md5OfBody": "7610a5fcb32508ce19a1507802089e18",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:830321976775:ses-event",
                        "awsRegion": "us-east-1"
                    }
                ]
            }

        message['Records'][0]['body'] = {
            "id": "896f5694-a2f9-43aa-8b52-66b540cf0918",
            "timestamp": "2025-03-20T14:51:11.257Z",
            "estado": "queued",
            "bounce": {
                "timestamp": "2025-03-20T14:51:11.257Z",
                "bouncedRecipients": [
                    {
                        "action": "failed",
                        "diagnosticCode": "Amazon SES did not send the message to this address because it is on the suppression list for your account. For more information about removing addresses from the suppression list, see the Amazon SES Developer Guide at https://docs.aws.amazon.com/ses/latest/DeveloperGuide/sending-email-suppression-list.html",
                        "emailAddress": "valentin.solis@honorario.uaysen.cl",
                        "status": "5.1.1"
                    }
                ],
                "bounceSubType": "OnAccountSuppressionList",
                "bounceType": "Permanent",
                "feedbackId": "01000195b4096bdf-b25226b7-63a2-4c06-9770-e5a5f320cabf-000000",
                "reportingMTA": "dns; amazonses.com"
            },
            "expiration": 1774018271257,
            "mail": {
                "timestamp": "2025-03-20T14:51:10.894Z",
                "commonHeaders": {
                    "from": [
                        "Febos <informacion@empresas.febos.cl>"
                    ],
                    "messageId": "01000195b4096aae-eb9bcd3b-c8f9-46ac-baa2-5617f7ff634b-000000",
                    "subject": "Recuperación de contraseña",
                    "to": [
                        "valentin.solis@honorario.uaysen.cl"
                    ]
                },
                "destination": [
                    "valentin.solis@honorario.uaysen.cl"
                ],
                "headers": [
                    {
                        "name": "Content-Type",
                        "value": "multipart/mixed; boundary=\"===============8163948611933252407==\""
                    },
                    {
                        "name": "MIME-Version",
                        "value": "1.0"
                    },
                    {
                        "name": "From",
                        "value": "Febos <informacion@empresas.febos.cl>"
                    },
                    {
                        "name": "Subject",
                        "value": "Recuperación de contraseña"
                    },
                    {
                        "name": "To",
                        "value": "valentin.solis@honorario.uaysen.cl"
                    }
                ],
                "headersTruncated": False,
                "messageId": "01000195b4096aae-eb9bcd3b-c8f9-46ac-baa2-5617f7ff634b-000000",
                "sendingAccountId": "830321976775",
                "source": "informacion@empresas.febos.cl",
                "sourceArn": "arn:aws:ses:us-east-1:830321976775:identity/empresas.febos.cl",
                "tags": {
                    "empresa": [
                        "61980520-8"
                    ],
                    "ses:caller-identity": [
                        "ses-event-manager-ProcessSESSendQueueRole-T37E7DP89RXL"
                    ],
                    "ses:configuration-set": [
                        "default"
                    ],
                    "ses:from-domain": [
                        "empresas.febos.cl"
                    ],
                    "ses:operation": [
                        "SendRawEmail"
                    ],
                    "ses:source-ip": [
                        "44.204.40.111"
                    ],
                    "ses:source-tls-version": [
                        "TLSv1.3"
                    ],
                    "stage": [
                        "produccion"
                    ]
                }
            },
            "messageId": "01000195b4096aae-eb9bcd3b-c8f9-46ac-baa2-5617f7ff634b-000000",
            "eventType": "Bounce"
        }


        class Contexto:
            aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_group_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_stream_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

            def __int__(self):
                self.aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


        print(json.dumps(message))
        # handler(message, Contexto())
