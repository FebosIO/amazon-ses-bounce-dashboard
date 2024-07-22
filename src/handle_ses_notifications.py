import json
import os
import traceback
import uuid
from datetime import datetime, timedelta

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
    records = message.get('Records', [])
    for record in records:
        try:
            procesar_record(record)
        except:
            traceback.print_exc()
            pass


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


def procesar_record(record):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)

    message = sqs_body.get('Message', '{}')
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


async def procesar_eventos_suppression(type_, event_detail, mail, timestamp, message_id, company_id='0',
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


