import email
import json
import logging
import os
import re

from dateutil import parser
from langdetect import detect
from talon import quotations

from utils import s3, sqs
from utils.dynamo import get_dynamo_client
from utils.events import send_event

TABLE_EVENT_NAME = os.environ.get('TABLE_EVENT_NAME', 'ses-event')
TABLE_EMAIL_SUPPRESSION_NAME = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME', 'ses-email-suppression')
TABLE_EMAIL_RECEIVED_NAME = os.environ.get('TABLE_EMAIL_RECEIVED_NAME', 'ses-event-received')
TABLE_EMAIL_REFERENCES_NAME = os.environ.get('TABLE_EMAIL_REFERENCES_NAME', 'ses-event-references')

# Inicializa el cliente DynamoDB y el cliente de SQS (para enviar eventos)
dynamodb = get_dynamo_client()
table_received = dynamodb.Table(TABLE_EMAIL_RECEIVED_NAME)
table_references = dynamodb.Table(TABLE_EMAIL_REFERENCES_NAME)

# Constants
TTL = int(os.environ.get('TTL', 525600))

logger = logging.getLogger()


def handler(message, context):
    return sqs.procesar_mensajes(message, procesar_record, context)


def map_headers(headers_list):
    headers = {}
    for header in headers_list:
        name: str = header.get('name')
        name = name.lower()
        headers[name] = header.get('value')
    return headers


def store_part(content, content_type, bucket_name, object_key, filename):
    file_key = object_key + "/" + filename
    params = {
        'Bucket': bucket_name,
        'Key': file_key,
        'Body': content
    }
    put_response = s3.put_object(
        **params
    )
    content_length = len(content)
    return {
        "key": file_key,
        "contentType": content_type,
        "contentLength": content_length
    }


def procesar_record(record, context):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)
    mail = sqs_body.get('mail')
    common_headers = mail.get('commonHeaders')
    headers = map_headers(mail.get('headers', []))
    email_id = common_headers.get('messageId').replace("<", "").replace(">", "")
    subject = common_headers.get('subject')
    from_email = common_headers.get('from')[0]
    to_email = common_headers.get('to')
    cc_email = common_headers.get('cc', [])
    bcc_email = common_headers.get('bcc', [])

    receipt = sqs_body.get('receipt')

    timestamp = receipt.get('timestamp')  # "2024-07-22T21:02:12.524Z"
    datetime_timestamp = parser.isoparse(timestamp)

    unix_timestamp = int(datetime_timestamp.timestamp())

    action = receipt.get('action')
    bucket_name = action.get('bucketName')
    object_key = action.get('objectKey')
    # download file from s3
    s3_response = s3.s3_get_object_bytes(f"{bucket_name}/{object_key}")
    file_bytes = s3_response[0]
    em = email.message_from_bytes(file_bytes)

    references = headers.get('references', '').split(' ')
    references = [reference.replace("<", "").replace(">", "") for reference in references]

    attachments = process_attachments(bucket_name, em, object_key)
    clean_body, email_language, text, html = process_body(em, from_email)
    content = store_part(clean_body, "text/plain", bucket_name, object_key, "body.txt")
    text = store_part(text, "text/plain", bucket_name, object_key, "body.txt") if text else None
    html = store_part(html, "text/html", bucket_name, object_key, "body.txt") if html else None

    print(f"Saved {len(attachments)} parts")

    has_attachments = len(attachments) > 0

    save_data = {
        'id': email_id,
        'timestamp': timestamp,
        'subject': subject,
        'from': from_email,
        'to': to_email,
        'cc': cc_email,
        'bcc': bcc_email,
        'language': email_language,
        'content': content,
        'text': text,
        'html': html,
        'has_attachments': has_attachments,
        'attachments': attachments,
        'ttl': unix_timestamp + TTL,
        'requestId': context.aws_request_id,
        'references': references

    }
    table_received.put_item(Item=save_data)
    if 'references' in save_data:
        del save_data['references']
    if 'attachments' in save_data:
        del save_data['attachments']
    send_event('email-received', save_data)
    references_data = []
    for reference in references:
        references_data.append({
            'id': email_id,
            'reference': reference,
            'ttl': unix_timestamp + TTL,
        })
    # batch put item
    with table_references.batch_writer() as batch:
        for reference in references_data:
            batch.put_item(Item=reference)


# Patrones comunes en varios idiomas para firmas
signature_patterns = {
    'es': ['-- ', 'Enviado desde', 'Atentamente', 'Saludos', 'Cordialmente'],
    'en': ['-- ', 'Sent from my', 'Best regards', 'Sincerely', 'Kind regards'],
    'fr': ['-- ', 'Envoyé de', 'Cordialement', 'Sincèrement', 'Meilleures salutations'],
    'de': ['-- ', 'Gesendet von', 'Mit freundlichen Grüßen', 'Beste Grüße', 'Herzliche Grüße'],
    'pt': ['-- ', 'Enviado do meu', 'Atenciosamente', 'Cumprimentos', 'Saudações'],
    # Puedes añadir más idiomas y patrones comunes aquí
}


def remove_signature_by_patterns(body, language):
    """ Elimina la firma usando patrones específicos según el idioma detectado. """
    pattern_signature = None
    patterns = signature_patterns.get(language, [])
    lines = body.splitlines()
    for i, line in enumerate(lines):
        for pattern in patterns:
            if pattern in body and line.strip().startswith(pattern):
                body = body.split(line)[0]  # Elimina desde el patrón en adelante
                pattern_signature = pattern

    return body.strip(), pattern_signature


def process_body(em, from_email):
    parts = em.walk()
    part_idx = 0
    attachments = []
    html = None
    text = None
    for part in parts:
        part_idx += 1
        filename = part.get_filename()
        content_type = part.get_content_type()
        content_disposition = str(part.get_content_disposition())
        content = part.get_payload(decode=True)
        if content_type == 'message/rfc822':
            content = part.get_payload(decode=False)[0].as_string()
        charset = part.get_content_charset()
        logger.debug(
            f"Part: {part_idx}. Content charset: {charset}. Content type: {content_type}. Content disposition: {content_disposition}. Filename: {filename}");
        if not filename:
            if content_type == 'text/plain':
                if 'attachment' not in content_disposition:
                    filename = "body.txt"
                else:
                    continue
            elif content_type == 'text/html':
                if 'attachment' not in content_disposition:
                    filename = "body.html"
                else:
                    continue
            else:
                continue
        if filename and content:
            if charset:
                content = content.decode(charset)
            if content_type == 'text/html':
                html = content
            else:
                text = content
            # store the decoded MIME part in S3 with the filename appended to the object key
            content_length = len(content)
            attachments.append({
                "contentType": content_type,
                "contentDisposition": content_disposition,
                "charset": charset,
                "contentLength": content_length,
                "content": content
            })
            logger.info(f"Part {part_idx}: Content type: {content_type}. Content disposition: {content_disposition}.")
        else:
            logger.error(
                f"Part ({part_idx}): has no content. Content type: {content_type}. Content disposition: {content_disposition}.")
    try:
        email_language = detect(text)
    except:
        email_language = 'es'
    content = text or html
    clean_body = content
    # replace excesive salto de linea
    clean_body = re.sub(r'\n\r', '\n', clean_body)
    clean_body = re.sub(r'\r\n', '\n', clean_body)
    clean_body = re.sub(r'\n{2,}', '\n', clean_body)
    clean_body = quotations.extract_from(content)
    clean_body, pattern_signature = remove_signature_by_patterns(content, email_language)
    return clean_body, email_language, text, html


def process_attachments(bucket_name, em, object_key):
    parts = em.walk()
    part_idx = 0
    attachments = []
    for part in parts:
        part_idx += 1

        # get information about the MIME part
        content_type, content_disposition, content, charset, filename = [None] * 5
        filename = part.get_filename()

        content_type = part.get_content_type()
        content_disposition = str(part.get_content_disposition())
        content = part.get_payload(decode=True)
        if content_type == 'message/rfc822':
            content = part.get_payload(decode=False)[0].as_string()
        charset = part.get_content_charset()
        logger.debug(
            f"Part: {part_idx}. Content charset: {charset}. Content type: {content_type}. Content disposition: {content_disposition}. Filename: {filename}");

        # make file name for body, and untitled text or html parts
        # add additional content types that we want to support non-existent filenames
        if not filename:
            if content_type == 'text/plain':
                if 'attachment' not in content_disposition:
                    filename = "body.txt"
                    continue
                else:
                    filename = f"untitled_{part_idx}.txt"
            elif content_type == 'text/html':
                if 'attachment' not in content_disposition:
                    filename = "body.html"
                    continue
                else:
                    filename = f"untitled_{part_idx}.html"
            else:
                filename = None
        # TODO: consider overriding or sanitizing the filenames since that is tainted data and might be subject to abuse in object key names
        # technically, the entire message is tainted data, so it would be the responsibility of downstream parsers to ensure protection from interpreter abuse

        # skip parts that aren't attachment parts
        # if content_type in ["multipart/mixed", "multipart/related", "multipart/alternative"]:
        #     continue

        if filename and content:
            # decode the content based on the character set specified
            # TODO: add error handling
            if charset:
                content = content.decode(charset)
            # store the decoded MIME part in S3 with the filename appended to the object key
            file_key = object_key + "/" + filename
            params = {
                'Bucket': bucket_name,
                'Key': file_key,
                'Body': content
            }
            put_response = s3.put_object(
                **params
            )
            content_length = len(content)
            attachments.append({
                "key": file_key,
                "contentType": content_type,
                "contentDisposition": content_disposition,
                "charset": charset,
                "contentLength": content_length
            })
            logger.info(
                f"Part {part_idx}: Content type: {content_type}. Content disposition: {content_disposition} stored in {file_key}.")
        else:
            logger.error(
                f"Part ({part_idx}): has no content. Content type: {content_type}. Content disposition: {content_disposition}.")
    return attachments


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
        logger.setLevel(logging.NOTSET)


        class Contexto:
            aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_group_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_stream_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

            def __int__(self):
                self.aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


        handler(message, Contexto())
