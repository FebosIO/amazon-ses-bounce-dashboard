import email
import json
import logging
import os
import re
import traceback
import uuid
from email.header import decode_header

from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit
from boto3.dynamodb.conditions import Key
from dateutil import parser
from langdetect import detect

from utils import s3, sqs
from utils.dynamo import get_dynamo_client
from utils.events import send_event

STAGE = os.getenv("STAGE", "dev")
metrics = Metrics()

TABLE_EVENT_NAME = os.environ.get('TABLE_EVENT_NAME', 'ses-event')
TABLE_EMAIL_SUPPRESSION_NAME = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME', 'ses-email-suppression')
TABLE_EMAIL_RECEIVED_NAME = os.environ.get('TABLE_EMAIL_RECEIVED_NAME', 'ses-event-received')
TABLE_EMAIL_REFERENCES_NAME = os.environ.get('TABLE_EMAIL_REFERENCES_NAME', 'ses-event-references')
DEDUPLICATED_SQS_NAME = os.environ.get('DEDUPLICATED_SQS_NAME',
                                       'ses-event-manager-EmailNotificationDeduplicatedQueue-oGI4QbXoRVCP.fifo')
DEDUPLICATED_SQS_ARN = os.environ.get('DEDUPLICATED_SQS_ARN',
                                      'arn:aws:sqs:us-east-1:830321976775:ses-event-manager-EmailNotificationDeduplicatedQueue-oGI4QbXoRVCP.fifo')

# Inicializa el cliente DynamoDB y el cliente de SQS (para enviar eventos)
dynamodb = get_dynamo_client()
table_received = dynamodb.Table(TABLE_EMAIL_RECEIVED_NAME)
table_references = dynamodb.Table(TABLE_EMAIL_REFERENCES_NAME)

# Constants
TTL = int(os.environ.get('TTL', 525600))

logger = logging.getLogger()

# Patrones comunes en varios idiomas para firmas
signature_patterns = {
    'es': ['Enviado desde', 'Atentamente', 'Saludos', 'Cordialmente', '--'],
    'en': ['Sent from my', 'Best regards', 'Sincerely', 'Kind regards', '--'],
    'fr': ['Envoyé de', 'Cordialement', 'Sincèrement', 'Meilleures salutations', '-- '],
    'de': ['Gesendet von', 'Mit freundlichen Grüßen', 'Beste Grüße', 'Herzliche Grüße', '-- '],
    'pt': ['Enviado do meu', 'Atenciosamente', 'Cumprimentos', 'Saudações', '-- '],
    # Puedes añadir más idiomas y patrones comunes aquí
}


@metrics.log_metrics
def handler(message, context):
    return sqs.procesar_mensajes(message, procesar_record, context, deduplicate_fn=deduplicate_event)
    # return sqs.procesar_mensajes(message, procesar_record, context)


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


def clean_email_address(email_address):
    '''
    >>> clean_email_address("PROCESADORA Y COMERCIAL DE MINERALES Y GRANOS INDUSTRIALES MIGRIN SA <99572740-4@prd.inbox.febos.cl>")
    :param email_address:
    :return:
    '''
    try:
        # if email has <email> get only email
        if '<' in email_address and '>' in email_address:
            email_address = email_address[email_address.index('<') + 1:email_address.index('>')]
        return email_address
    except:
        return email_address


def deduplicate_event(record, context):
    source = record.get('eventSource', '')
    if source != DEDUPLICATED_SQS_ARN:
        sqs_body: dict = record.get('body', '')
        if isinstance(sqs_body, str):
            sqs_body = json.loads(sqs_body)
        mail = sqs_body.get('mail')
        common_headers = mail.get('commonHeaders')
        email_id = get_message_id(mail, common_headers, {})
        if len(email_id) > 128:
            email_id = email_id[:128]
        sqs_response = sqs.enviar_mensaje(DEDUPLICATED_SQS_NAME, sqs_body, MessageDeduplicationId=email_id)
        return True
    return False


def procesar_record(record, context):
    sqs_body: dict = record.get('body', '')
    if isinstance(sqs_body, str):
        sqs_body = json.loads(sqs_body)

    receipt = sqs_body.get('receipt')

    action = receipt.get('action')
    bucket_name = action.get('bucketName')
    object_key = action.get('objectKey')
    if 'AMAZON_SES_SETUP_NOTIFICATION' in object_key:
        return
    mail = sqs_body.get('mail')
    common_headers = mail.get('commonHeaders')
    headers = map_headers(mail.get('headers', []))
    subject = common_headers.get('subject')
    from_email = common_headers.get('from', headers.get('from'))
    if isinstance(from_email, str):
        from_email = from_email.split(',')[0]
    cc_email = common_headers.get('cc', [])
    bcc_email = common_headers.get('bcc', [])

    timestamp = receipt.get('timestamp')  # "2024-07-22T21:02:12.524Z"
    datetime_timestamp = parser.isoparse(timestamp)

    unix_timestamp = int(datetime_timestamp.timestamp())

    # download file from s3

    s3_response = s3.s3_get_object_bytes(f"{bucket_name}/{object_key}")
    file_bytes = s3_response[0]
    em = email.message_from_bytes(file_bytes)

    email_id = get_message_id(mail, common_headers, em)

    print(f"Processing email {email_id}")

    to_email, tos = get_destinations(common_headers, em, headers)
    references = headers.get('references', '').split(' ')
    references = [reference.replace("<", "").replace(">", "") for reference in references]

    clean_body, email_language, text, html = process_body(em, from_email)
    content = store_part(clean_body, "text/plain", bucket_name, object_key, "content.txt")
    text = store_part(text, "text/plain", bucket_name, object_key, "body.txt") if text else None
    html = store_part(html, "text/html", bucket_name, object_key, "body.html") if html else None

    attachments = process_attachments(bucket_name, em, object_key)
    num_attachments = len(attachments)
    attachments_size = sum([attachment['contentLength'] for attachment in attachments])
    has_attachments = len(attachments) > 0
    for email_address in to_email:
        metrics.add_dimension(name="to", value=clean_email_address(email_address))
        metrics.add_metric(name="EmailReceived", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="Attachments", unit=MetricUnit.Count, value=num_attachments)
        metrics.add_metric(name="AttachmentsSize", unit=MetricUnit.Count, value=attachments_size)

    save_data = {
        'id': email_id,
        'timestamp': timestamp,
        'subject': subject,
        'from': from_email,
        'to': to_email,
        'tos': tos,
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
        'references': references,
        "object_key": object_key

    }
    table_received.put_item(Item=save_data)
    if 'references' in save_data:
        del save_data['references']
    if 'attachments' in save_data:
        del save_data['attachments']
    send_event('email-received', save_data)
    references_data = []
    for reference in references:
        if reference and str(reference).strip():
            references_data.append({
                'id': email_id,
                'reference': reference,
                'ttl': unix_timestamp + TTL,
            })
    if len(references_data) > 0:
        # batch put item
        with table_references.batch_writer() as batch:
            for reference in references_data:
                batch.put_item(Item=reference)


def get_destinations(common_headers, em, headers):
    to_email = common_headers.get('to', headers.get('to'))
    if isinstance(to_email, str):
        to_email = to_email.split(",")
    tos = None
    try:
        received = em.get('received', headers.get('received'''))
        matchs = re.findall(r'for(.*);', received)
        receibed_email = matchs[0] if received else None
        if receibed_email and '@' in receibed_email and (
                not to_email or receibed_email not in to_email):  # posiblemente una redireccion o un grupo de google
            tos = to_email
            to_email = [receibed_email.strip()]
            print("new to", receibed_email.strip())
    except:
        traceback.print_exc()
    to_email = [clean_email_address(email_address) for email_address in to_email]
    return to_email, tos


def get_message_id(mail, common_headers, em):
    message_id = common_headers.get('messageId')
    if not message_id:
        message_id = mail.get('messageId')
    if not message_id:
        message_id = em.get('Message-ID')
    if not message_id:
        message_id = str(uuid.uuid4())
    return message_id.replace("<", "").replace(">", "").strip()


def remove_signature_by_patterns(body, language):
    """ Elimina la firma usando patrones específicos según el idioma detectado. """
    pattern_signature = None
    patterns = signature_patterns.get(language, [])
    lines = body.splitlines()
    for i, line in enumerate(lines):
        for pattern in patterns:
            try:
                if pattern in body and line.strip().startswith(pattern):
                    body = body.split(line)[0].strip()  # Elimina desde el patrón en adelante
                    pattern_signature = pattern
                elif pattern in body and line.strip().__eq__(pattern.strip()):
                    body = body.split(line)[0].strip()  # Elimina desde el patrón en adelante
                    pattern_signature = pattern
            except:
                pass

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
        else:
            continue
        content = part.get_payload(decode=True)
        if content_type == 'message/rfc822':
            content = part.get_payload(decode=False)[0].as_string()

        # si el conent es bytes pasar a string
        if filename and content:
            if charset:
                try:
                    try:
                        content = content.decode(charset)
                    except LookupError as e:
                        content = content.decode('utf-8')
                        charset = 'utf-8'
                except UnicodeDecodeError as e:
                    logger.error(f"Error decoding bytes to string: {e}")
                    posibles_encode = ['latin1', 'utf-8', 'ascii']
                    encontrado = False
                    while not encontrado and len(posibles_encode) > 0:
                        try:
                            content = content.decode(posibles_encode.pop(0))
                            encontrado = True
                        except UnicodeDecodeError:
                            pass
            try:
                if isinstance(content, bytes):
                    content = content.decode(charset)
            except:
                pass

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
    if not content:
        content = ""
    clean_body = content
    if isinstance(clean_body, bytes):
        try:
            clean_body = clean_body.decode('utf-8')
        except UnicodeDecodeError as e:
            logger.error(f"Error decoding bytes to string: {e}")
            clean_body = clean_body.decode('utf-8', errors='replace')
    # replace excesive salto de linea
    clean_body = re.sub(r'\n\r', '\n', clean_body)
    clean_body = re.sub(r'\r\n', '\n', clean_body)
    clean_body = re.sub(r'\n{2,}', '\n', clean_body)
    # clean_body = quotations.extract_from(clean_body)
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
            filename = decode_mime_words(filename)
            # replace all \n and \r in filename
            filename = filename.replace("\n", " ").replace("\r", "")
            filename = filename.replace("\\", "/")
            filename = filename.replace("//", "/")
            # if filename has / get only file name
            if '/' in filename:
                filename = filename.split('/')[-1]

            # decode the content based on the character set specified
            # TODO: add error handling
            if charset:
                try:
                    content = content.decode(charset)
                except Exception as e:
                    logger.error(f"Error decoding bytes to string: {e}")
            # store the decoded MIME part in S3 with the filename appended to the object key
            id = str(uuid.uuid4())
            file_key: str = object_key + "/" + filename
            # replace // by /
            file_key = file_key.replace("//", "/")
            params = {
                'Bucket': bucket_name,
                'Key': file_key,
                'Body': content
            }
            put_response = s3.put_object(
                **params
            )
            etag = put_response['ETag']
            version = put_response['VersionId']
            content_length = len(content)
            attachments.append({
                "id": id,
                "key": file_key,
                "name": filename,
                "contentType": content_type,
                "contentDisposition": content_disposition,
                "charset": charset,
                "contentLength": content_length,
                "etag": etag,
                "version": version
            })
            logger.info(
                f"Part {part_idx}: Content type: {content_type}. Content disposition: {content_disposition} stored in {file_key}.")
        else:
            logger.debug(
                f"Part ({part_idx}): has no content. Content type: {content_type}. Content disposition: {content_disposition}.")
    return attachments


def decode_mime_words(encoded_text):
    try:
        """ Decodifica el texto MIME en UTF-8 """
        decoded_fragments = decode_header(encoded_text)
        decoded_string = ''
        for fragment, encoding in decoded_fragments:
            if isinstance(fragment, bytes):
                decoded_string += fragment.decode(encoding or 'utf-8')
            else:
                decoded_string += fragment
        return decoded_string
    except:
        traceback.print_exc()
        return encoded_text


def get_headers(em):
    headers = {}
    for k, v in em.items():
        headers[k.lower()] = v
    return headers


def procesar_recibidos(recibidos):
    bucket_name = 'febos-io'
    for recibido in recibidos:
        try:
            object_key = recibido['object_key']
            id = recibido['id']
            print(id)
            if 'produccion' in object_key.lower() or 'prod' in object_key.lower():
                to = recibido.get('to', [])
                attachments = recibido.get('attachments', [])
                # if any to end with @febos.cl
                # if any(['febos.' not in x.lower() for x in to]):
                if (
                        True
                        or any(['<' in x.lower() for x in to])
                        or any(['//' in x['name'].lower() for x in attachments])
                        or any(['\\' in x['name'].lower() for x in attachments])
                        or any(['\n' in x['name'].lower() for x in attachments])
                        or any(['\r' in x['name'].lower() for x in attachments])
                ):
                    # corregir to guadar y lanzar evento
                    print(to)
                    s3_response = s3.s3_get_object_bytes(f"{bucket_name}/{object_key}")
                    file_bytes = s3_response[0]
                    em = email.message_from_bytes(file_bytes)
                    try:
                        headers = get_headers(em)
                        to_email, tos = get_destinations(headers, em, headers)
                        attachments = process_attachments(bucket_name, em, object_key)

                        print("new to", to_email, tos)
                        recibido['to'] = to_email
                        recibido['tos'] = tos
                        # update dynamo by id and timestamp
                        update_response = table_received.update_item(
                            Key={
                                'id': recibido['id'],
                                'timestamp': recibido['timestamp']
                            },
                            UpdateExpression='SET #to = :val0, #tos = :val1, #att = :val2',
                            ExpressionAttributeValues={
                                ':val2': attachments,
                                ':val0': to_email,
                                ':val1': to
                            },
                            ExpressionAttributeNames={
                                '#to': 'to',
                                '#tos': 'tos',
                                '#att': 'attachments'
                            }
                        )
                        print(update_response)
                        # send_event('email-received', recibido)
                    except:
                        traceback.print_exc()
        except:
            traceback.print_exc()
        finally:
            print(recibido)


if __name__ == '__main__2':
    find_id = "a3ddf532b5ba4a057364866e2b4d04b2@copeval.cl"
    find_id = "230f9a25f39aa7460b416ae3336d90b8@copeval.cl"
    find_id = "98e26d3abd7d69602ade4b7100cdc092@copeval.cl"
    find_id = "3a1c9bc50370494784277db9a9334189@justtime-erp.cl"
    find_id = "21b1f7ea96ed472a88d86cf3f6fa6a52@justtime-erp.cl"
    find_id = "241411697.41728301007476.JavaMail.root@soap-sdk"
    params = {
        'KeyConditionExpression': Key('id').eq(find_id)
    }
    # find by id
    recibidos_response = table_received.query(**params)
    # load all email received from dynamodb
    # recibidos_response = table_received.scan(**params)
    last_evaluated_key = 'A'
    recibidos = recibidos_response['Items']
    last_evaluated_key = recibidos_response.get('LastEvaluatedKey', None)
    while len(recibidos) > 0:
        procesar_recibidos(recibidos)
        recibidos = []
        if last_evaluated_key:
            recibidos_response = table_received.scan(ExclusiveStartKey=last_evaluated_key)
            last_evaluated_key = recibidos_response.get('LastEvaluatedKey', None)
            recibidos = recibidos_response.get('Items', [])

if __name__ == "__main__":
    with open('/Users/claudiomiranda/IdeaProjects/amazon-ses-bounce-dashboard/events/email_received.json', 'r') as f:
        message = json.load(f)
        if 'Records' not in message:
            message = {
                "Records": [
                    {'messageId': '6f92e936-3383-4493-b3b3-e9a87a96c71c',
                     'receiptHandle': 'AQEBnuDR6UR4MANWNf3mv5Mj6G/VbNseyypX043UU6/2RgHkkzs1/StLF83Hbl0sSnk3nBYi9Jq031EXiXn0MmzYJtADuKo04PM8UgIq4l2r6zlXLYIbCnuBIglbEGDsPbkZpQKrvf/ram5k2tyxgaxpizyuWuGQYig+z606+kRAc784FyKU3q+vQhuMD7GW3h/zEOADrLrfeXRrskdLu3sXxjjjlsBXDbRK2D8AVk/kbVnOOZqGLc5DL/q1RlJuWnBpGshoCS4jzhnAFf3808b3ciP2H21Xd6LXoGKwm3sQCI1HSVbDfgQqEVulqmrzf/6hMaekVq2Q/QR4/BsEyKIVp+ORLfr9QZsDfk8QPLQcfxeQhpMy5CO8FzQ+hF8WYaBfcnh2x656H4M9rNQyXLEtOOAwOxDGRM34LB4/LPUabZanpkKSLeiEUhvaif4Zj9P5',
                     'body': message,
                     'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1728065849741',
                                    'SenderId': 'AIDA4CUYL4XDUYJU3T2TZ',
                                    'ApproximateFirstReceiveTimestamp': '1728065849742'}, 'messageAttributes': {},
                     'md5OfBody': '20e30c56eba44bef1df349af55c5a12b', 'eventSource': 'aws:sqs',
                     'eventSourceARN': 'arn:aws:sqs:us-east-1:830321976775:ses-event-manager-EmailNotificationQueue-PzbmPHPzbGTO',
                     'awsRegion': 'us-east-1'}
                ]
            }
        logger.setLevel(logging.NOTSET)


        class Contexto:
            aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_group_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            log_stream_name = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

            def __int__(self):
                self.aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


        print(json.dumps(message))
        handler(message, Contexto())
