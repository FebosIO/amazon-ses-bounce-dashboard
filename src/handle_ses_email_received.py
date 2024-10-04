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
    email_id = common_headers.get('messageId').replace("<", "").replace(">", "")

    print(f"Processing email {email_id}")

    subject = common_headers.get('subject')
    from_email = common_headers.get('from')[0]
    cc_email = common_headers.get('cc', [])
    bcc_email = common_headers.get('bcc', [])

    timestamp = receipt.get('timestamp')  # "2024-07-22T21:02:12.524Z"
    datetime_timestamp = parser.isoparse(timestamp)

    unix_timestamp = int(datetime_timestamp.timestamp())

    # download file from s3

    s3_response = s3.s3_get_object_bytes(f"{bucket_name}/{object_key}")
    file_bytes = s3_response[0]
    em = email.message_from_bytes(file_bytes)

    to_email = common_headers.get('to')
    if isinstance(to_email, str):
        to_email = [to_email]
    tos = None
    try:
        received = em.get('received', '')
        matchs = re.findall(r'for(.*);', received)
        receibed_email = matchs[0] if received else None
        if receibed_email and receibed_email not in to_email:  # posiblemente una redireccion o un grupo de google
            tos = to_email
            to_email = [receibed_email.strip()]
            print("new to", receibed_email.strip())
    except:
        traceback.print_exc()

    to_email = [clean_email_address(email_address) for email_address in to_email]
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
    metrics.add_metric(name="EmailReceived", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name="Attachments", unit=MetricUnit.Count, value=num_attachments)
    metrics.add_metric(name="AttachmentsSize", unit=MetricUnit.Count, value=attachments_size)
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
                content = content.decode(charset)
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
            # decode the content based on the character set specified
            # TODO: add error handling
            if charset:
                content = content.decode(charset)
            # store the decoded MIME part in S3 with the filename appended to the object key
            id = str(uuid.uuid4())
            file_key = object_key + "/" + filename
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
            logger.error(
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


if __name__ == "__main__":
    with open('/Users/claudiomiranda/IdeaProjects/amazon-ses-bounce-dashboard/events/email_received.json', 'r') as f:
        message = json.load(f)
        if 'Records' not in message:
            message = {
                "Records": [
                    {'messageId': '6f92e936-3383-4493-b3b3-e9a87a96c71c',
                     'receiptHandle': 'AQEBnuDR6UR4MANWNf3mv5Mj6G/VbNseyypX043UU6/2RgHkkzs1/StLF83Hbl0sSnk3nBYi9Jq031EXiXn0MmzYJtADuKo04PM8UgIq4l2r6zlXLYIbCnuBIglbEGDsPbkZpQKrvf/ram5k2tyxgaxpizyuWuGQYig+z606+kRAc784FyKU3q+vQhuMD7GW3h/zEOADrLrfeXRrskdLu3sXxjjjlsBXDbRK2D8AVk/kbVnOOZqGLc5DL/q1RlJuWnBpGshoCS4jzhnAFf3808b3ciP2H21Xd6LXoGKwm3sQCI1HSVbDfgQqEVulqmrzf/6hMaekVq2Q/QR4/BsEyKIVp+ORLfr9QZsDfk8QPLQcfxeQhpMy5CO8FzQ+hF8WYaBfcnh2x656H4M9rNQyXLEtOOAwOxDGRM34LB4/LPUabZanpkKSLeiEUhvaif4Zj9P5',
                     'body': '{"notificationType":"Received","mail":{"timestamp":"2024-10-04T16:31:14.331Z","source":"61102025-2+bncBAABBUNQQC4AMGQE7MD7I3Q@febos.cl","messageId":"bmql2tc8b4h90qte7gat7fdsq0opui6spk9o3fo1","destination":["61102025-2@febos.cl"],"headersTruncated":true,"headers":[{"name":"Return-Path","value":"<61102025-2+bncBAABBUNQQC4AMGQE7MD7I3Q@febos.cl>"},{"name":"Received","value":"from mail-oa1-f69.google.com (mail-oa1-f69.google.com [209.85.160.69]) by inbound-smtp.us-east-1.amazonaws.com with SMTP id bmql2tc8b4h90qte7gat7fdsq0opui6spk9o3fo1 for inbox@empresas.febos.io; Fri, 04 Oct 2024 16:31:14 +0000 (UTC)"},{"name":"X-SES-Spam-Verdict","value":"PASS"},{"name":"X-SES-Virus-Verdict","value":"PASS"},{"name":"Received-SPF","value":"pass (spfCheck: domain of febos.cl designates 209.85.160.69 as permitted sender) client-ip=209.85.160.69; envelope-from=61102025-2+bncBAABBUNQQC4AMGQE7MD7I3Q@febos.cl; helo=mail-oa1-f69.google.com;"},{"name":"Authentication-Results","value":"amazonses.com; spf=pass (spfCheck: domain of febos.cl designates 209.85.160.69 as permitted sender) client-ip=209.85.160.69; envelope-from=61102025-2+bncBAABBUNQQC4AMGQE7MD7I3Q@febos.cl; helo=mail-oa1-f69.google.com; dkim=permerror header.i=@febos-cl.20230601.gappssmtp.com; dmarc=pass header.from=febos.cl;"},{"name":"X-SES-RECEIPT","value":"AEFBQUFBQUFBQUFISVo5akxoaTljdmtYZFFaenJvTDhaemR2U1p5THFsdk1vNFovUVdmV2dKWWtFK0p6SmhTN2FsNDFta1o3QXUwMTIzdFlsNTZ6QVFmbmllSm5OR0g0cUs2aEpLNzMxb2tsY1MrRG10RW8wYzh4R2xvMURDeVJSeUMvZXQvZVlvbEJsaFZTTnU1MWtkK3NVSW50SjA1cU5tc1dFME9meUZJODlLVlk1dHU5V2Zpd3lYTktTWDZERkQ1UWZkdTZ3TDZqVW45VnNrNXdmc245K3JqbTNJZzAwbTZodkhOVnhhRGRmTFhiUXBMeUhtMmxtSnpUd1kvWHlwVWc0NWxqNzduTDdKMENCQklicFR0bVRFeUVXVzlIMHFmeE9kSEJIY0VoMnFmc042bDZlWHJ1NCtOYmlLQ2xRUngybFZ6UE5BSUEyYUtjQTNFNFYwZVVuTWJpYzh6NTg="},{"name":"X-SES-DKIM-SIGNATURE","value":"a=rsa-sha256; q=dns/txt; b=d1HMw6+CckEAbu6BKg1w0OWrwNnUgJL2M50N+5/OwXX3Mn2S4RnvEj9shI6+NOjIoAIutgErwLta2PTPSuAsIDraVqXo3Uc2OUU79WjqCs8mME8T9Rnci8cuxb4qXpbvA5UGk7PSMToGT17eIsVBsykPOv0boa+xR5Jv+RlPIOU=; c=relaxed/simple; s=ug7nbtf4gccmlpwj322ax3p6ow6yfsug; d=amazonses.com; t=1728059475; v=1; bh=pfuIBlmyiPy3fDj5YL1xIhQNQWhkRkrKTAUF7SHbPjM=; h=From:To:Cc:Bcc:Subject:Date:Message-ID:MIME-Version:Content-Type:X-SES-RECEIPT;"},{"name":"Received","value":"by mail-oa1-f69.google.com with SMTP id 586e51a60fabf-286fa354e34sf1892938fac.1 for <inbox@empresas.febos.io>; Fri, 04 Oct 2024 09:31:14 -0700 (PDT)"},{"name":"ARC-Seal","value":"i=2; a=rsa-sha256; t=1728059473; cv=pass; d=google.com; s=arc-20240605; b=jP9HpaeDlAxe67MoTGH2DmHM9iogZduxEW9adxVzb+zTHMKEo9gBoAytLuqxu+L7iA b6wf+hvUW/fI2aZCtG1TU9NDSKaAPn5uzx3dPQkFCYmaBHUS2A6uqPwSUFpYYjvIFRMM qVKLe/7de7wDmIHE03mrSx6b+kL939vwZkMOixTuNBhUJbxmwbPiNkriIIe5oTJrpIBv UEkPPV2cFhfmTre7f7JqEUahh1cH4Dmgs8gQ4YmTVF6tBWP4teN4cvP1H0O7hKfaxIiq ojrAvtiHShYPDbcrukdar7jT5lKgYrz2jksVw8lhhTiBAcX5H3LNbGOv3MN1XDkc42SW kDSQ=="},{"name":"ARC-Message-Signature","value":"i=2; a=rsa-sha256; c=relaxed/relaxed; d=google.com; s=arc-20240605; h=list-unsubscribe:list-archive:list-help:list-post:list-id :mailing-list:precedence:reply-to:message-id:subject:to:from:date :type:mime-version:disposition:content-transfer-encoding:dkim-filter :dkim-signature; bh=TkRyv7mvEYU/hkrWRNVG0zUXzodPNiGp+DpD2KaNx28=; fh=LL+84wMlj5znTL1f1JINktTG3JWDXnG5a8tZ+79OWzo=; b=lQ3m1USuGSZeo6SpWjOotnz9ZC6GwyUdDfl8YvlX6NCyo8zGV/ii3P347hG8GQ0KaT AmhiQ054BVpXnnuWk4YnKKmwYFHpH6HpCrl2bIYHA62R4PC8oQ1yKyDgoUCqrRBLf6Oh JI7wXV08Bh2fAfcAmtZ3QHeDG35qMAMaRCoUE+C5fHy6XObT5RY4cuSIY38sxKvU4hNJ uTNsBkq7XAkKltfesI6eIiO9q+LLkYwygXymjHS4Y6U8EbsWgv1C/lDFNEEJmjnWdFT2 4kuc3ivrjWCBQ01fwaaaJxMabX8xayzfjDnQMWpjmnjNLQbXAC3EVRuBsd4uxYokeIxj hXGg==; darn=empresas.febos.io"},{"name":"ARC-Authentication-Results","value":"i=2; mx.google.com; dkim=pass header.i=@smtp.suiteelectronica.com header.s=EFD3DD06-6846-11EE-B0C0-94148666698A header.b=\\"g7zw/Axs\\"; spf=pass (google.com: domain of dte_prod_alcon@smtp.suiteelectronica.com designates 44.207.60.23 as permitted sender) smtp.mailfrom=dte_prod_alcon@smtp.suiteelectronica.com; dmarc=pass (p=QUARANTINE sp=QUARANTINE dis=NONE) header.from=suiteelectronica.com"},{"name":"DKIM-Signature","value":"v=1; a=rsa-sha256; c=relaxed/relaxed; d=febos-cl.20230601.gappssmtp.com; s=20230601; t=1728059473; x=1728664273; darn=empresas.febos.io; h=list-unsubscribe:list-archive:list-help:list-post:list-id:mailing-list:precedence:reply-to:x-original-authentication-results:x-original-sender:message-id:subject:to:from:date:type:mime-version:disposition:content-transfer-encoding:dkim-filter:from:to:cc:subject:date:message-id:reply-to; bh=TkRyv7mvEYU/hkrWRNVG0zUXzodPNiGp+DpD2KaNx28=; b=ftIPDhI/VQ05erCVTOYwrUX7RMM1L615W8/ro7or4yQ60CZniTZlXf5dmwVVRlmgBMT3ppSWN7cz8zPLPbXLMvSp8GOfptHJYkf7c2kYHTSXu3nYhoTfP+RYeNIKszctvQEveSrlaObeoFCzwVYPM/aRvsXDtnWbGmf4HKXHP/U4jVPHL5HEQH4mEkAsNX7iT8iwu9uufFsRStr4E3XMHT6QBruKQbUN6B+rtWaLrBhDRfPp0msqbvD6WIVdpUUpzU/h2i+QnOtAUmC//V07pGnUnSieGUyGEqhaXVINm4lzZJLWLgaBYDqYqMMIKAGEb9MBvXWm2WE6erGPBsivAQ=="},{"name":"X-Google-DKIM-Signature","value":"v=1; a=rsa-sha256; c=relaxed/relaxed; d=1e100.net; s=20230601; t=1728059473; x=1728664273; h=list-unsubscribe:list-archive:list-help:list-post :x-spam-checked-in-group:list-id:mailing-list:precedence:reply-to :x-original-authentication-results:x-original-sender:message-id :subject:to:from:date:type:mime-version:disposition :content-transfer-encoding:dkim-filter:x-beenthere :x-gm-message-state:from:to:cc:subject:date:message-id:reply-to; bh=TkRyv7mvEYU/hkrWRNVG0zUXzodPNiGp+DpD2KaNx28=; b=glMgN6OQZT8uDJS66BaSvG2YGR6/Gh1qTiCuv3Ot4A1Jhum9C50IwO/fJ1ndv+aYv+ 7nWue+vyIYlUrbFwUfXso0xc4gb5jqFp6OsePECCCb+3gXEca9I+QFZy/CNeZqEeHmVR MGWLti4U+8Sl8TUWY7yYTfmIdexvg8qNvO5s3MGeP7YCoyeqgkaOoypLSoO8bRzryc7E eCOEcEAjdRgPYmZPwsa3Qt1RAT450jNFeosDpJdrp8E02gBycyXxYr+3a1f5E6KQfDNi op6dRhkywt4xRjnHhn8OFPGkBzGH8wzopEcu+wmrXXJe0bigdN44oj09IRfzyr2yCa/n kizw=="},{"name":"X-Forwarded-Encrypted","value":"i=2; AJvYcCUHwn7Pnpb26RK0ZKbYvIndp9BgNPfirOCEI+gk5fXB7OZUGoffKCcv/1EzriLBz8H6mc+P6Q==@empresas.febos.io"},{"name":"X-Gm-Message-State","value":"AOJu0YyzZEtz8wRUNipzdYgHRG/1NK8iRzgLphedw/ErbE0qbmumZCSb DTNuYstY0vRS425v8WXUijheqFM7dc2mUhOWlIrd55nlVsJgwiJnMSp8et46lGs5/w=="},{"name":"X-Google-Smtp-Source","value":"AGHT+IEflBcjhYjPhO5es1Dns+u+lUwhR5HLfYAZUudDh9esL4MWfoZAyDsM8iSFe7ceS5rSisazpA=="},{"name":"X-Received","value":"by 2002:a05:6871:24e3:b0:287:410:5374 with SMTP id 586e51a60fabf-287c1dad900mr2826069fac.16.1728059473304; Fri, 04 Oct 2024 09:31:13 -0700 (PDT)"},{"name":"X-BeenThere","value":"61102025-2@febos.cl"},{"name":"Received","value":"by 2002:a05:6871:5384:b0:277:c40a:8a68 with SMTP id 586e51a60fabf-287a4199a8bls1644537fac.0.-pod-prod-03-us; Fri, 04 Oct 2024 09:31:12 -0700 (PDT)"},{"name":"X-Received","value":"by 2002:a05:6359:4c11:b0:1b8:203b:db84 with SMTP id e5c5f4694b2df-1c2b7f36a15mr207967855d.4.1728059471630; Fri, 04 Oct 2024 09:31:11 -0700 (PDT)"},{"name":"ARC-Seal","value":"i=1; a=rsa-sha256; t=1728059471; cv=none; d=google.com; s=arc-20240605; b=cH8fJTJzHtY2azXAPHYmiJCydGu6cV5Byg6z+W2bBvrchHQbPnC0bKM9AvRJ7qSwUo xAaULUHFGTD1WzVKKR8pMZmWFz7Ir7C4sZg9d7zhj+Wqk2tAO2Xjj74V0kmF+iZWTBjD CnLk3DRW4snOORBfDPd7Luny9EVd8l/Xp87YY9CudkUtMN9H7cky6V0c8rRpoaARmKDy wcLIxJ/gJTP6W+1C3mdI1clHKlpQrDMzosg9GDGLOtvOaKGolTWTXVaqKflyFxsCPyL9 F+A4pg3Zmvue76a6xCj+CupNixPsJAkQRS47JnYid79Zk/jlh5Fj2o0SgWR9WKanZcqC wIxg=="},{"name":"ARC-Message-Signature","value":"i=1; a=rsa-sha256; c=relaxed/relaxed; d=google.com; s=arc-20240605; h=message-id:subject:to:from:date:type:mime-version:disposition :content-transfer-encoding:dkim-signature:dkim-filter; bh=TkRyv7mvEYU/hkrWRNVG0zUXzodPNiGp+DpD2KaNx28=; fh=rHYqI+ASMw4rHifxJ6lRr2SZYXfRl/Nnw4MH/l97feM=; b=CE4bw9iE8FnatEW09ljKUoUMhfeDXF/hCAYJzZMfzz8VNlT1GX9quG87s3uF49cvln TRqTDtgawyp2p/80BkAy2Q1Eswt8pxe5BAi6nh9DsleJVKlW2jK5Dripdl6Ix169aFB8 52R7uushJ9uzaQ3UEbr4Z98EhO0u8XjO1LPL0ZB/x7111fFWK4dfW7ss/sqHYvVNXvFT EmocPn5tIJdUctLsaU2JcEZCPXKgGWLkqFKzDM7+NCxZBjo0VSuW61rnzUWcZ0xH+l+R 1i0/+xXlbxeCUbh4zknSU+/JCYlQPSZ1192T9MuNPMCONRWn68MUtHEFLw0mM1Y7qGtH bJyA==; dara=google.com"},{"name":"ARC-Authentication-Results","value":"i=1; mx.google.com; dkim=pass header.i=@smtp.suiteelectronica.com header.s=EFD3DD06-6846-11EE-B0C0-94148666698A header.b=\\"g7zw/Axs\\"; spf=pass (google.com: domain of dte_prod_alcon@smtp.suiteelectronica.com designates 44.207.60.23 as permitted sender) smtp.mailfrom=dte_prod_alcon@smtp.suiteelectronica.com; dmarc=pass (p=QUARANTINE sp=QUARANTINE dis=NONE) header.from=suiteelectronica.com"},{"name":"Received","value":"from smtp.suiteelectronica.com (smtp.suiteelectronica.com. [44.207.60.23]) by mx.google.com with ESMTPS id d75a77b69052e-45da74e6400si1289351cf.213.2024.10.04.09.31.11 for <61102025-2@febos.cl> (version=TLS1_2 cipher=ECDHE-ECDSA-AES128-GCM-SHA256 bits=128/128); Fri, 04 Oct 2024 09:31:11 -0700 (PDT)"},{"name":"Received-SPF","value":"pass (google.com: domain of dte_prod_alcon@smtp.suiteelectronica.com designates 44.207.60.23 as permitted sender) client-ip=44.207.60.23;"},{"name":"Received","value":"from smtp.suiteelectronica.com (localhost [127.0.0.1]) by smtp.suiteelectronica.com (Postfix) with ESMTPS id 672B31074C4B for <61102025-2@febos.cl>; Fri,  4 Oct 2024 16:31:11 +0000 (UTC)"},{"name":"Received","value":"from localhost (localhost [127.0.0.1]) by smtp.suiteelectronica.com (Postfix) with ESMTP id 4E1921074C55 for <61102025-2@febos.cl>; Fri,  4 Oct 2024 16:31:11 +0000 (UTC)"},{"name":"DKIM-Filter","value":"OpenDKIM Filter v2.10.3 smtp.suiteelectronica.com 4E1921074C55"},{"name":"Received","value":"from smtp.suiteelectronica.com ([127.0.0.1]) by localhost (smtp.suiteelectronica.com [127.0.0.1]) (amavisd-new, port 10026) with ESMTP id TZpmxFl4aWOI for <61102025-2@febos.cl>; Fri,  4 Oct 2024 16:31:11 +0000 (UTC)"},{"name":"Received","value":"from localhost.localdomain (ec2-54-204-245-82.compute-1.amazonaws.com [54.204.245.82]) by smtp.suiteelectronica.com (Postfix) with ESMTPA id 2F8E71074C4F; Fri,  4 Oct 2024 16:31:11 +0000 (UTC)"},{"name":"Content-Transfer-Encoding","value":"7bit"},{"name":"Content-Type","value":"multipart/mixed; boundary=\\"_----------=_172805947011280140\\"; charset=\\"iso-8859-1\\""},{"name":"Disposition","value":"attachment"},{"name":"MIME-Version","value":"1.0"},{"name":"Type","value":"text/plain"},{"name":"Date","value":"Fri, 4 Oct 2024 13:31:10 -0300"},{"name":"From","value":"dte_prod_alcon via SANIDAD NAVAL - HOSPITALES <61102025-2@febos.cl>"},{"name":"To","value":"61102025-2@febos.cl"},{"name":"Subject","value":"Alcon Laboratorios Chile Ltd. envía GUIA DE DESPACHO ELECTRÓNICA Nº 449270"},{"name":"X-Mailer","value":"MIME::Lite 3.033 (F2.85; T2.18; A2.21; B3.16; Q3.16)"},{"name":"Message-Id","value":"<20241004163111.2F8E71074C4F@smtp.suiteelectronica.com>"},{"name":"X-Original-Sender","value":"dte_prod_alcon@smtp.suiteelectronica.com"},{"name":"X-Original-Authentication-Results","value":"mx.google.com;       dkim=pass header.i=@smtp.suiteelectronica.com header.s=EFD3DD06-6846-11EE-B0C0-94148666698A header.b=\\"g7zw/Axs\\";       spf=pass (google.com: domain of dte_prod_alcon@smtp.suiteelectronica.com designates 44.207.60.23 as permitted sender) smtp.mailfrom=dte_prod_alcon@smtp.suiteelectronica.com; dmarc=pass (p=QUARANTINE sp=QUARANTINE dis=NONE) header.from=suiteelectronica.com"},{"name":"X-Original-From","value":"dte_prod_alcon@smtp.suiteelectronica.com"},{"name":"Reply-To","value":"dte_prod_alcon@smtp.suiteelectronica.com"},{"name":"Precedence","value":"list"},{"name":"Mailing-list","value":"list 61102025-2@febos.cl; contact 61102025-2+owners@febos.cl"},{"name":"List-ID","value":"<61102025-2.febos.cl>"},{"name":"X-Spam-Checked-In-Group","value":"61102025-2@febos.cl"}],"commonHeaders":{"returnPath":"61102025-2+bncBAABBUNQQC4AMGQE7MD7I3Q@febos.cl","from":["dte_prod_alcon via SANIDAD NAVAL - HOSPITALES <61102025-2@febos.cl>"],"replyTo":["dte_prod_alcon@smtp.suiteelectronica.com"],"date":"Fri, 4 Oct 2024 13:31:10 -0300","to":["61102025-2@febos.cl"],"messageId":"<20241004163111.2F8E71074C4F@smtp.suiteelectronica.com>","subject":"Alcon Laboratorios Chile Ltd. envía GUIA DE DESPACHO ELECTRÓNICA Nº 449270"}},"receipt":{"timestamp":"2024-10-04T16:31:14.331Z","processingTimeMillis":1272,"recipients":["inbox@empresas.febos.io"],"spamVerdict":{"status":"PASS"},"virusVerdict":{"status":"PASS"},"spfVerdict":{"status":"PASS"},"dkimVerdict":{"status":"GRAY"},"dmarcVerdict":{"status":"PASS"},"action":{"type":"S3","topicArn":"arn:aws:sns:us-east-1:830321976775:ses-event-manager-EmailNotificationTopic-13Gug6rA6bdq","bucketName":"febos-io","objectKeyPrefix":"email/produccion/inbox","objectKey":"email/produccion/inbox/bmql2tc8b4h90qte7gat7fdsq0opui6spk9o3fo1"}}}',
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
