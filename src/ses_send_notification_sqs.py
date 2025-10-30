import datetime
import json
import os
import time
import traceback
from _decimal import Decimal

from utils import sqs
from utils.dynamo import get_dynamo_client
from utils.events import send_event
from utils.logic import value_or_default
from utils.s3 import s3_get_object_string, s3_get_object_file
from utils.ses_client import SesClient

TTL = int(os.environ.get('TTL') or '525600')
table_email_name = os.environ.get('TABLE_EMAIL_NAME') or 'ses-email'
table_event_name = os.environ.get('TABLE_EVENT_NAME') or 'ses-event'
table_email_suppression_name = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME') or 'ses-email-suppression'

dynamo_client = get_dynamo_client()

table_email = dynamo_client.Table(table_email_name)
table_event = dynamo_client.Table(table_event_name)
table_suppression = dynamo_client.Table(table_email_suppression_name)


def handler(message, context):
    return sqs.procesar_mensajes(message, procesar_record, context)


def procesar_record(record, context):
    id = None
    params = None
    messageId = None
    vencimiento: datetime.datetime = agregar_minutos(datetime.datetime.now(), TTL)
    expiration = Decimal(time.mktime(vencimiento.timetuple()))
    try:
        sqsBody = json.loads(record['body'])
        id = value_or_default(sqsBody, 'id')
        params = {
            'id': id,
        }

        item = table_email.get_item(Key=params)
        item = item['Item']
        destinatarios = value_or_default(item, 'destinatarios', [])
        copias = value_or_default(item, 'copias', [])
        manifiesto = value_or_default(item, 'manifiesto')
        ConfigurationSetName = value_or_default(item, 'ConfigurationSetName', 'default')
        respuesta_email, subject, tiene_adjuntos, sender, status = send_notification_from_manifest(
            destinatarios,
            manifiesto,
            ConfigurationSetName,
            item,
            copias=copias,
            expiration=expiration
        )
        if respuesta_email:
            messageId = respuesta_email['MessageId']
        response = table_email.update_item(
            Key=params,
            UpdateExpression="set messageId = :messageId, expiration = :expiration, subject = :subject, estado = :status, tieneAdjuntos = :tieneAdjuntos, sender= :sender",
            ExpressionAttributeValues={
                ':messageId': messageId,
                ':status': status,
                ':expiration': expiration,
                ':subject': subject,
                ':sender': sender,
                ':tieneAdjuntos': tiene_adjuntos
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            'statusCode': 200,
            'headers': {}
        }
    except Exception as e:
        evento = {
            "id": id,
            "messageId": id,
            "timestamp": datetime.datetime.now().isoformat(),
            "type": 'Error',
            "event": {
                "error": {
                    "message": str(e),
                }
            },
            "mail": {}
        }
        if expiration:
            evento['expiration'] = expiration
        table_event.put_item(Item=evento)
        traceback.print_exc()
        response = table_email.update_item(
            Key=params,
            UpdateExpression="set estado = :status, expiration = :expiration",
            ExpressionAttributeValues={
                ':status': "error",
                ':expiration': expiration,
            },
            ReturnValues="UPDATED_NEW"
        )
        print(record)
        print(e)
        raise e


def send_notification_from_manifest(
        destinatario,
        manifiesto,
        ConfigurationSetName="default",
        item={},
        copias=[],
        expiration=None
):
    status = "error"
    response = None
    empresa = "0"
    stage = item.get('stage', 'produccion')
    if 'empresa' in item:
        empresa = item['empresa']

    destinatarios = []
    if isinstance(destinatario, list):
        destinatarios = destinatario
    else:
        destinatarios = destinatario.split(',')
    _copias = []
    if isinstance(copias, list):
        _copias = copias
    else:
        _copias = copias.split(',')

    contenido = s3_get_object_string(manifiesto)[0]
    configuracion_manifiesto = json.loads(contenido)

    headers = value_or_default(configuracion_manifiesto, 'headers', [])

    attachments = []
    adjuntos = value_or_default(configuracion_manifiesto, 'adjuntos', [])
    for adjunto in adjuntos:
        file_path = s3_get_object_file(adjunto['ruta'], adjunto['nombreArchivo'])[0]
        attachments.append({
            'file_name': adjunto['nombreArchivo'],
            'file_path': file_path,
            'subtype': adjunto['mediatype'] + '/' + adjunto['submediatype']
        })

    emailField = pasar_campos_en_manifiesto_a_objeto(configuracion_manifiesto)

    tags = []
    if 'empresa' in item:
        tags.append({'Name': 'empresa', 'Value': empresa})
    tags.append({'Name': 'stage', 'Value': stage})
    destinatarios = verificar_correos_suprimidos(item['id'], empresa, destinatarios, expiration=expiration, stage=stage)

    if len(destinatarios) == 0:
        evento = {
            "id": item['id'],
            "messageId": item['id'],
            "timestamp": datetime.datetime.now().isoformat(),
            "type": 'Discarded',
            "event": {
                "suppression": item['id']
            },
            "mail": {}
        }
        if expiration:
            evento['expiration'] = expiration
        table_event.put_item(Item=evento)
        status = 'empty'
        return response, emailField['subject']['value'], len(attachments) > 0, emailField['from']['value'], status

    client = SesClient(config_set_name=ConfigurationSetName)
    response = client.send_email(
        to_addresses=destinatarios,
        cc_addresses=_copias,
        sender_email=emailField['from']['value'],
        subject=emailField['subject']['value'],
        body_text=value_or_default(value_or_default(emailField, 'text', {}), 'value', '-'),
        body_html=emailField['html']['value'],
        attachments=attachments,
        tags=tags,
        headers=headers
    )
    evento = {
        "id": item['id'],
        "messageId": item['id'],
        "timestamp": datetime.datetime.now().isoformat(),
        "type": 'sended',
        "event": {
        },
        "mail": {}
    }
    if expiration:
        evento['expiration'] = expiration
    table_event.put_item(Item=evento)
    return response, emailField['subject']['value'], len(attachments) > 0, emailField['from']['value'], "sended"


def pasar_campos_en_manifiesto_a_objeto(manifiesto):
    output = {}
    for field in manifiesto['campos']:
        output[field['key']] = field
    return output


def verificar_correos_suprimidos(messageId, empresa_id='0', correos=[], expiration=None, stage=None):
    if not correos or len(correos) == 0:
        return correos
    indice = 0
    prefijo = "id"
    valores = {

    }
    fixes_supresed = ['sysadmin@febos.io']
    for correo in fixes_supresed:
        if correo in correos:
            evento = {
                "id": messageId,
                "emailAddress": correo,
                "messageId": messageId,
                "timestamp": datetime.datetime.now().isoformat(),
                "type": 'Suppression',
                "stage": stage,
                "companyId": empresa_id,
                "event": {
                    "suppression": correo
                },
                "mail": {}
            }
            if expiration:
                evento['expiration'] = expiration
            table_event.put_item(Item=evento)
            correos.remove(correo)
            try:
                send_event(
                    event_type='skip-by-suppression',
                    event_data=evento
                )
            except:
                traceback.print_exc()
    in_expresion = []
    for correo in correos:
        expresion_id = f":{prefijo}{indice}"
        valores[expresion_id] = correo
        in_expresion.append(expresion_id)
        indice = indice + 1

    response = table_suppression.scan(
        FilterExpression=f'id IN ( {", ".join(in_expresion)} )',
        ExpressionAttributeValues=valores
    )
    # Verificar si se encontraron elementos en la tabla
    if response['Count'] > 0:
        # Eliminamos correos suprimidos y agregamos el evento de que no se envio por supresion previa
        for item in response['Items']:
            evento = {
                "id": messageId,
                "emailAddress": item['id'],
                "messageId": messageId,
                "timestamp": datetime.datetime.now().isoformat(),
                "type": 'Suppression',
                "stage": stage,
                "companyId": empresa_id,
                "event": {
                    "suppression": item['id']
                },
                "mail": {}
            }
            if expiration:
                evento['expiration'] = expiration
            table_event.put_item(Item=evento)
            correos.remove(item['id'])
            try:
                send_event(
                    event_type='skip-by-suppression',
                    event_data=evento
                )
            except:
                traceback.print_exc()
    return correos


def agregar_minutos(fecha: datetime, aAgregar=0):
    return fecha + datetime.timedelta(minutes=aAgregar)


if __name__ == '__main__':
    null = None
    false = False
    ids = """11c3052b27216249702ad362940634fd1079""".split("\n")
    for id in ids:
        try:
            handler({
                "Records": [
                    {
                        "messageId": "ced098c8-41c1-46fd-a5c5-4df6f92bc4bc",
                        "receiptHandle": "AQEBicc01R5MN0W/oDvzunU9QCFDS55XrYgqEOBtkWJya1lNd3o6S3BciUyDaDffAWEBukRYxrtxyMXub4J/uZNOcI4TwJ0XH/yRcQusGZRmLB/LmimFbqqCe+Tna7g3W6I+cAcO0XvmUa9OJ9pchpLDc09nnY5LQvsDCo+DYrW01MPoSXSDa4MqyJjINc0ZZiteCfWVin3oh0EoQ21ztjdmqz+w0ErepJkh7jKFNZarG/qwYvGT8loB00JY1Ox/hOcloVTiuQK7OOjxGAtoEy+1mEc3brn11Q+HeP3KoG6Phy0=",
                        "body": json.dumps({
                            "id": "0ee65754297dd24aeb296812b8e31903bc87",
                            "application": "FEB",
                            "ConfigurationSetName": "default",
                            "copias": [
                            ],
                            "destinatarios": [
                                "christian.diaz@out.fidseguros.cl"
                            ],
                            "documentoId": null,
                            "domain": "empresas.febos.cl",
                            "empresa": "77096952-2",
                            "estado": "sended",
                            "expiration": 1769168241,
                            "manifiesto": "febos-io/chile/produccion/email/0ee65754297dd24aeb296812b8e31903bc87/0ee65754297dd24aeb296812b8e31903bc87.json",
                            "messageId": "0100019492f3d89f-9a72b4c5-31ed-412f-8f10-0aaf950e7ef6-000000",
                            "pais": "chile",
                            "proceso": "reporte",
                            "sender": "Febos <informacion@empresas.febos.cl>",
                            "servicio": "DTE",
                            "stage": "produccion",
                            "subject": "reporte de documento",
                            "tieneAdjuntos": false,
                            "timestamp": "2025-01-23T11:37:21.573361"
                        }),
                        "attributes": {
                            "ApproximateReceiveCount": "3",
                            "AWSTraceHeader": "Root=1-6675e1fc-60cbfa1f36f930e074e78c8a;Parent=9a61c4c93febbe99;Sampled=1;Lineage=28b1e350:0%7C74085691:0",
                            "SentTimestamp": "1719001604967",
                            "SequenceNumber": "18886808484581103616",
                            "MessageGroupId": "354061be2200f24e732b29024e5746f3a0bd",
                            "SenderId": "AROA4CUYL4XDWRQJ5I45V:ses-send-email",
                            "MessageDeduplicationId": "354061be2200f24e732b29024e5746f3a0bd",
                            "ApproximateFirstReceiveTimestamp": "1719001604967"
                        },
                        "messageAttributes": {},
                        "md5OfBody": "aa3b254cd25340873379033ef1b46cf0",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:830321976775:ses-send-email.fifo",
                        "awsRegion": "us-east-1"
                    }
                ]
            }, None)
        except:
            traceback.print_exc()
